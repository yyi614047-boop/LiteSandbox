"""Local HTTP API for managing and operating sandboxes."""

import base64
import json
import logging
import math
import subprocess
import time
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import BoundedSemaphore, Thread
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

from .egress import load_api_routes, normalize_hosts
from .preview import APP_PORT, PreviewGateway
from .runs import RunRegistry
from .runtime import ContainerIndex, ContainerRecord, DockerRuntime, WarmPool, reap_expired


def serve(runtime: DockerRuntime, port: int, *, reap_interval: float = 30, pool_size: int = 0, preview_port: int | None = None, api_routes_file: str | None = None) -> None:
    if not math.isfinite(reap_interval) or reap_interval <= 0:
        raise ValueError("reap_interval must be finite and positive")

    if not 0 <= pool_size <= 32:
        raise ValueError("pool_size must be between 0 and 32")
    pool = WarmPool(runtime, pool_size)
    index = ContainerIndex()
    runs = RunRegistry()
    configured_routes = load_api_routes(api_routes_file)
    stream_slots = BoundedSemaphore(16)

    class SandboxServer(ThreadingHTTPServer):
        next_reap: float

        request_queue_size = 64

        def service_actions(self) -> None:
            if time.monotonic() < self.next_reap:
                return
            try:
                removed = reap_expired(runtime)
                pool.forget(removed)
                for sandbox_id in removed:
                    index.discard(sandbox_id)
            except (OSError, subprocess.CalledProcessError, RuntimeError):
                logging.exception("Periodic sandbox cleanup failed")
            finally:
                self.next_reap = time.monotonic() + reap_interval

    class SandboxHandler(BaseHTTPRequestHandler):
        def send_json(self, status: int, data: dict[str, object]) -> None:
            body = json.dumps(data).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def read_json_object(self) -> dict[str, object] | None:
            if self.headers.get_content_type() != "application/json":
                self.send_json(415, {"error": "expected_json"})
                return None
            try:
                self.connection.settimeout(5)
                size = int(self.headers.get("Content-Length", "0"))
                if not 1 <= size <= 4096:
                    raise ValueError
                data = json.loads(self.rfile.read(size))
                if not isinstance(data, dict):
                    raise ValueError
            except (ValueError, TimeoutError, RecursionError):
                self.send_json(400, {"error": "invalid_json_body"})
                return None
            return data

        def do_POST(self) -> None:
            path = urlsplit(self.path).path
            if path.startswith("/sandboxes/") and path.endswith("/preview"):
                self.issue_preview()
                return
            if path.startswith("/sandboxes/") and path.endswith("/runs"):
                self.start_run()
                return
            if path.startswith("/sandboxes/") and path.endswith("/exec"):
                self.execute_command()
                return
            if path != "/sandboxes":
                self.send_json(404, {"error": "route_not_found"})
                return
            data = self.read_json_object()
            if data is None:
                return
            ttl = data.get("ttl_seconds", 300)
            try:
                hosts = tuple(normalize_hosts(data.get("allowed_hosts", [])))
            except ValueError:
                self.send_json(400, {"error": "invalid_allowed_hosts"})
                return
            route_names = data.get("api_routes", [])
            if (not isinstance(route_names, list) or len(route_names) > 16
                    or any(not isinstance(name, str) or name not in configured_routes for name in route_names)):
                self.send_json(400, {"error": "invalid_api_routes"})
                return
            routes = {name: configured_routes[name] for name in route_names}
            offline = data.get("offline", False)
            if (data.keys() - {"ttl_seconds", "allowed_hosts", "api_routes", "offline"}
                    or type(ttl) is not int or not 1 <= ttl <= 3600
                    or type(offline) is not bool or (offline and (hosts or routes))):
                self.send_json(400, {"error": "invalid_sandbox_request"})
                return

            try:
                if offline or hosts or routes:
                    sandbox_id = uuid4().hex
                    container_id = runtime.start_new(sandbox_id, time.time() + ttl,
                                                     allowed_hosts=hosts, api_routes=routes, offline=offline)
                    pool_hit = False
                else:
                    sandbox_id, container_id, pool_hit = pool.acquire(ttl)
            except (OSError, subprocess.SubprocessError, RuntimeError) as error:
                self.log_error("Docker create or start failed: %s", error)
                self.send_json(503, {"error": "runtime_unavailable"})
                return
            index.remember(sandbox_id, container_id)
            self.send_json(201, {"sandbox_id": sandbox_id, "container_id": container_id, "pool_hit": pool_hit})

        def issue_preview(self) -> None:
            if gateway is None:
                self.send_json(404, {"error": "preview_disabled"})
                return
            data = self.read_json_object()
            if data is None:
                return
            ttl = data.get("ttl_seconds", 300)
            if (data.keys() - {"port", "ttl_seconds"} or data.get("port", APP_PORT) != APP_PORT
                    or type(ttl) is not int or not 1 <= ttl <= 3600):
                self.send_json(400, {"error": "invalid_preview_request"})
                return
            sandbox = self.lookup_sandbox(suffix="/preview")
            if sandbox is None:
                return
            sandbox_id, record = sandbox
            now = time.time()
            if record is None or record.expired(now):
                self.send_json(404, {"error": "sandbox_not_found"})
                return
            deadline = math.ceil(min(now + ttl, record.expires_at or now + ttl))
            self.send_json(200, {"url": gateway.url(sandbox_id, deadline), "expires_at": deadline})

        def read_exec_request(self, default_timeout: int, max_timeout: int) -> tuple[list[str], int] | None:
            data = self.read_json_object()
            if data is None:
                return None
            argv = data.get("argv")
            timeout = data.get("timeout_seconds", default_timeout)
            if (
                data.keys() - {"argv", "timeout_seconds"}
                or not isinstance(argv, list) or not argv or not argv[0]
                or any(not isinstance(arg, str) or "\x00" in arg or
                       any(0xD800 <= ord(char) <= 0xDFFF for char in arg) for arg in argv)
                or type(timeout) is not int or not 1 <= timeout <= max_timeout
            ):
                self.send_json(400, {"error": "invalid_exec_request"})
                return None
            return argv, timeout

        def start_run(self) -> None:
            request = self.read_exec_request(300, 3600)
            if request is None:
                return
            sandbox = self.lookup_sandbox(suffix="/runs")
            if sandbox is None:
                return
            sandbox_id, record = sandbox
            if record is None or record.expired(time.time()):
                self.send_json(404, {"error": "sandbox_not_found"})
                return
            argv, timeout = request
            prefix = runtime.exec_prefix(record.container_id, interactive=True)
            run = runs.start(sandbox_id, prefix, argv, timeout)
            if run is None:
                self.send_json(503, {"error": "run_capacity_reached"})
                return
            self.send_json(202, run.describe())

        def run_request(self, path: str) -> bool:
            parts = path.split("/")
            if len(parts) < 4 or parts[1] != "sandboxes" or parts[3] != "runs":
                return False
            logs = parts[5:] == ["logs"]
            events = parts[5:] == ["events"]
            if len(parts) != 5 and not ((logs or events) and self.command == "GET"):
                self.send_json(404, {"error": "route_not_found"})
                return True
            run = runs.get(parts[2], parts[4])
            if run is None:
                self.send_json(404, {"error": "run_not_found"})
            elif events:
                self.stream_run(run)
            elif self.command == "DELETE":
                run.stop()
                self.send_json(202, run.describe())
            else:
                self.send_json(200, run.describe(logs=logs))
            return True

        def stream_run(self, run) -> None:
            if not stream_slots.acquire(blocking=False):
                self.send_json(503, {"error": "stream_capacity_reached"})
                return
            try:
                self.connection.settimeout(10)
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Connection", "close")
                self.end_headers()
                for event, data in run.events():
                    if isinstance(data, bytes):
                        data = {"base64": base64.b64encode(data).decode("ascii")}
                    frame = f"event: {event}\ndata: {json.dumps(data)}\n\n"
                    self.wfile.write(frame.encode("utf-8"))
                    self.wfile.flush()
            except OSError:
                self.close_connection = True
            finally:
                stream_slots.release()

        def execute_command(self) -> None:
            request = self.read_exec_request(10, 60)
            if request is None:
                return
            argv, timeout = request
            sandbox_id = self.parse_sandbox_id("/exec")
            if sandbox_id is None:
                return
            try:
                container_id = index.get(sandbox_id)
                if container_id is None:
                    container_id = runtime.list_containers().get(sandbox_id)
                    if container_id is None:
                        self.send_json(404, {"error": "sandbox_not_found"})
                        return
                    index.remember(sandbox_id, container_id)

                result = runtime.execute(container_id, argv, timeout)
                if result["sandbox_destroyed"]:
                    index.discard(sandbox_id)
                elif result["exit_code"] != 0:
                    # Docker CLI uses nonzero exits for both application errors
                    # and externally removed containers. Never replay a command.
                    if runtime.list_containers().get(sandbox_id) != container_id:
                        index.discard(sandbox_id)
                        self.send_json(404, {"error": "sandbox_not_found"})
                        return
            except (OSError, subprocess.CalledProcessError, RuntimeError) as error:
                self.log_error("Docker execution or cleanup failed: %s", error)
                self.send_json(503, {"error": "runtime_unavailable"})
                return
            self.send_json(200, result)

        def parse_sandbox_id(self, suffix: str) -> str | None:
            path = urlsplit(self.path).path.removesuffix(suffix)
            prefix = "/sandboxes/"
            sandbox_id = path.removeprefix(prefix)
            if not path.startswith(prefix) or not sandbox_id or "/" in sandbox_id:
                self.send_json(404, {"error": "route_not_found"})
                return None

            return sandbox_id

        def lookup_sandbox(self, *, suffix: str = "") -> tuple[str, ContainerRecord | None] | None:
            sandbox_id = self.parse_sandbox_id(suffix)
            if sandbox_id is None:
                return None
            try:
                record = runtime.list_container_records().get(sandbox_id)
            except (OSError, subprocess.CalledProcessError, RuntimeError) as error:
                self.log_error("Docker lookup failed: %s", error)
                self.send_json(503, {"error": "runtime_unavailable"})
                return None

            return sandbox_id, record

        def valid_workspace_path(self, path: object) -> bool:
            return (
                isinstance(path, str)
                and len(path) <= 1024
                and "\x00" not in path
                and not any(0xD800 <= ord(char) <= 0xDFFF for char in path)
                and all(part not in {"", ".", ".."} for part in path.split("/"))
            )

        def do_PUT(self) -> None:
            path = urlsplit(self.path).path
            if not path.startswith("/sandboxes/") or not path.endswith("/files"):
                self.send_json(404, {"error": "route_not_found"})
                return

            data = self.read_json_object()
            if data is None:
                return
            workspace_path = data.get("path")
            content = data.get("content")
            if (
                data.keys() != {"path", "content"}
                or not self.valid_workspace_path(workspace_path)
                or not isinstance(content, str)
            ):
                self.send_json(400, {"error": "invalid_file_request"})
                return
            try:
                payload = content.encode("utf-8")
            except UnicodeEncodeError:
                self.send_json(400, {"error": "invalid_file_request"})
                return

            sandbox = self.lookup_sandbox(suffix="/files")
            if sandbox is None:
                return
            _, record = sandbox
            if record is None:
                self.send_json(404, {"error": "sandbox_not_found"})
                return

            parent, _, name = workspace_path.rpartition("/")
            script = """cd -P "./$1" || exit 44
case "$PWD/" in /workspace/*) ;; *) exit 43 ;; esac
target="./$2"
[ ! -L "$target" ] || exit 43
[ ! -e "$target" ] || [ -f "$target" ] || exit 43
temp=$(mktemp ./.litesandbox-write.XXXXXXXX) || exit 1
trap 'rm -f -- "$temp"' EXIT
printf '%s' "$3" | base64 -d >"$temp" || exit 1
mv -fT -- "$temp" "$target"
"""
            encoded = base64.b64encode(payload).decode("ascii")
            try:
                result = runtime.execute(
                    record.container_id, ["sh", "-c", script, "write-file", parent, name, encoded], 10
                )
            except (OSError, subprocess.CalledProcessError) as error:
                self.log_error("Docker file write failed: %s", error)
                self.send_json(503, {"error": "runtime_unavailable"})
                return
            if result["timed_out"]:
                index.discard(sandbox[0])
                self.send_json(504, {"error": "file_write_timed_out", "sandbox_destroyed": True})
            elif result["exit_code"] == 43:
                self.send_json(400, {"error": "invalid_workspace_path"})
            elif result["exit_code"] == 44:
                self.send_json(404, {"error": "directory_not_found"})
            elif result["exit_code"] != 0:
                self.send_json(503, {"error": "file_write_failed"})
            else:
                self.send_json(200, {"bytes_written": len(payload)})

        def read_workspace_file(self, query: str) -> None:
            params = parse_qs(query, keep_blank_values=True)
            paths = params.get("path", [])
            if params.keys() != {"path"} or len(paths) != 1 or not self.valid_workspace_path(paths[0]):
                self.send_json(400, {"error": "invalid_workspace_path"})
                return

            sandbox = self.lookup_sandbox(suffix="/files")
            if sandbox is None:
                return
            _, record = sandbox
            if record is None:
                self.send_json(404, {"error": "sandbox_not_found"})
                return

            script = """target=$(realpath "./$1") || exit 44
case "$target" in /workspace/*) ;; *) exit 43 ;; esac
[ -f "$target" ] || exit 44
exec 3<"$target" || exit 44
opened=$(readlink /proc/self/fd/3) || exit 43
case "$opened" in /workspace/*) ;; *) exit 43 ;; esac
[ -f /proc/self/fd/3 ] || exit 44
head -c 65537 <&3
"""
            try:
                result = runtime.execute(record.container_id, ["sh", "-c", script, "read-file", paths[0]], 10)
            except (OSError, subprocess.CalledProcessError) as error:
                self.log_error("Docker file read failed: %s", error)
                self.send_json(503, {"error": "runtime_unavailable"})
                return
            if result["timed_out"]:
                index.discard(sandbox[0])
                self.send_json(504, {"error": "file_read_timed_out", "sandbox_destroyed": True})
            elif result["exit_code"] == 43:
                self.send_json(400, {"error": "invalid_workspace_path"})
            elif result["exit_code"] == 44:
                self.send_json(404, {"error": "file_not_found"})
            elif result["exit_code"] != 0:
                self.send_json(503, {"error": "file_read_failed"})
            else:
                self.send_json(200, {
                    "content": result["stdout"],
                    "encoding": "utf-8",
                    "truncated": result["truncated"],
                })

        def do_GET(self) -> None:
            url = urlsplit(self.path)
            if self.run_request(url.path):
                return
            if url.path == "/healthz":
                self.send_json(200, {"status": "ok"})
                return
            if url.path in {"/sandboxes", "/readyz"}:
                try:
                    containers = runtime.list_container_records()
                except (OSError, subprocess.CalledProcessError, RuntimeError) as error:
                    self.log_error("Docker list failed: %s", error)
                    self.send_json(503, {"error": "runtime_unavailable"})
                    return
                if url.path == "/readyz":
                    self.send_json(200, {"status": "ready"})
                    return
                now = time.time()
                self.send_json(200, {"sandboxes": [
                    record.describe(sandbox_id, now)
                    for sandbox_id, record in sorted(containers.items())
                ]})
                return
            if url.path.startswith("/sandboxes/") and url.path.endswith("/files"):
                self.read_workspace_file(url.query)
                return
            sandbox = self.lookup_sandbox()
            if sandbox is None:
                return
            sandbox_id, record = sandbox
            if record is None:
                self.send_json(404, {"error": "sandbox_not_found"})
                return
            self.send_json(200, record.describe(sandbox_id, time.time()))

        def do_DELETE(self) -> None:
            if self.run_request(urlsplit(self.path).path):
                return
            sandbox = self.lookup_sandbox()
            if sandbox is None:
                return
            _, record = sandbox
            if record is not None:
                try:
                    runtime.remove(record.container_id)
                except (OSError, subprocess.CalledProcessError) as error:
                    self.log_error("Docker removal failed: %s", error)
                    self.send_json(503, {"error": "runtime_unavailable"})
                    return
            index.discard(sandbox[0])
            self.send_response(204)
            self.end_headers()

    with ExitStack() as stack:
        server = stack.enter_context(SandboxServer(("127.0.0.1", port), SandboxHandler))
        gateway = None
        if preview_port is not None:
            gateway = stack.enter_context(PreviewGateway(runtime, preview_port))
            thread = Thread(target=gateway.serve_forever, daemon=True)
            thread.start()
            stack.callback(thread.join)
            stack.callback(gateway.shutdown)
        removed = reap_expired(runtime)
        for sandbox_id, container_id in runtime.list_containers().items():
            index.remember(sandbox_id, container_id)
        server.next_reap = time.monotonic() + reap_interval
        host, bound_port = server.server_address
        try:
            pool.open()
            print(f"Listening on http://{host}:{bound_port} (reaped {len(removed)})", flush=True)
            server.serve_forever(poll_interval=min(0.5, reap_interval))
        except KeyboardInterrupt:
            pass
        finally:
            runs.close()
            pool.close()
