import argparse
import base64
import json
import logging
import math
import subprocess
import time
from collections import OrderedDict
from collections.abc import Sequence
from contextlib import ExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import BoundedSemaphore, Condition, Lock, Thread
from typing import BinaryIO, NamedTuple
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

from litesandbox_egress import load_api_routes, normalize_hosts, relay_command, start_proxy
from litesandbox_runs import RunRegistry
from litesandbox_preview import APP_PORT, PreviewGateway
from litesandbox_client import ExecutionResult, LiteSandbox, LiteSandboxError, Sandbox


class ContainerRecord(NamedTuple):
    container_id: str
    state: str
    expires_at: float | None
    allowed_hosts: tuple[str, ...] = ()

    def expired(self, now: float) -> bool:
        return self.expires_at is not None and self.expires_at <= now

    def describe(self, sandbox_id: str, now: float) -> dict[str, object]:
        return {"sandbox_id": sandbox_id, **self._asdict(), "expired": self.expired(now)}


class DockerRuntime:
    """Translate container operations into Docker CLI calls."""

    OWNER_LABEL = "litesandbox.owner"
    SANDBOX_LABEL = "litesandbox.sandbox-id"
    EGRESS_LABEL = "litesandbox.egress"
    EXPIRY_LABEL = "litesandbox.expires-at"

    def __init__(self, owner: str = "local") -> None:
        if not owner.strip():
            raise ValueError("Provide a nonempty sandbox owner")
        self._owner = owner

    def create(
        self, image: str, sandbox_id: str, *, expires_at: float | None = None, standby: bool = False,
        allowed_hosts: tuple[str, ...] = (), api_routes: dict | None = None, offline: bool = False,
    ) -> str:
        labels = [
            "--label", f"{self.OWNER_LABEL}={self._owner}",
            "--label", f"{self.SANDBOX_LABEL}={sandbox_id}",
        ]
        if standby:
            labels.extend(["--name", f"litesandbox-idle-{sandbox_id}"])
        if expires_at is not None:
            labels.extend(["--label", f"{self.EXPIRY_LABEL}={expires_at}"])
        command = ["tail", "-f", "/dev/null"]
        if allowed_hosts or api_routes:
            labels.extend([
                "--mount", "type=volume,dst=/run",
                "--label", f"{self.EGRESS_LABEL}=" + ",".join(allowed_hosts),
                "--env", "LITESANDBOX_SOURCE_GATEWAY=http://127.0.0.1:3128/",
            ])
            command = relay_command()
        created = subprocess.run(
            [
                "docker", "create",
                *labels,
                "--network", "none" if offline or allowed_hosts or api_routes else "bridge",
                "--tmpfs", "/workspace:rw,nosuid,nodev,size=64m,uid=1000,gid=1000,mode=0700",
                "--user", "1000:1000",
                "--cap-drop", "ALL",
                "--security-opt", "no-new-privileges",
                "--memory", "128m",
                "--cpus", "1",
                "--pids-limit", "64",
                image, *command,
            ],
            stdout=subprocess.PIPE,
            text=True,
            check=True,
        )
        return created.stdout.strip()

    def list_containers(self) -> dict[str, str]:
        """Map this owner's sandbox IDs to full Docker container IDs."""
        return {
            sandbox_id: record.container_id
            for sandbox_id, record in self.list_container_records().items()
        }

    def list_container_records(self, *, include_standby: bool = False) -> dict[str, ContainerRecord]:
        """Return container state and expiry keyed by sandbox ID."""
        sandbox_field = '{{.Label "' + self.SANDBOX_LABEL + '"}}'
        hosts_field = '{{.Label "' + self.EGRESS_LABEL + '"}}'
        expiry_field = '{{.Label "' + self.EXPIRY_LABEL + '"}}'
        result = subprocess.run(
            [
                "docker", "ps", "--all", "--no-trunc",
                "--filter", f"label={self.OWNER_LABEL}={self._owner}",
                "--filter", f"label={self.SANDBOX_LABEL}",
                "--format", "{{.ID}}\t{{.Names}}\t{{.State}}\t" + sandbox_field + "\t" + hosts_field + "\t" + expiry_field,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        containers: dict[str, ContainerRecord] = {}
        for line in result.stdout.splitlines():
            container_id, name, state, sandbox_id, hosts, expiry = line.split("\t", 5)
            if name == f"litesandbox-idle-{sandbox_id}":
                if not include_standby:
                    continue
                # Remove by idle name so a concurrent claim is never reaped.
                container_id = name
            lease_prefix = f"litesandbox-lease-{sandbox_id}-"
            if name.startswith(lease_prefix):
                expiry = name[len(lease_prefix):]
            if not sandbox_id or sandbox_id in containers:
                raise RuntimeError(f"Invalid or duplicate sandbox label on {container_id}")
            try:
                expires_at = float(expiry)
            except ValueError:
                expires_at = None
            if expires_at is not None and not math.isfinite(expires_at):
                expires_at = None
            containers[sandbox_id] = ContainerRecord(container_id, state, expires_at, tuple(hosts.split(",")) if hosts else ())
        return containers

    def start_new(self, sandbox_id: str, expires_at: float, *, standby: bool = False,
                  allowed_hosts: tuple[str, ...] = (), api_routes: dict | None = None,
                  offline: bool = False) -> str:
        image = "python:3.11-alpine" if allowed_hosts or api_routes else "alpine:3.22"
        container_id = self.create(image, sandbox_id, expires_at=expires_at, standby=standby,
                                   allowed_hosts=allowed_hosts, api_routes=api_routes, offline=offline)
        try:
            self.start(container_id)
            if allowed_hosts or api_routes:
                start_proxy(container_id, allowed_hosts, api_routes)
        except BaseException:
            self.remove(container_id)
            raise
        return container_id

    def claim(self, sandbox_id: str, ttl: int) -> None:
        deadline = time.time() + ttl
        subprocess.run(
            ["docker", "rename", f"litesandbox-idle-{sandbox_id}",
             f"litesandbox-lease-{sandbox_id}-{deadline}"],
            check=True, capture_output=True,
        )

    def start(self, container_id: str) -> None:
        subprocess.run(
            ["docker", "start", container_id],
            stdout=subprocess.DEVNULL,
            check=True,
        )

    def remove(self, container_id: str) -> None:
        result = subprocess.run(
            ["docker", "rm", "--force", "--volumes", container_id],
            capture_output=True,
            text=True,
        )
        if result.stderr.strip().endswith(f"No such container: {container_id}"):
            return
        result.check_returncode()

    def execute(self, container_id: str, argv: list[str], timeout: int) -> dict[str, object]:
        output_limit = 64 * 1024
        buffers = [bytearray(), bytearray()]
        truncated = [False, False]

        def drain(stream: BinaryIO, index: int) -> None:
            buffer = buffers[index]
            while chunk := stream.read(8192):
                remaining = output_limit - len(buffer)
                buffer.extend(chunk[:remaining])
                truncated[index] |= len(chunk) > remaining

        timed_out = False
        with subprocess.Popen(
            [*self.exec_prefix(container_id), *argv],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        ) as process:
            assert process.stdout is not None and process.stderr is not None
            readers = [
                Thread(target=drain, args=(stream, index))
                for index, stream in enumerate((process.stdout, process.stderr))
            ]
            for reader in readers:
                reader.start()
            try:
                exit_code = process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out = True
                exit_code = None
                process.kill()
                process.wait()
                self.remove(container_id)
            finally:
                for reader in readers:
                    reader.join()

        stdout, stderr = (buffer.decode("utf-8", errors="replace") for buffer in buffers)
        return {
            "exit_code": exit_code,
            "stdout": stdout,
            "stderr": stderr,
            "timed_out": timed_out,
            "truncated": any(truncated),
            "sandbox_destroyed": timed_out,
        }

    def exec_prefix(self, container_id: str, *, interactive: bool = False) -> list[str]:
        command = ["docker", "exec", "--workdir", "/workspace"]
        if interactive:
            command.append("--interactive")
        return [*command, container_id]


def reap_expired(runtime: DockerRuntime) -> list[str]:
    """Remove this owner's expired containers and return their sandbox IDs."""
    now = time.time()
    removed = []
    for sandbox_id, record in runtime.list_container_records(include_standby=True).items():
        if record.expired(now):
            runtime.remove(record.container_id)
            removed.append(sandbox_id)
    return removed


class SandboxSession:
    """Own one Docker container from the first command until the session ends."""

    def __init__(
        self, image: str = "alpine:3.22", *, owner: str = "local",
        ttl_seconds: float | None = None, offline: bool = False,
    ) -> None:
        if ttl_seconds is not None and (not math.isfinite(ttl_seconds) or ttl_seconds <= 0):
            raise ValueError("ttl_seconds must be finite and positive")
        self.image = image
        self._ttl_seconds = ttl_seconds
        self._offline = offline
        self._sandbox_id = uuid4().hex
        self._runtime = DockerRuntime(owner)
        self._container_id: str | None = None
        self._closed = False

    @property
    def sandbox_id(self) -> str:
        return self._sandbox_id

    def __enter__(self) -> "SandboxSession":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        try:
            self.close()
        except subprocess.CalledProcessError as cleanup_error:
            if exc is None:
                raise
            exc.add_note(f"Docker cleanup failed: {cleanup_error}")

    def exec(self, argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
        if isinstance(argv, str) or not argv:
            raise ValueError("Provide a nonempty list of command arguments")

        return subprocess.run(
            [*self._exec_prefix(), *argv],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )

    def upload(self, local_path: str | Path, workspace_path: str) -> None:
        """Copy a host file to an existing directory in the workspace."""
        with Path(local_path).open("rb") as source:
            subprocess.run(
                self._transfer_command(workspace_path, upload=True),
                stdin=source,
                check=True,
            )

    def download(self, workspace_path: str, local_path: str | Path) -> None:
        """Copy a regular workspace file to a host path."""
        result = subprocess.run(
            self._transfer_command(workspace_path, upload=False),
            stdout=subprocess.PIPE,
            check=True,
        )
        Path(local_path).write_bytes(result.stdout)

    def _transfer_command(self, path: str, *, upload: bool) -> list[str]:
        parts = path.split("/")
        if any(part in {"", ".", ".."} for part in parts) or "\x00" in path:
            raise ValueError("Use a relative workspace file path without . or ..")

        # Resolve the parent inside the container and disallow symlink targets.
        script = """
parent=$(realpath "./$1") || exit 1
case "$parent/" in /workspace/*) ;; *) exit 1 ;; esac
file="$parent/$2"
if [ -L "$file" ]; then exit 1; fi
"""
        if upload:
            script += '[ ! -e "$file" ] || [ -f "$file" ] || exit 1\ncat > "$file"'
        else:
            script += '[ -f "$file" ] || exit 1\ncat "$file"'
        return [
            *self._exec_prefix(interactive=upload),
            "sh", "-c", script, "transfer", "/".join(parts[:-1]), parts[-1],
        ]

    def _exec_prefix(self, *, interactive: bool = False) -> list[str]:
        if self._closed:
            raise RuntimeError("The sandbox session is closed")
        if self._container_id is None:
            self._start()
        assert self._container_id is not None
        return self._runtime.exec_prefix(self._container_id, interactive=interactive)

    def _start(self) -> None:
        expires_at = None
        if self._ttl_seconds is not None:
            expires_at = time.time() + self._ttl_seconds
        container_id = self._runtime.create(self.image, self.sandbox_id, expires_at=expires_at, offline=self._offline)
        self._container_id = container_id
        try:
            self._runtime.start(container_id)
        except BaseException as start_error:
            try:
                self.close()
            except subprocess.CalledProcessError as cleanup_error:
                start_error.add_note(f"Docker cleanup failed: {cleanup_error}")
            raise

    def close(self) -> None:
        self._closed = True
        if self._container_id is not None:
            self._runtime.remove(self._container_id)
            self._container_id = None


class WarmPool:
    """Hand out each fresh standby once. Refill outside the request thread."""

    def __init__(self, runtime: DockerRuntime, capacity: int) -> None:
        self.runtime = runtime
        self.capacity = capacity
        self.idle: list[tuple[str, str]] = []
        self.condition = Condition()
        self.stopping = False
        self.worker = Thread(target=self._refill, daemon=True)

    def _prepare(self) -> tuple[str, str]:
        sandbox_id = uuid4().hex
        # Unclaimed containers left by a crashed process expire after one hour.
        container_id = self.runtime.start_new(sandbox_id, time.time() + 3600, standby=True)
        return sandbox_id, container_id

    def open(self) -> None:
        for _ in range(self.capacity):
            self.idle.append(self._prepare())
        if self.capacity:
            self.worker.start()

    def _refill(self) -> None:
        while True:
            with self.condition:
                self.condition.wait_for(lambda: self.stopping or len(self.idle) < self.capacity)
                if self.stopping:
                    return
            try:
                item = self._prepare()
            except (OSError, subprocess.CalledProcessError):
                logging.exception("Could not replenish standby container")
                with self.condition:
                    self.condition.wait_for(lambda: self.stopping, timeout=1)
                continue
            with self.condition:
                self.idle.append(item)

    def acquire(self, ttl: int) -> tuple[str, str, bool]:
        with self.condition:
            item = self.idle.pop(0) if self.idle else None
            self.condition.notify()
        if item is not None:
            sandbox_id, container_id = item
            try:
                self.runtime.claim(sandbox_id, ttl)
            except subprocess.CalledProcessError:
                # The idle container may already have expired.
                self.runtime.remove(f"litesandbox-idle-{sandbox_id}")
            else:
                return sandbox_id, container_id, True
        sandbox_id = uuid4().hex
        return sandbox_id, self.runtime.start_new(sandbox_id, time.time() + ttl), False

    def forget(self, removed_ids: list[str]) -> None:
        if removed_ids:
            removed = set(removed_ids)
            with self.condition:
                self.idle = [item for item in self.idle if item[0] not in removed]
                self.condition.notify()

    def close(self) -> None:
        with self.condition:
            self.stopping = True
            self.condition.notify_all()
        if self.worker.is_alive():
            self.worker.join()
        for sandbox_id, _ in self.idle:
            self.runtime.remove(f"litesandbox-idle-{sandbox_id}")


class ContainerIndex:
    """Bounded per-service cache of full Docker IDs, never of container state."""

    def __init__(self) -> None:
        self.ids: OrderedDict[str, str] = OrderedDict()
        self.lock = Lock()

    def remember(self, sandbox_id: str, container_id: str) -> None:
        with self.lock:
            self.ids[sandbox_id] = container_id
            self.ids.move_to_end(sandbox_id)
            if len(self.ids) > 4096:
                self.ids.popitem(last=False)

    def get(self, sandbox_id: str) -> str | None:
        with self.lock:
            container_id = self.ids.get(sandbox_id)
            if container_id is not None:
                self.ids.move_to_end(sandbox_id)
            return container_id

    def discard(self, sandbox_id: str) -> None:
        with self.lock:
            self.ids.pop(sandbox_id, None)


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


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the LiteSandbox demo or manage containers.")
    commands = parser.add_subparsers(dest="command")
    recover = commands.add_parser("recover", help="Remove expired containers and list survivors.")
    recover.add_argument("--owner", default="local", help="Container owner label (default: local).")
    server = commands.add_parser("serve", help="Start the local sandbox management service.")
    server.add_argument("--owner", default="local", help="Container owner label (default: local).")
    server.add_argument("--port", type=int, default=8765, help="Local HTTP port (default: 8765).")
    server.add_argument("--reap-interval", type=float, default=30, help="Seconds between cleanup runs (default: 30).")
    server.add_argument("--api-routes-file", help="Host-side JSON file defining outbound API routes.")
    server.add_argument("--preview-port", type=int, help="Enable local HTTP previews on this port.")
    server.add_argument("--pool-size", type=int, default=0, choices=range(33), metavar="0..32")
    args = parser.parse_args(argv)

    if args.command == "serve":
        if not math.isfinite(args.reap_interval) or args.reap_interval <= 0:
            server.error("--reap-interval must be finite and positive")
        serve(DockerRuntime(args.owner), args.port, reap_interval=args.reap_interval, pool_size=args.pool_size, preview_port=args.preview_port, api_routes_file=args.api_routes_file)
        return

    if args.command == "recover":
        runtime = DockerRuntime(args.owner)
        removed = reap_expired(runtime)
        print(json.dumps({"removed": removed, "containers": runtime.list_containers()}))
        return

    with SandboxSession() as session:
        write = session.exec(["sh", "-c", "printf 'hello from the sandbox\n' > message.txt"])
        write.check_returncode()

        read = session.exec(["cat", "message.txt"])
        read.check_returncode()
        print(read.stdout, end="")


if __name__ == "__main__":
    main()
