"""Standard-library client for the local LiteSandbox HTTP service."""

from __future__ import annotations

import json
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import ProxyHandler, Request, build_opener


class LiteSandboxError(RuntimeError):
    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class LiteSandboxClient:
    def __init__(self, port: int = 8765) -> None:
        if not 1 <= port <= 65535:
            raise ValueError("port must be between 1 and 65535")
        self._base = f"http://127.0.0.1:{port}/sandboxes"
        self._opener = build_opener(ProxyHandler({}))

    @staticmethod
    def sandbox_route(sandbox_id: str) -> str:
        if not isinstance(sandbox_id, str) or re.fullmatch(r"[0-9a-f]{32}", sandbox_id) is None:
            raise ValueError("sandbox_id must be a 32-character lowercase hexadecimal ID")
        return f"/{sandbox_id}"

    def _request(self, method: str, route: str = "", data: dict | None = None,
                 *, timeout: int = 15) -> dict:
        try:
            body = None if data is None else json.dumps(data, ensure_ascii=False).encode("utf-8")
            headers = {} if body is None else {"Content-Type": "application/json"}
            request = Request(self._base + route, data=body, headers=headers, method=method)
            with self._opener.open(request, timeout=timeout) as response:
                if response.status == 204:
                    return {}
                result = json.load(response)
                if not isinstance(result, dict):
                    raise LiteSandboxError("Expected a JSON object from LiteSandbox")
                return result
        except HTTPError as error:
            with error:
                detail = error.read(4096).decode("utf-8", errors="replace")
            raise LiteSandboxError(f"LiteSandbox HTTP {error.code} {detail}", status=error.code) from error
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise LiteSandboxError(f"LiteSandbox request failed: {error}") from error

    def create(self, ttl_seconds: int = 300, *, allowed_hosts: list[str] | None = None, api_routes: list[str] | None = None, offline: bool = False) -> str:
        return self.create_info(ttl_seconds, allowed_hosts=allowed_hosts, api_routes=api_routes, offline=offline)["sandbox_id"]

    def create_info(self, ttl_seconds: int = 300, *, allowed_hosts: list[str] | None = None, api_routes: list[str] | None = None, offline: bool = False) -> dict:
        data = {"ttl_seconds": ttl_seconds}
        if allowed_hosts is not None:
            data["allowed_hosts"] = allowed_hosts
        if api_routes is not None:
            data["api_routes"] = api_routes
        if offline is not False:
            data["offline"] = offline
        result = self._request("POST", data=data, timeout=60)
        self.sandbox_route(result.get("sandbox_id"))
        return result

    @contextmanager
    def session(self, ttl_seconds: int = 300, *, allowed_hosts: list[str] | None = None, api_routes: list[str] | None = None, offline: bool = False) -> Iterator[str]:
        """Create a sandbox and attempt deletion when the context exits."""
        sandbox_id = self.create(ttl_seconds, allowed_hosts=allowed_hosts, api_routes=api_routes, offline=offline)
        try:
            yield sandbox_id
        except BaseException as task_error:
            try:
                self.delete(sandbox_id)
            except LiteSandboxError as cleanup_error:
                task_error.add_note(f"Could not delete sandbox {sandbox_id}: {cleanup_error}")
            raise
        else:
            self.delete(sandbox_id)

    def list(self) -> list[dict]:
        """List the containers currently managed by the connected service."""
        sandboxes = self._request("GET").get("sandboxes")
        if not isinstance(sandboxes, list):
            raise LiteSandboxError("Expected a sandbox list from LiteSandbox")
        return sandboxes

    def get(self, sandbox_id: str) -> dict:
        return self._request("GET", self.sandbox_route(sandbox_id))

    def delete(self, sandbox_id: str) -> None:
        self._request("DELETE", self.sandbox_route(sandbox_id))

    def exec(self, sandbox_id: str, argv: list[str], timeout_seconds: int = 10) -> dict:
        return self._request(
            "POST", self.sandbox_route(sandbox_id) + "/exec",
            {"argv": argv, "timeout_seconds": timeout_seconds},
            timeout=min(max(timeout_seconds, 1), 60) + 15,
        )

    def start_run(self, sandbox_id: str, argv: list[str], timeout_seconds: int = 300) -> dict:
        return self._request("POST", self.sandbox_route(sandbox_id) + "/runs",
                             {"argv": argv, "timeout_seconds": timeout_seconds})

    def get_run(self, sandbox_id: str, run_id: str) -> dict:
        return self._request("GET", self._run_route(sandbox_id, run_id))

    def run_logs(self, sandbox_id: str, run_id: str) -> dict:
        return self._request("GET", self._run_route(sandbox_id, run_id) + "/logs")

    def stream_run(self, sandbox_id: str, run_id: str) -> Iterator[tuple[str, dict | None]]:
        """Subscribe to output chunks encoded as base64 and a final done event."""
        route = self._run_route(sandbox_id, run_id) + "/events"
        try:
            with self._opener.open(self._base + route, timeout=20) as response:
                event = None
                for line in response:
                    if line.startswith(b"event: "):
                        event = line[7:].strip().decode("ascii")
                    elif line.startswith(b"data: ") and event is not None:
                        yield event, json.loads(line[6:])
                        if event == "done":
                            return
                        event = None
                raise LiteSandboxError("Run event stream ended before completion")
        except HTTPError as error:
            error.close()
            raise LiteSandboxError(f"LiteSandbox HTTP {error.code}", status=error.code) from error
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise LiteSandboxError(f"Run event stream failed: {error}") from error

    def cancel_run(self, sandbox_id: str, run_id: str) -> dict:
        return self._request("DELETE", self._run_route(sandbox_id, run_id))

    def _run_route(self, sandbox_id: str, run_id: str) -> str:
        if not isinstance(run_id, str) or re.fullmatch(r"[0-9a-f]{32}", run_id) is None:
            raise ValueError("run_id must be a 32-character lowercase hexadecimal ID")
        return self.sandbox_route(sandbox_id) + "/runs/" + run_id

    def preview_url(self, sandbox_id: str, port: int = 3000, ttl_seconds: int = 300) -> str:
        route = self.sandbox_route(sandbox_id) + "/preview"
        return self._request("POST", route, {"port": port, "ttl_seconds": ttl_seconds})["url"]

    def read_file(self, sandbox_id: str, path: str) -> dict:
        return self._request("GET", self.sandbox_route(sandbox_id) + "/files?" + urlencode({"path": path}))

    def write_file(self, sandbox_id: str, path: str, content: str) -> dict:
        return self._request("PUT", self.sandbox_route(sandbox_id) + "/files", {"path": path, "content": content})


@dataclass(frozen=True)
class ExecutionResult:
    """Result of a command, including output and sandbox lifecycle flags."""

    exit_code: int | None
    stdout: str
    stderr: str
    timed_out: bool
    truncated: bool
    sandbox_destroyed: bool


@dataclass(frozen=True)
class Sandbox:
    """Operations bound to one remote workspace for the lifetime of a session."""

    _client: LiteSandboxClient
    sandbox_id: str

    def exec(self, argv: list[str], timeout_seconds: int = 10) -> ExecutionResult:
        """Run a command. Reaching the command deadline destroys the sandbox."""
        data = self._client.exec(self.sandbox_id, argv, timeout_seconds)
        return ExecutionResult(
            exit_code=data["exit_code"], stdout=data["stdout"], stderr=data["stderr"],
            timed_out=data["timed_out"], truncated=data["truncated"],
            sandbox_destroyed=data["sandbox_destroyed"],
        )

    def start_run(self, argv: list[str], timeout_seconds: int = 300) -> str:
        return self._client.start_run(self.sandbox_id, argv, timeout_seconds)["run_id"]

    def get_run(self, run_id: str) -> dict:
        return self._client.get_run(self.sandbox_id, run_id)

    def run_logs(self, run_id: str) -> dict:
        """Return the bounded stdout/stderr snapshot collected so far."""
        return self._client.run_logs(self.sandbox_id, run_id)

    def stream_run(self, run_id: str) -> Iterator[tuple[str, dict | None]]:
        return self._client.stream_run(self.sandbox_id, run_id)

    def cancel_run(self, run_id: str) -> dict:
        return self._client.cancel_run(self.sandbox_id, run_id)

    def preview_url(self, port: int = 3000, ttl_seconds: int = 300) -> str:
        """Issue a local browser URL for an HTTP app listening inside this sandbox."""
        return self._client.preview_url(self.sandbox_id, port, ttl_seconds)

    def write_file(self, path: str, content: str) -> int:
        """Write UTF-8 text and return the number of bytes written."""
        return self._client.write_file(self.sandbox_id, path, content)["bytes_written"]

    def read_text(self, path: str) -> str:
        """Read a complete UTF-8 text file or raise if the service truncated it."""
        result = self._client.read_file(self.sandbox_id, path)
        if result["truncated"]:
            raise LiteSandboxError(f"File exceeds the service's text read limit: {path}")
        return result["content"]


class LiteSandbox:
    """Python SDK for the local LiteSandbox service."""

    def __init__(self, port: int = 8765) -> None:
        self._client = LiteSandboxClient(port)

    @contextmanager
    def session(self, ttl_seconds: int = 300, *, allowed_hosts: list[str] | None = None, api_routes: list[str] | None = None, offline: bool = False) -> Iterator[Sandbox]:
        """Create a bound workspace and delete it on context exit."""
        with self._client.session(ttl_seconds, allowed_hosts=allowed_hosts, api_routes=api_routes, offline=offline) as sandbox_id:
            yield Sandbox(self._client, sandbox_id)

    def list(self) -> list[dict]:
        """List sandboxes belonging to the connected service."""
        return self._client.list()
