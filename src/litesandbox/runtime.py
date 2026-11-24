"""Docker sandbox lifecycle, execution and prewarming."""

import logging
import math
import subprocess
import time
from collections import OrderedDict
from collections.abc import Sequence
from pathlib import Path
from threading import Condition, Lock, Thread
from typing import BinaryIO, NamedTuple
from uuid import uuid4

from .egress import relay_command, start_proxy


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
