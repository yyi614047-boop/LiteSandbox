"""In-memory tracking for commands running in their own sandbox process groups."""

import subprocess
import time
from threading import Condition, Lock, Thread
from uuid import uuid4


OUTPUT_LIMIT = 64 * 1024
MAX_ACTIVE = 16
MAX_RECORDS = 128

# Only the supervisor reads Docker stdin. The command gets /dev/null.
# The watcher belongs to the same process group, so it can only signal a
# group it still belongs to. EOF also stops the group on a closed control pipe.
SUPERVISOR = """
leader=$$
exec 3<&0
(
    IFS= read -r message <&3
    /bin/busybox kill -KILL "-$leader"
) &
watcher=$!
"$@" </dev/null 3<&- &
child=$!
wait "$child"
status=$?
kill "$watcher" 2>/dev/null
wait "$watcher" 2>/dev/null
exit "$status"
"""


class Run:
    def __init__(self, sandbox_id: str) -> None:
        self.sandbox_id = sandbox_id
        self.run_id = uuid4().hex
        self.state = "starting"
        self.exit_code = None
        self.started_at = time.time()
        self.finished_at = None
        self.process = None
        self.stop_reason = None
        self.stdout = bytearray()
        self.stderr = bytearray()
        self.truncated = False
        self.lock = Condition()

    def describe(self, *, logs: bool = False) -> dict:
        with self.lock:
            data = {
                "sandbox_id": self.sandbox_id, "run_id": self.run_id,
                "state": self.state, "exit_code": self.exit_code,
                "started_at": self.started_at, "finished_at": self.finished_at,
                "truncated": self.truncated,
            }
            if logs:
                data["stdout"] = self.stdout.decode("utf-8", errors="replace")
                data["stderr"] = self.stderr.decode("utf-8", errors="replace")
            return data

    def drain(self, stream, buffer: bytearray) -> None:
        while chunk := stream.read1(8192):
            with self.lock:
                remaining = OUTPUT_LIMIT - len(buffer)
                self.truncated |= len(chunk) > remaining
                buffer.extend(chunk[:remaining])
                self.lock.notify_all()

    def events(self):
        """Yield retained bytes and then new output until the run completes."""
        offsets = {"stdout": 0, "stderr": 0}
        while True:
            with self.lock:
                changed = self.lock.wait_for(
                    lambda: self.finished_at is not None
                    or any(len(getattr(self, name)) > offset for name, offset in offsets.items()),
                    timeout=5,
                )
                chunks = []
                for name, offset in offsets.items():
                    buffer = getattr(self, name)
                    if len(buffer) > offset:
                        chunks.append((name, bytes(buffer[offset:])))
                        offsets[name] = len(buffer)
                finished = self.finished_at is not None
            for chunk in chunks:
                yield chunk
            if finished:
                yield "done", self.describe()
                return
            if not changed:
                yield "heartbeat", None

    def stop(self, reason: str = "cancelled") -> None:
        with self.lock:
            if self.finished_at is not None or self.stop_reason is not None:
                return
            if self.process is not None and self.process.poll() is not None:
                return
            self.stop_reason = reason
            self.state = "cancelling"
            self._signal()

    def _signal(self) -> None:
        if self.process is not None:
            try:
                self.process.stdin.write(b"stop\n")
                self.process.stdin.flush()
            except OSError:
                pass

    def execute(self, command: list[str], timeout: int) -> None:
        try:
            with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE) as process:
                with self.lock:
                    self.process = process
                    if self.stop_reason is None:
                        self.state = "running"
                    else:
                        self._signal()
                readers = [
                    Thread(target=self.drain, args=(process.stdout, self.stdout), daemon=True),
                    Thread(target=self.drain, args=(process.stderr, self.stderr), daemon=True),
                ]
                for reader in readers:
                    reader.start()
                try:
                    code = process.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    self.stop("timed_out")
                    try:
                        code = process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                        code = None
                for reader in readers:
                    reader.join()
                with self.lock:
                    self.exit_code = code
                    self.state = (
                        self.stop_reason if self.stop_reason and code in (137, -9)
                        else "failed" if code != 0 else "succeeded"
                    )
        except OSError as error:
            with self.lock:
                self.stderr.extend(str(error).encode("utf-8", errors="replace")[:OUTPUT_LIMIT])
                self.state = "failed"
        finally:
            with self.lock:
                self.finished_at = time.time()
                self.process = None
                self.lock.notify_all()


class RunRegistry:
    def __init__(self) -> None:
        self.lock = Lock()
        self.runs: dict[str, Run] = {}

    def start(self, sandbox_id: str, prefix: list[str], argv: list[str], timeout: int) -> Run | None:
        with self.lock:
            if sum(run.describe()["finished_at"] is None for run in self.runs.values()) >= MAX_ACTIVE:
                return None
            if len(self.runs) >= MAX_RECORDS:
                oldest_finished = next(key for key, run in self.runs.items()
                                       if run.describe()["finished_at"] is not None)
                del self.runs[oldest_finished]
            run = Run(sandbox_id)
            self.runs[run.run_id] = run
        # Docker exec starts a process-group leader. A waiting shell keeps
        # setsid from having to fork away from the tracked Docker process.
        command = [
            *prefix, "sh", "-c", '/bin/busybox setsid "$@"; exit "$?"', "launch",
            "sh", "-c", SUPERVISOR, "run", *argv,
        ]
        Thread(target=run.execute, args=(command, timeout), daemon=True).start()
        return run

    def get(self, sandbox_id: str, run_id: str) -> Run | None:
        with self.lock:
            run = self.runs.get(run_id)
            return run if run is not None and run.sandbox_id == sandbox_id else None

    def close(self) -> None:
        with self.lock:
            for run in self.runs.values():
                run.stop()
