"""Expose one existing LiteSandbox workspace over MCP stdio."""

import argparse
from typing_extensions import TypedDict

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError

from litesandbox_client import LiteSandboxClient, LiteSandboxError


class ExecutionResult(TypedDict):
    exit_code: int | None
    stdout: str
    stderr: str
    timed_out: bool
    truncated: bool
    sandbox_destroyed: bool


class ReadResult(TypedDict):
    content: str
    encoding: str
    truncated: bool


class WriteResult(TypedDict):
    bytes_written: int


class RunStatus(TypedDict):
    sandbox_id: str
    run_id: str
    state: str
    exit_code: int | None
    started_at: float
    finished_at: float | None
    truncated: bool


class RunLogs(RunStatus):
    stdout: str
    stderr: str


def make_server(sandbox_id: str, port: int) -> FastMCP:
    client = LiteSandboxClient(port)
    client.sandbox_route(sandbox_id)
    server = FastMCP("LiteSandbox")

    @server.tool(name="exec")
    def execute(argv: list[str], timeout_seconds: int = 10) -> ExecutionResult:
        """Run a command in /workspace and return its exit code and output.

        Provide an argument list. Timeout must be 1 to 60 seconds. A timed-out
        command destroys the bound sandbox.
        """
        try:
            return client.exec(sandbox_id, argv, timeout_seconds)
        except LiteSandboxError as error:
            raise ToolError(str(error)) from error

    @server.tool()
    def read_file(path: str) -> ReadResult:
        """Read UTF-8 text from a /workspace-relative path, up to 64 KiB."""
        try:
            return client.read_file(sandbox_id, path)
        except LiteSandboxError as error:
            raise ToolError(str(error)) from error

    @server.tool()
    def write_file(path: str, content: str) -> WriteResult:
        """Create or replace a UTF-8 file under /workspace.

        The parent directory must already exist. The total JSON request is
        limited to 4 KiB.
        """
        try:
            return client.write_file(sandbox_id, path, content)
        except LiteSandboxError as error:
            raise ToolError(str(error)) from error

    @server.tool()
    def start_run(argv: list[str], timeout_seconds: int = 300) -> RunStatus:
        """Start a background command in /workspace and return its run ID.

        Timeout must be 1 to 3600 seconds. Its deadline stops the command's
        process group while leaving the workspace available.
        """
        try:
            return client.start_run(sandbox_id, argv, timeout_seconds)
        except LiteSandboxError as error:
            raise ToolError(str(error)) from error

    @server.tool()
    def get_run(run_id: str) -> RunStatus:
        """Get run status. Use finished_at to tell when the run has stopped."""
        try:
            return client.get_run(sandbox_id, run_id)
        except (ValueError, LiteSandboxError) as error:
            raise ToolError(str(error)) from error

    @server.tool()
    def run_logs(run_id: str) -> RunLogs:
        """Read the stdout and stderr collected so far, up to 64 KiB each."""
        try:
            return client.run_logs(sandbox_id, run_id)
        except (ValueError, LiteSandboxError) as error:
            raise ToolError(str(error)) from error

    @server.tool()
    def cancel_run(run_id: str) -> RunStatus:
        """Request cancellation of one run. Poll its status to confirm completion."""
        try:
            return client.cancel_run(sandbox_id, run_id)
        except (ValueError, LiteSandboxError) as error:
            raise ToolError(str(error)) from error

    return server


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sandbox-id", required=True)
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    try:
        server = make_server(args.sandbox_id, args.port)
    except ValueError as error:
        parser.error(str(error))
    server.run(transport="stdio")


if __name__ == "__main__":
    main()
