"""Run one Codex task with a disposable LiteSandbox workspace."""

import argparse
import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path

from litesandbox_client import LiteSandboxClient, LiteSandboxError


def codex_command(codex: str, sandbox_id: str, port: int, prompt: str) -> list[str]:
    adapter = Path(__file__).resolve().with_name("litesandbox_mcp.py")
    args = [str(adapter), "--sandbox-id", sandbox_id, "--port", str(port)]
    config = "mcp_servers.litesandbox"
    instructions = (
        "Perform the task's file and command operations through the litesandbox "
        "MCP tools. Use the sandbox workspace for the task.\n\n" + prompt
    )
    return [
        codex, "exec", "--skip-git-repo-check",
        "-c", f"{config}.command={json.dumps(sys.executable, ensure_ascii=False)}",
        "-c", f"{config}.args={json.dumps(args, ensure_ascii=False)}",
        "-c", f"{config}.tool_timeout_sec=90",
        "--", instructions,
    ]


def run_task(codex: str, prompt: str, port: int, ttl_seconds: int,
             allowed_hosts: list[str] | None = None, *, offline: bool = False) -> int:
    client = LiteSandboxClient(port)
    result = None
    try:
        with client.session(ttl_seconds, allowed_hosts=allowed_hosts, offline=offline) as sandbox_id:
            if allowed_hosts:
                prompt = (
                    "For HTTPS downloads inside the sandbox, prepend "
                    "http://127.0.0.1:3128/ to the source URL. Allowed source domains: "
                    + ", ".join(allowed_hosts) + ".\n\n" + prompt
                )
            result = subprocess.run(
                codex_command(codex, sandbox_id, port, prompt),
                stdin=subprocess.DEVNULL,
            )
    except LiteSandboxError as error:
        if result is None:
            raise
        print(f"Could not delete sandbox {sandbox_id}. TTL will retry cleanup: {error}", file=sys.stderr)
        return result.returncode or 1
    return result.returncode


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("prompt")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--ttl-seconds", type=int, default=1800)
    network = parser.add_mutually_exclusive_group()
    network.add_argument("--offline", action="store_true", help="Disable container networking.")
    network.add_argument("--allow-host", action="append", help="Allow HTTPS downloads from this exact domain (repeatable).")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535 or not 1 <= args.ttl_seconds <= 3600:
        parser.error("port must be 1–65535 and TTL must be 1–3600 seconds")
    codex = shutil.which("codex")
    if codex is None:
        parser.error("codex executable was not found on PATH")
    if importlib.util.find_spec("mcp") is None:
        parser.error("install MCP with python -m pip install -r requirements-mcp.txt")
    try:
        return run_task(codex, args.prompt, args.port, args.ttl_seconds, args.allow_host, offline=args.offline)
    except KeyboardInterrupt as error:
        for note in getattr(error, "__notes__", ()):
            print(note, file=sys.stderr)
        return 130
    except (OSError, ValueError, LiteSandboxError) as error:
        print(f"LiteSandbox task failed: {error}", file=sys.stderr)
        for note in getattr(error, "__notes__", ()):
            print(note, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
