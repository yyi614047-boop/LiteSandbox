"""Command line interface for LiteSandbox."""

import argparse
import json
import math
from collections.abc import Sequence

from .runtime import DockerRuntime, SandboxSession, reap_expired
from .server import serve


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
