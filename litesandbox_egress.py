"""Read-only HTTPS source gateway for offline sandbox workloads."""

import http.client
import ipaddress
import json
import os
import re
import select
import socket
import socketserver
import ssl
import subprocess
import sys
import time
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urljoin, urlsplit

SOCKET = "/run/.litesandbox-egress.sock"
PORT = 3128
MAX_BYTES = 16 * 1024 * 1024
LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")


def normalize_hosts(values):
    if not isinstance(values, list) or len(values) > 16:
        raise ValueError("expected at most 16 exact DNS names")
    hosts = set()
    for value in values:
        if not isinstance(value, str):
            raise ValueError("invalid hostname")
        host = value.lower()
        parts = host.split(".")
        if (not host.isascii() or len(host) > 253 or len(parts) < 2
                or not parts[-1].isalpha() or not all(LABEL.fullmatch(p) for p in parts)):
            raise ValueError("invalid hostname")
        hosts.add(host)
    return sorted(hosts)


ROUTE_NAME = re.compile(r"[a-z][a-z0-9-]{0,31}")
SECRET_NAME = re.compile(r"[A-Z_][A-Z0-9_]{0,63}")
MAX_API_BODY = 1024 * 1024


def load_api_routes(path):
    if path is None:
        return {}
    routes = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(routes, dict):
        raise ValueError("API routes must be a JSON object")
    for name, route in routes.items():
        if (not ROUTE_NAME.fullmatch(name) or not isinstance(route, dict)
                or route.keys() != {"url", "method", "token_env"}
                or route["method"] not in ("GET", "POST")
                or not isinstance(route["token_env"], str)
                or not SECRET_NAME.fullmatch(route["token_env"])
                or not isinstance(route["url"], str)
                or not re.fullmatch(r"https://[a-z0-9.-]+/[A-Za-z0-9_/.~:-]*", route["url"])):
            raise ValueError(f"Invalid API route: {name}")
        target = urlsplit(route["url"])
        normalize_hosts([target.hostname])
        token = os.environ.get(route["token_env"], "")
        if not re.fullmatch(r"[!-~]{1,8192}", token):
            raise ValueError(f"Missing or invalid bearer token for API route: {name}")
    return routes


def invoke_api(route, body):
    target = urlsplit(route["url"])
    headers = {
        "Authorization": "Bearer " + os.environ[route["token_env"]],
        "Accept-Encoding": "identity",
    }
    if route["method"] == "POST":
        headers["Content-Type"] = "application/json"
    connection = PublicHTTPS(target.hostname, timeout=8, context=ssl.create_default_context())
    try:
        connection.request(route["method"], target.path, body=body, headers=headers)
        response = connection.getresponse()
        if 300 <= response.status < 400:
            raise PermissionError("API redirects are denied")
        data = response.read(MAX_BYTES + 1)
        if len(data) > MAX_BYTES:
            raise OverflowError("API response too large")
        # Return JSON responses only. The configured upstream is trusted with the token.
        if data:
            json.loads(data)
        return response.status, data
    finally:
        connection.close()


class PublicHTTPS(http.client.HTTPSConnection):
    def connect(self):
        addresses = socket.getaddrinfo(self.host, 443, socket.AF_INET, socket.SOCK_STREAM)
        ips = [item[4][0] for item in addresses]
        parsed = [ipaddress.ip_address(ip) for ip in ips]
        if not parsed or any(not ip.is_global or ip.is_multicast for ip in parsed):
            raise PermissionError("destination is not public")
        # Pin the validated IP, retaining the original hostname for TLS.
        raw = socket.create_connection((ips[0], 443), timeout=self.timeout)
        try:
            self.sock = self._context.wrap_socket(raw, server_hostname=self.host)
        except BaseException:
            raw.close()
            raise


def fetch(url, hosts, method):
    for _ in range(5):
        if not url.isascii() or len(url) > 4096 or any(ord(c) <= 32 or ord(c) == 127 for c in url):
            raise ValueError("invalid URL")
        target = urlsplit(url)
        if (target.scheme != "https" or target.hostname not in hosts
                or target.netloc.lower() not in {target.hostname, f"{target.hostname}:443"}
                or target.fragment):
            raise PermissionError("destination is not allowed")
        path = target.path or "/"
        if target.query:
            path += "?" + target.query
        conn = PublicHTTPS(target.hostname, timeout=8, context=ssl.create_default_context())
        try:
            conn.request(method, path, headers={"Accept-Encoding": "identity"})
            response = conn.getresponse()
            if response.status in (301, 302, 303, 307, 308) and response.getheader("Location"):
                url = urljoin(url, response.getheader("Location"))
                continue
            length = response.getheader("Content-Length")
            if method != "HEAD" and length is not None and int(length) > MAX_BYTES:
                raise OverflowError("source response too large")
            data = response.read(MAX_BYTES + 1)
            if len(data) > MAX_BYTES:
                raise OverflowError("source response too large")
            if method != "HEAD" and length is not None and int(length) != len(data):
                raise OSError("incomplete source response")
            media = response.getheader("Content-Type", "application/octet-stream")
            if not re.fullmatch(r"[ -~]{1,250}", media):
                media = "application/octet-stream"
            return response.status, media, data
        finally:
            conn.close()
    raise ValueError("too many redirects")


class SourceHandler(BaseHTTPRequestHandler):
    timeout = 10

    def address_string(self):
        return "sandbox"

    def do_HEAD(self):
        self.do_GET()

    def do_POST(self):
        self.call_api()

    def call_api(self):
        route = self.server.routes.get(self.path.removeprefix("/api/")) if self.path.startswith("/api/") else None
        if route is None or self.command != route["method"]:
            self.send_error(403, "API route denied")
            return
        try:
            body = None
            if self.command == "POST":
                lengths = self.headers.get_all("Content-Length", [])
                if (len(lengths) != 1 or self.headers.get("Transfer-Encoding")
                        or self.headers.get_content_type() != "application/json"
                        or not re.fullmatch(r"[0-9]{1,7}", lengths[0])):
                    raise ValueError("invalid JSON request framing")
                length = int(lengths[0])
                if not 1 <= length <= MAX_API_BODY:
                    raise OverflowError("API request too large")
                raw = self.rfile.read(length)
                if len(raw) != length or not isinstance(json.loads(raw), dict):
                    raise ValueError("expected a JSON object")
                body = raw
            status, data = invoke_api(route, body)
        except PermissionError:
            self.send_error(403, "API destination denied")
            return
        except OverflowError:
            self.send_error(413, "API message too large")
            return
        except (OSError, http.client.HTTPException, KeyError):
            self.send_error(502, "API unavailable")
            return
        except (ValueError, RecursionError):
            self.send_error(400, "Invalid API request or response")
            return
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/api/"):
            self.call_api()
            return
        try:
            if not self.path.startswith("/https://"):
                raise ValueError("use /https://host/path")
            status, media, body = fetch(self.path[1:], self.server.hosts, self.command)
        except PermissionError:
            self.send_error(403, "Destination denied")
            return
        except ValueError:
            self.send_error(400, "Invalid source URL")
            return
        except OverflowError:
            self.send_error(413, "Download too large")
            return
        except (OSError, http.client.HTTPException):
            self.send_error(502, "Source unavailable")
            return
        self.send_response(status)
        self.send_header("Content-Type", media)
        if self.command != "HEAD":
            self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class LocalRelay(socketserver.BaseRequestHandler):
    def handle(self):
        with socket.socket(socket.AF_UNIX) as upstream:
            upstream.connect(SOCKET)
            peers = {self.request: upstream, upstream: self.request}
            while peers:
                readable, _, _ = select.select(list(peers), [], [], 20)
                if not readable:
                    return
                for sock in readable:
                    data = sock.recv(65536)
                    if data:
                        peers[sock].sendall(data)
                    else:
                        peers.pop(sock).shutdown(socket.SHUT_WR)


def run_proxy(hosts, routes):
    with socketserver.UnixStreamServer(SOCKET, SourceHandler) as server:
        server.routes = routes
        server.hosts = frozenset(normalize_hosts(hosts))
        os.chmod(SOCKET, 0o666)
        server.serve_forever()


def relay_command():
    return ["python", "-I", "-B", "-u", "-c", Path(__file__).read_text(encoding="utf-8"), "relay"]


def start_proxy(container_id, hosts, routes=None):
    routes = routes or {}
    secret_flags = [flag for name in sorted({route["token_env"] for route in routes.values()})
                    for flag in ("--env", name)]
    # Sharing the workload PID namespace ties proxy lifetime to workload PID 1.
    # The helper has a separate network namespace, root filesystem and Unix UID.
    subprocess.run([
        "docker", "run", "--detach", "--rm", "--name", "ls-egress-" + container_id,
        "--pid", "container:" + container_id, "--volumes-from", container_id + ":rw",
        "--network", "bridge", "--read-only", "--user", "0:0",
        "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
        "--memory", "96m", "--pids-limit", "16", "--cpus", "0.5",
        *secret_flags, "python:3.11-alpine", "python", "-I", "-B", "-u", "-c",
        Path(__file__).read_text(encoding="utf-8"), "proxy", json.dumps(routes), *hosts,
    ], stdout=subprocess.DEVNULL, check=True)
    probe = "import socket; s=socket.socket(socket.AF_UNIX); s.connect(" + repr(SOCKET) + "); s.close()"
    deadline = time.monotonic() + 8
    while time.monotonic() < deadline:
        result = subprocess.run(["docker", "exec", container_id, "python", "-I", "-c", probe],
                                capture_output=True, timeout=5)
        if result.returncode == 0:
            return
        time.sleep(0.1)
    raise RuntimeError("egress proxy did not become ready")


if __name__ == "__main__":
    if sys.argv[1] == "proxy":
        run_proxy(sys.argv[3:], json.loads(sys.argv[2]))
    elif sys.argv[1] == "relay":
        with socketserver.ThreadingTCPServer(("127.0.0.1", PORT), LocalRelay) as server:
            server.serve_forever()
