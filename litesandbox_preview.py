"""Loopback-only, read-only HTTP previews for offline Alpine sandboxes."""

import base64
import hashlib
import hmac
import io
import re
import secrets
import struct
import subprocess
import time
from http.client import HTTPException, HTTPResponse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import BoundedSemaphore


MAX_RESPONSE = 4 * 1024 * 1024
APP_PORT = 3000


class BufferedSocket:
    def __init__(self, data: bytes) -> None:
        self.data = data

    def makefile(self, mode: str) -> io.BytesIO:
        return io.BytesIO(self.data)


class PreviewGateway(ThreadingHTTPServer):
    def __init__(self, runtime, port: int) -> None:
        self.runtime = runtime
        self.secret = secrets.token_bytes(32)
        self.capacity = BoundedSemaphore(8)
        super().__init__(("127.0.0.1", port), PreviewHandler)

    def url(self, sandbox_id: str, deadline: int) -> str:
        payload = bytes.fromhex(sandbox_id) + struct.pack(">Q", deadline)
        signature = hmac.digest(self.secret, payload, hashlib.sha256)[:12]
        host = base64.b32encode(payload + signature).decode().lower().rstrip("=")
        return f"http://{host}.localhost:{self.server_port}/"

    def resolve(self, host: str) -> str | None:
        match = re.fullmatch(rf"([a-z2-7]{{58}})\.localhost:{self.server_port}", host.lower())
        if not match:
            return None
        token = base64.b32decode(match[1].upper() + "======")
        payload, signature = token[:-12], token[-12:]
        expected = hmac.digest(self.secret, payload, hashlib.sha256)[:12]
        if not hmac.compare_digest(signature, expected):
            return None
        if int.from_bytes(payload[16:], "big") <= time.time():
            return None
        return payload[:16].hex()


class PreviewHandler(BaseHTTPRequestHandler):
    server: PreviewGateway

    def do_GET(self) -> None:
        self.proxy()

    def do_HEAD(self) -> None:
        self.proxy()

    def proxy(self) -> None:
        sandbox_id = self.server.resolve(self.headers.get("Host", ""))
        if sandbox_id is None:
            self.send_error(404)
            return
        if (not self.path.startswith("/") or len(self.path) > 4096
                or any(not 33 <= ord(ch) <= 126 for ch in self.path)):
            self.send_error(400)
            return
        if self.headers.get("Service-Worker"):
            self.send_error(403)
            return
        if not self.server.capacity.acquire(blocking=False):
            self.send_error(503)
            return
        try:
            self.forward(sandbox_id)
        except subprocess.TimeoutExpired:
            self.send_error(504)
        except (OSError, subprocess.CalledProcessError, HTTPException, ValueError, RuntimeError):
            self.send_error(502)
        finally:
            self.server.capacity.release()

    def forward(self, sandbox_id: str) -> None:
        runtime = self.server.runtime
        record = runtime.list_container_records().get(sandbox_id)
        if record is None or record.expired(time.time()):
            self.send_error(404)
            return

        request = (
            f"{self.command} {self.path} HTTP/1.0\r\n"
            f"Host: localhost:{APP_PORT}\r\nConnection: close\r\n\r\n"
        ).encode("ascii")
        # Only the HTTP request goes over stdin. The relay command is constant.
        relay = f"/bin/busybox timeout 5 /bin/busybox nc 127.0.0.1 {APP_PORT} | /bin/busybox head -c {MAX_RESPONSE + 1}"
        result = subprocess.run(
            [*runtime.exec_prefix(record.container_id, interactive=True), "sh", "-c", relay],
            input=request, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=8, check=True,
        )
        if len(result.stdout) > MAX_RESPONSE:
            self.send_error(502, "Preview response too large")
            return
        with HTTPResponse(BufferedSocket(result.stdout), method=self.command) as upstream:
            upstream.begin()
            if not 200 <= upstream.status <= 599:
                self.send_error(502)
                return
            declared = upstream.length
            body = upstream.read(MAX_RESPONSE + 1)
            if declared is not None and len(body) != declared:
                self.send_error(502, "Incomplete upstream response")
                return
            media = upstream.getheader("Content-Type", "application/octet-stream")
            if not re.fullmatch(r"[\x20-\x7e]{1,256}", media):
                media = "application/octet-stream"
            self.send_response(upstream.status)
            self.send_header("Content-Type", media)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Content-Security-Policy",
                             "default-src 'self'; script-src 'self' 'unsafe-inline'; "
                             "style-src 'self' 'unsafe-inline'; object-src 'none'; "
                             "worker-src 'none'; form-action 'none'; base-uri 'none'")
            if self.command != "HEAD":
                self.send_header("Content-Length", str(len(body)))
            try:
                self.end_headers()
                if body:
                    self.wfile.write(body)
            except ConnectionError:
                # A browser can cancel asset requests when the tab closes.
                self.close_connection = True
