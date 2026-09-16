"""Minimal HTTP framework on top of http.server (stdlib only).

Provides just enough of Express for this app: path patterns with `:param`
segments, JSON bodies, cookies, query parsing, and SSE-friendly raw writes.

Handlers receive a `Request` and return either:
  * a `Response` (status, body, headers),
  * a tuple `(status, obj)` which is JSON-encoded, or
  * `None` after writing directly to `request.wfile` (SSE and streams).

Threading model: `ThreadingHTTPServer` gives each connection its own thread,
which is what the long-lived SSE connections need without asyncio.
"""

from __future__ import annotations

import json
import re
import threading
import urllib.parse
from http import HTTPStatus
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from http.server import HTTPServer as _HTTPServer
from socketserver import ThreadingMixIn
from typing import Any, Callable, Optional


class Request:
    def __init__(self, handler: "Handler", params: dict[str, str], body: bytes):
        self.handler = handler
        self.method = handler.command
        self.path = handler.path.split("?", 1)[0]
        self.query = {
            k: v[-1] if v else ""
            for k, v in urllib.parse.parse_qs(
                urllib.parse.urlsplit(handler.path).query, keep_blank_values=True
            ).items()
        }
        self.params = params
        self.headers = handler.headers
        self._body = body
        self._json: Any = ...
        self._cookies: Optional[dict[str, str]] = None

    @property
    def body(self) -> bytes:
        return self._body

    @property
    def json(self) -> Any:
        if self._json is ...:
            try:
                self._json = json.loads(self._body) if self._body else None
            except (ValueError, UnicodeDecodeError):
                self._json = None
        return self._json

    @property
    def cookies(self) -> dict[str, str]:
        if self._cookies is None:
            jar = SimpleCookie()
            raw = self.headers.get("Cookie")
            if raw:
                try:
                    jar.load(raw)
                except Exception:
                    pass
            self._cookies = {k: m.value for k, m in jar.items()}
        return self._cookies

    # Raw stream access for SSE handlers.
    def write(self, data: bytes) -> bool:
        """Write raw bytes; False when the client went away."""
        try:
            self.handler.wfile.write(data)
            self.handler.wfile.flush()
            return True
        except (BrokenPipeError, ConnectionResetError, OSError):
            return False

    @property
    def wfile(self):
        return self.handler.wfile

    @property
    def client_gone(self) -> bool:
        return getattr(self.handler, "_client_gone", False)


class Response:
    def __init__(
        self,
        status: int = 200,
        body: Any = b"",
        headers: Optional[dict[str, str]] = None,
        content_type: Optional[str] = None,
    ):
        self.status = status
        self.body = body
        self.headers = headers or {}
        self.content_type = content_type


class HTTPError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


Route = tuple[str, re.Pattern, Callable[[Request], Any]]
_Middleware = Callable[[Request], Optional[Any]]


def _compile(path: str) -> re.Pattern:
    # "/api/sessions/:id/events" -> named groups
    pattern = re.sub(r":([A-Za-z_][A-Za-z0-9_]*)", r"(?P<\1>[^/]+)", path)
    return re.compile(f"^{pattern}$")


class App:
    """Route table shared by the handler. Not thread-local; routes are fixed at boot."""

    def __init__(self):
        self.routes: list[Route] = []
        self.middleware: list[tuple[str, _Middleware]] = []
        self.fallback: Optional[Callable[[Request], Any]] = None

    def add(self, method: str, path: str, handler: Callable[[Request], Any]):
        self.routes.append((method.upper(), _compile(path), handler))

    def get(self, path: str):
        def deco(fn):
            self.add("GET", path, fn)
            return fn
        return deco

    def post(self, path: str):
        def deco(fn):
            self.add("POST", path, fn)
            return fn
        return deco

    def put(self, path: str):
        def deco(fn):
            self.add("PUT", path, fn)
            return fn
        return deco

    def patch(self, path: str):
        def deco(fn):
            self.add("PATCH", path, fn)
            return fn
        return deco

    def delete(self, path: str):
        def deco(fn):
            self.add("DELETE", path, fn)
            return fn
        return deco

    def use(self, prefix: str, fn: _Middleware):
        """Middleware: return a Response to short-circuit, None to continue."""
        self.middleware.append((prefix, fn))

    def dispatch(self, req: Request) -> Any:
        for prefix, fn in self.middleware:
            if req.path.startswith(prefix):
                out = fn(req)
                if out is not None:
                    return out
        for method, pattern, handler in self.routes:
            if method != req.method:
                continue
            m = pattern.match(req.path)
            if m:
                req.params = {k: urllib.parse.unquote(v) for k, v in m.groupdict().items()}
                return handler(req)
        if self.fallback:
            return self.fallback(req)
        raise HTTPError(404, "Not found")


def json_response(obj: Any, status: int = 200, headers: Optional[dict[str, str]] = None) -> Response:
    return Response(status, json.dumps(obj).encode(), headers, "application/json")


def error_response(status: int, message: str) -> Response:
    return json_response({"error": message}, status)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    app: App = None  # type: ignore[assignment]
    server_version = "pithagoras/0.1"

    # Quieter logging, matching the portal's sparse output.
    def log_message(self, fmt: str, *args):  # noqa: A003
        pass

    def _read_body(self) -> bytes:
        length = self.headers.get("Content-Length")
        if not length:
            return b""
        try:
            n = int(length)
        except ValueError:
            return b""
        if n <= 0 or n > 16 * 1024 * 1024:
            return b""
        return self.rfile.read(n)

    def _handle(self):
        body = self._read_body()
        req = Request(self, {}, body)
        try:
            out = self.app.dispatch(req)
        except HTTPError as e:
            out = error_response(e.status, e.message)
        except (BrokenPipeError, ConnectionResetError):
            return
        except Exception as e:  # handler bug — say so, don't hang the socket
            import traceback
            traceback.print_exc()
            out = error_response(500, str(e) or e.__class__.__name__)
        if out is None:
            # Handler wrote the response itself (SSE, streams).
            return
        self._send(out)

    def _send(self, out: Any):
        if isinstance(out, tuple):
            status, obj = out[0], out[1]
            out = json_response(obj, status)
        elif isinstance(out, (dict, list)):
            out = json_response(out)
        elif isinstance(out, str):
            out = Response(200, out.encode(), content_type="text/plain; charset=utf-8")
        elif isinstance(out, bytes):
            out = Response(200, out)
        if not isinstance(out, Response):
            out = json_response(out)

        body = out.body
        if isinstance(body, str):
            body = body.encode()
        try:
            self.send_response(out.status)
            ctype = out.content_type or (
                "application/json" if out.headers.get("Content-Type") is None and _looks_json(body) else None
            )
            if ctype and "Content-Type" not in out.headers:
                self.send_header("Content-Type", ctype)
            for k, v in out.headers.items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        self._handle()

    def do_POST(self):
        self._handle()

    def do_PUT(self):
        self._handle()

    def do_PATCH(self):
        self._handle()

    def do_DELETE(self):
        self._handle()

    def do_HEAD(self):
        self._handle()

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()


def _looks_json(body: bytes) -> bool:
    return body[:1] in (b"{", b"[")


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def make_server(app: App, port: int, host: str = "0.0.0.0") -> Server:
    handler_cls = type("BoundHandler", (Handler,), {"app": app})
    return Server((host, port), handler_cls)


def set_cookie_header(name: str, value: str, *, max_age: Optional[int] = None,
                      http_only: bool = True, path: str = "/", same_site: str = "Lax",
                      secure: bool = False) -> str:
    parts = [f"{name}={value}", f"Path={path}", f"SameSite={same_site}"]
    if max_age is not None:
        parts.append(f"Max-Age={max_age}")
    if http_only:
        parts.append("HttpOnly")
    if secure:
        parts.append("Secure")
    return "; ".join(parts)
