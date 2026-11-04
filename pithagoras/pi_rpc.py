"""Client for `pi --mode rpc` — port of pi/rpc-client.ts.

Newline-delimited JSON over the child's stdio. Framing note from pi's docs:
records are separated by LF only, so the buffer is split manually on "\\n".

Threading model: a reader thread per stream (stdout, stderr) dispatches
messages; commands are answered through per-id Events. Every public method
is safe to call from the request threads.
"""

from __future__ import annotations

import json
import subprocess
import threading
from typing import Any, Callable, Optional

from .live_events import nanoid


class PiError(RuntimeError):
    pass


class PiRpcClient:
    """Talks to one pi child process. `on(event_name, cb)` subscribes to
    'event' (a pi message), 'stderr' (a line of text) and 'exit' (code, signal)."""

    def __init__(self, child: subprocess.Popen, session_file: Optional[str] = None):
        self._child = child
        self._session_file = session_file
        self._buffer = ""
        self._pending: dict[str, dict] = {}
        self._closed = False
        self._lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._listeners: dict[str, list[Callable]] = {"event": [], "stderr": [], "exit": [], "error": []}

        assert child.stdout is not None and child.stdin is not None and child.stderr is not None
        self._stdout = child.stdout
        self._stderr = child.stderr
        self._stdin = child.stdin

        threading.Thread(target=self._read_loop, daemon=True).start()
        threading.Thread(target=self._stderr_loop, daemon=True).start()
        threading.Thread(target=self._wait_loop, daemon=True).start()

    # --- subscription ---

    def on(self, name: str, cb: Callable) -> None:
        self._listeners.setdefault(name, []).append(cb)

    def off(self, name: str, cb: Callable) -> None:
        try:
            self._listeners.get(name, []).remove(cb)
        except ValueError:
            pass

    def _emit(self, name: str, *args) -> None:
        for cb in list(self._listeners.get(name, [])):
            try:
                cb(*args)
            except Exception:
                import traceback
                traceback.print_exc()

    # --- reader threads ---

    @staticmethod
    def _read_chunk(stream) -> bytes:
        # Raw FileIO (bufsize=0) has no read1; BufferedReader has no readinto
        # on some builds. Both have read(), which blocks until at least one
        # byte is available, which is all the framing needs.
        try:
            return stream.read1(65536)  # type: ignore[attr-defined]
        except AttributeError:
            return stream.read(65536)

    def _read_loop(self) -> None:
        try:
            while True:
                chunk = self._read_chunk(self._stdout)
                if not chunk:
                    break
                self._consume(chunk.decode("utf-8", "replace"))
        except (OSError, ValueError):
            pass

    def _stderr_loop(self) -> None:
        try:
            while True:
                chunk = self._read_chunk(self._stderr)
                if not chunk:
                    break
                self._emit("stderr", chunk.decode("utf-8", "replace"))
        except (OSError, ValueError):
            pass

    def _wait_loop(self) -> None:
        code = self._child.wait()
        with self._lock:
            already = self._closed
            self._closed = True
            pending = list(self._pending.values())
            self._pending.clear()
        for p in pending:
            p["timer"].cancel()
            p["reject"](PiError(f"pi exited (code={code}) before replying"))
        if not already:
            self._emit("exit", code, None)

    # --- protocol ---

    def _consume(self, chunk: str) -> None:
        self._buffer += chunk
        # Split on LF only — see framing note above.
        lines = self._buffer.split("\n")
        self._buffer = lines.pop()
        for line in lines:
            trimmed = line.strip()
            if not trimmed:
                continue
            try:
                msg = json.loads(trimmed)
            except ValueError:
                # A non-JSON line is pi logging to stdout; surface it rather than crash.
                self._emit("stderr", trimmed + "\n")
                continue
            self._dispatch(msg)

    def _dispatch(self, msg: dict) -> None:
        if msg.get("type") == "response" and msg.get("id"):
            with self._lock:
                waiter = self._pending.pop(str(msg["id"]), None)
            if waiter:
                waiter["timer"].cancel()
                waiter["resolve"](msg)
                return
        self._emit("event", msg)

    def _write(self, payload: str) -> None:
        with self._write_lock:
            self._stdin.write(payload.encode())
            self._stdin.flush()

    def send(self, type_: str, params: Optional[dict] = None, timeout_ms: int = 30_000) -> dict:
        """Send a command and wait for its matching `response`."""
        if self._closed:
            raise PiError("pi process is not running")
        id_ = nanoid(12)
        done = threading.Event()
        result: dict[str, Any] = {}

        def resolve(msg):
            result["msg"] = msg
            done.set()

        def reject(err: Exception):
            result["err"] = err
            done.set()

        timer = threading.Timer(timeout_ms / 1000, lambda: reject(PiError(f"pi command '{type_}' timed out after {timeout_ms}ms")))
        with self._lock:
            self._pending[id_] = {"resolve": resolve, "reject": reject, "timer": timer}
        timer.daemon = True
        timer.start()
        try:
            self._write(json.dumps({"id": id_, "type": type_, **(params or {})}) + "\n")
        except (OSError, ValueError) as e:
            with self._lock:
                self._pending.pop(id_, None)
            timer.cancel()
            raise PiError(str(e))
        done.wait(timeout_ms / 1000 + 5)
        if "err" in result:
            raise result["err"]
        if "msg" not in result:
            raise PiError(f"pi command '{type_}' timed out after {timeout_ms}ms")
        return result["msg"]

    # --- commands ---

    def prompt(self, message: str, options: Optional[dict] = None) -> None:
        """Submit a prompt. Returns as soon as pi accepts it — the actual work
        streams back as events, which is what lets a task keep running after
        the browser that started it has gone away."""
        res = self.send("prompt", {"message": message})
        if res.get("success") is False:
            raise PiError(res.get("error") or "pi rejected the prompt")

    def abort(self) -> None:
        try:
            self.send("abort")
        except PiError:
            pass

    def dispose(self) -> None:
        if not self._closed:
            try:
                self._child.terminate()
            except OSError:
                pass

    def _data(self, type_: str, params: Optional[dict] = None, timeout_ms: int = 30_000) -> Any:
        res = self.send(type_, params, timeout_ms)
        if res.get("success") is False:
            raise PiError(res.get("error") or f"pi rejected '{type_}'")
        return res.get("data")

    def get_state(self) -> dict:
        return self._data("get_state")

    def get_stats(self) -> dict:
        return self._data("get_session_stats")

    def get_thinking_levels(self) -> list:
        d = self._data("get_available_thinking_levels")
        return (d or {}).get("levels") or []

    def get_models(self) -> list:
        d = self._data("get_available_models")
        return (d or {}).get("models") or []

    def get_commands(self) -> list:
        d = self._data("get_commands")
        return (d or {}).get("commands") or []

    def set_model(self, provider: str, model_id: str) -> None:
        # pi resolves the pair as `${provider}/${modelId}`; both are required.
        self._data("set_model", {"provider": provider, "modelId": model_id})

    def set_thinking_level(self, level: str) -> None:
        self._data("set_thinking_level", {"level": level})

    def set_auto_compaction(self, enabled: bool) -> None:
        self._data("set_auto_compaction", {"enabled": enabled})

    def set_auto_retry(self, enabled: bool) -> None:
        self._data("set_auto_retry", {"enabled": enabled})

    def compact(self) -> None:
        # Compaction takes a model call and a minute; the default 30s timeout
        # is not enough.
        self._data("compact", timeout_ms=600_000)

    def reload(self) -> None:
        self._data("reload")

    def refresh_settings(self) -> None:
        self.reload()

    def export_session(self, target: Optional[str] = None) -> str:
        raise PiError("Export is not available over the RPC protocol")

    def respond_ui(self, id_: str, response: dict) -> bool:
        """RPC mode answers dialogs with its own message type."""
        try:
            self._write(json.dumps({"type": "extension_ui_response", "id": id_, **response}) + "\n")
            return True
        except (OSError, ValueError):
            return False

    @property
    def running(self) -> bool:
        return not self._closed

    @property
    def session_file(self) -> Optional[str]:
        return self._session_file

    def is_idle(self) -> bool:
        """The RPC protocol cannot see inside pi; the run's own lifecycle
        events are the only signal."""
        return False
