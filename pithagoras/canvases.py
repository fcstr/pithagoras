"""Canvas storage — port of canvases.ts.

Temporary canvases survive tab reconnects, but never a server restart.
Persisted ones live in SQLite.
"""

from __future__ import annotations

import copy
import threading
from typing import Optional

from .db import get_db, _db_lock, _row, _rows, utcnow_iso
from .live_events import nanoid


class CanvasError(Exception):
    pass


class _Emitter:
    """Minimal per-key pub/sub (the EventEmitter the TS version uses)."""

    def __init__(self):
        self._listeners: dict[str, list] = {}
        self._lock = threading.Lock()

    def on(self, key: str, cb) -> None:
        with self._lock:
            self._listeners.setdefault(key, []).append(cb)

    def off(self, key: str, cb) -> None:
        with self._lock:
            lst = self._listeners.get(key)
            if lst and cb in lst:
                lst.remove(cb)

    def emit(self, key: str, event: dict) -> None:
        with self._lock:
            listeners = list(self._listeners.get(key, ()))
        for cb in listeners:
            try:
                cb(event)
            except Exception:
                pass


canvas_events = _Emitter()

# Temporary canvases survive tab reconnects, but never a server restart.
_temporary: dict[str, dict] = {}
_tmp_lock = threading.Lock()


def _update(row: dict, patch: dict) -> dict:
    nxt = {**row, **patch}
    if nxt.get("persisted"):
        with _db_lock:
            get_db().execute(
                "UPDATE canvases SET title=?, content=?, revision=?, status=?, active_call=?, "
                "agent_read_revision=?, updated_at=? WHERE id=? AND session_id=?",
                (
                    nxt["title"], nxt["content"], nxt["revision"], nxt["status"],
                    nxt["active_call"], nxt["agent_read_revision"], nxt["updated_at"],
                    nxt["id"], nxt["session_id"],
                ),
            )
    else:
        with _tmp_lock:
            _temporary[nxt["id"]] = copy.deepcopy(nxt)
    return nxt


def persist_canvas(session: str, canvas_id: str) -> dict:
    row = read_canvas(session, canvas_id)
    if row.get("persisted"):
        return row
    with _db_lock:
        get_db().execute(
            "INSERT INTO canvases (id,session_id,title,content,revision,status,active_call,agent_read_revision,updated_at)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (
                row["id"], row["session_id"], row["title"], row["content"], row["revision"],
                row["status"], row["active_call"], row["agent_read_revision"], row["updated_at"],
            ),
        )
    with _tmp_lock:
        _temporary.pop(canvas_id, None)
    row = {**row, "persisted": True}
    return _notify(row)


def list_canvases(session: str) -> list[dict]:
    with _db_lock:
        stored = _rows(get_db().execute("SELECT *, 1 AS persisted FROM canvases WHERE session_id = ?", (session,)))
    for r in stored:
        r["persisted"] = True
    with _tmp_lock:
        temps = [copy.deepcopy(r) for r in _temporary.values() if r["session_id"] == session]
    return sorted(stored + temps, key=lambda r: r["updated_at"], reverse=True)


def read_canvas(session: str, canvas_id: str) -> dict:
    with _tmp_lock:
        draft = _temporary.get(canvas_id)
        if draft and draft["session_id"] == session:
            return copy.deepcopy(draft)
    with _db_lock:
        row = _row(get_db().execute("SELECT * FROM canvases WHERE session_id = ? AND id = ?", (session, canvas_id)))
    if not row:
        raise CanvasError("Canvas not found in this session")
    row["persisted"] = True
    return row


def focus_canvas(session: str, canvas_id: str) -> dict:
    row = read_canvas(session, canvas_id)
    canvas_events.emit(session, {"type": "focus", "canvas": row})
    return row


def _notify(row: dict) -> dict:
    canvas_events.emit(row["session_id"], {"type": "update", "canvas": row})
    return row


def create_canvas(session: str, title: str) -> dict:
    with _db_lock:
        if not _row(get_db().execute("SELECT id FROM sessions WHERE id = ?", (session,))):
            raise CanvasError("Session not found")
    if not title.strip() or len(title) > 200:
        raise CanvasError("Title must contain 1–200 characters")
    canvas_id = nanoid()
    with _tmp_lock:
        _temporary[canvas_id] = {
            "id": canvas_id, "session_id": session, "title": title.strip(), "content": "",
            "revision": 0, "status": "draft", "active_call": None,
            "agent_read_revision": None, "updated_at": utcnow_iso(), "persisted": False,
        }
    row = read_canvas(session, canvas_id)
    canvas_events.emit(session, {"type": "create", "canvas": row})
    return row


def edit_canvas(session: str, canvas_id: str, revision: int, title: str, content: str) -> dict:
    if not title.strip() or len(title) > 200 or len(content) > 1_000_000:
        raise CanvasError("Invalid canvas title or content size")
    row = read_canvas(session, canvas_id)
    if row["active_call"] or row["revision"] != revision:
        raise CanvasError("Canvas changed or is being written. Reload before editing.")
    _update(row, {
        "title": title.strip(), "content": content,
        "revision": row["revision"] + 1, "status": "edited", "updated_at": utcnow_iso(),
    })
    return _notify(read_canvas(session, canvas_id))


def delete_canvas(session: str, canvas_id: str, revision: int) -> None:
    row = read_canvas(session, canvas_id)
    if row["active_call"] or row["revision"] != revision:
        raise CanvasError("Canvas changed or is being written. Reload before deleting.")
    if row.get("persisted"):
        with _db_lock:
            get_db().execute("DELETE FROM canvases WHERE id = ? AND session_id = ?", (canvas_id, session))
    else:
        with _tmp_lock:
            _temporary.pop(canvas_id, None)
    canvas_events.emit(session, {"type": "delete", "id": canvas_id})


def mark_canvas_read(session: str, canvas_id: str) -> dict:
    row = read_canvas(session, canvas_id)
    _update(row, {"agent_read_revision": row["revision"]})
    return read_canvas(session, canvas_id)


def begin_canvas_write(session: str, canvas_id: str, revision: int, call: str) -> dict:
    row = read_canvas(session, canvas_id)
    if row["agent_read_revision"] != row["revision"]:
        raise CanvasError("Read this canvas with canvas_read before editing; it is unread or was edited by the user.")
    # A future revision cannot describe an older document; recover from model guesses.
    # Keep stale revisions and concurrent writes protected below.
    revision = min(revision, row["revision"])
    if row["active_call"] or row["revision"] != revision:
        raise CanvasError("Canvas changed or is being written. Use its current revision.")
    _update(row, {"active_call": call, "status": "writing"})
    return _notify(read_canvas(session, canvas_id))


def save_canvas_prefix(session: str, canvas_id: str, call: str, content: str) -> dict:
    if len(content) > 1_000_000:
        raise CanvasError("Canvas exceeds one million characters")
    row = read_canvas(session, canvas_id)
    if row["active_call"] != call:
        raise CanvasError("Canvas write is no longer active")
    if row["content"] == content:
        return row
    _update(row, {"content": content, "revision": row["revision"] + 1, "updated_at": utcnow_iso()})
    return _notify(read_canvas(session, canvas_id))


def finish_canvas_write(session: str, canvas_id: str, call: str, interrupted: bool) -> Optional[dict]:
    row = read_canvas(session, canvas_id)
    if row["active_call"] != call:
        return None
    return _notify(_update(row, {
        "active_call": None, "agent_read_revision": row["revision"],
        "status": "interrupted" if interrupted else "saved", "updated_at": utcnow_iso(),
    }))
