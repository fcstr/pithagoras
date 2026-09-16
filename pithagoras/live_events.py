"""One current snapshot per message/tool, never a growing list of token events.

Port of live-events.ts. Streaming deltas are kept in memory and delivered live
with a negative seq; only completed messages and tool results hit SQLite.
"""

from __future__ import annotations

import json
import secrets
import time
from datetime import datetime, timezone
from typing import Any, Callable, Optional


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def nanoid(size: int = 21) -> str:
    # URL-safe alphabet like the JS package.
    return secrets.token_urlsafe(size * 2)[:size].replace("-", "_")


class LiveEvents:
    def __init__(self, store: Callable[[str, str, Any], dict]):
        self._store = store
        self._messages: dict[str, dict] = {}
        self._tools: dict[str, dict[str, dict]] = {}
        self._sequence = -int(time.time() * 1000) * 1000

    def _live(self, session: str, type_: str, payload: Any, at: Optional[str] = None) -> dict:
        self._sequence -= 1
        return {
            "seq": self._sequence,
            "session_id": session,
            "type": type_,
            "payload": json.dumps(payload),
            "created_at": at or _now_iso(),
        }

    def record(self, session: str, type_: str, payload: Any) -> dict:
        if type_ == "message_update":
            state = self._messages.get(session)
            if state is None:
                state = {
                    "streamId": nanoid(),
                    "message": {"role": "assistant", "content": []},
                    "at": _now_iso(),
                }
                self._messages[session] = state
            inner = (payload or {}).get("assistantMessageEvent")
            message = (inner or {}).get("partial") or (payload or {}).get("message")
            if message:
                state["message"] = message
            elif isinstance((inner or {}).get("delta"), str) and (inner or {}).get("type") in (
                "text_delta",
                "thinking_delta",
            ):
                index = inner.get("contentIndex")
                index = index if isinstance(index, int) else 0
                kind = "text" if inner["type"] == "text_delta" else "thinking"
                content = state["message"].setdefault("content", [])
                while len(content) <= index:
                    content.append(None)
                block = content[index]
                if not isinstance(block, dict) or kind not in block:
                    block = {"type": kind, kind: ""}
                    content[index] = block
                block[kind] = block.get(kind, "") + inner["delta"]
            # The SDK's full snapshot stays in this buffer; clients receive only the delta.
            update = {k: v for k, v in (inner or {}).items() if k != "partial"}
            return self._live(
                session,
                type_,
                {"type": type_, "streamId": state["streamId"], "assistantMessageEvent": update},
            )
        if type_ == "tool_execution_update":
            row = self._live(session, type_, payload)
            tools = self._tools.setdefault(session, {})
            tools[str((payload or {}).get("toolCallId") or (payload or {}).get("toolName") or "")] = row
            return row
        if type_ == "message_end" and (payload or {}).get("message", {}).get("role") == "assistant":
            state = self._messages.get(session)
            body = dict(payload)
            if state:
                body["streamId"] = state["streamId"]
            row = self._store(session, type_, body)
            self._messages.pop(session, None)
            return row
        if type_ == "tool_execution_end":
            row = self._store(session, type_, payload)
            tools = self._tools.get(session)
            if tools is not None:
                tools.pop(str((payload or {}).get("toolCallId") or (payload or {}).get("toolName") or ""), None)
                if not tools:
                    self._tools.pop(session, None)
            return row
        return self._store(session, type_, payload)

    def snapshot(self, session: str) -> list[dict]:
        state = self._messages.get(session)
        rows = (
            [
                self._live(
                    session,
                    "message_snapshot",
                    {"streamId": state["streamId"], "message": state["message"]},
                    state["at"],
                )
            ]
            if state
            else []
        )
        rows.extend(self._tools.get(session, {}).values())
        return rows

    def clear(self, session: str) -> None:
        self._messages.pop(session, None)
        self._tools.pop(session, None)
