"""Owns every running pi process — port of session-manager.ts.

The important property: a run is tied to this manager, not to any HTTP
request. Once a prompt is accepted the browser can disappear — output keeps
streaming into the event log, and a later reconnect replays it.

The TS version is single-threaded async; here a monitor thread per session
does the waiting and the HTTP handler threads only kick work off. All shared
maps are guarded by one lock.
"""

from __future__ import annotations

import json
import os
import re
import threading
from typing import Any, Callable, Optional

from .db import (
    append_event,
    get_session,
    get_settings,
    mark_orphaned_sessions_interrupted,
    browser_allowed,
    browser_allowlist,
    routine_guards,
    update_session,
)
from .executors import build_executor, EXECUTOR_KIND
from .live_events import LiveEvents
from .pi_builtins import find_server_builtin, run_builtin

# Thinking markers that escape into the answer.
#
# A reasoning model sometimes closes a thought inside the text it means to say,
# and a stray </think> then travels to whoever is reading — a chat window, a
# Telegram message. Stripped where the text leaves the portal rather than in
# the stored events, so the record of what the model actually produced stays
# intact.
_THINKING = re.compile(r"</?think(ing)?>", re.IGNORECASE)


def strip_thinking_markers(text: str) -> str:
    return _THINKING.sub("", text).strip()


def summarize_tool_input(p: dict) -> Optional[str]:
    """Mirrors what the web transcript shows, so a chat and the UI agree."""
    input_ = p.get("input") or p.get("args") or p.get("parameters")
    if not input_:
        return None

    def trim(v: str) -> str:
        return v[:79] + "…" if len(v) > 80 else v

    if isinstance(input_, str):
        return trim(input_)
    if isinstance(input_, dict):
        first = input_.get("command") or input_.get("file_path") or input_.get("path") or input_.get("pattern") or input_.get("query")
        if isinstance(first, str):
            return trim(first)
        return trim(json.dumps(input_))
    return None


SESSION_ROOT = os.path.abspath(os.environ.get("SESSION_DIR", "./data/sessions"))

# Events that must not be persisted.
#
# Beyond noise, extension dialogs are strictly live: a stored
# extension_ui_request would be replayed to every future reader, so reloading
# the page reopened a dialog whose extension had long since stopped waiting.
EPHEMERAL_EVENTS = {
    "queue_update",
    "extension_ui_request",
    "extension_ui_cancel",
    # Prefill progress: a hundred rows per long prompt, and meaningless once the
    # answer has arrived. Delivered to whoever is watching, never stored.
    "portal_prefill",
}

SETTLE_TIMEOUT_MS = 15 * 60_000
COMPACT_SETTLE_MS = 60_000


class SessionManager:
    def __init__(self):
        self._live: dict[str, dict] = {}  # id -> {"client": ..., "executor": ...}
        self._stream = LiveEvents(append_event)
        self._listeners: dict[str, list[Callable]] = {}
        self._lock = threading.RLock()
        # In-flight ask() per session, so messages in one chat are answered in turn.
        self._asking: dict[str, threading.Lock] = {}
        # Who sent the message being handled, per session. Per message rather
        # than per session because a group conversation has many senders.
        self._speaker: dict[str, dict] = {}
        # Compaction in flight, per session. A cancelled one is still running
        # until pi has unwound it, and both Stop and the next prompt have to wait.
        self._compacting: dict[str, threading.Event] = {}
        # A Stop that arrived before there was anything to stop.
        self._cancel_pending: set[str] = set()
        # Client startup in flight, so two callers cannot launch two of them.
        self._starting: dict[str, threading.Event] = {}
        self._start_result: dict[str, Any] = {}

        os.makedirs(SESSION_ROOT, exist_ok=True)
        orphaned = mark_orphaned_sessions_interrupted()
        if orphaned > 0:
            print(f"[portal] marked {orphaned} session(s) interrupted (server restarted mid-run)")

    # --- subscription (one event name per session, like the TS EventEmitter) ---

    def on(self, name: str, cb: Callable) -> None:
        with self._lock:
            self._listeners.setdefault(name, []).append(cb)

    def off(self, name: str, cb: Callable) -> None:
        with self._lock:
            try:
                self._listeners.get(name, []).remove(cb)
            except ValueError:
                pass

    def _emit(self, name: str, row: dict) -> None:
        with self._lock:
            listeners = list(self._listeners.get(name, []))
        for cb in listeners:
            try:
                cb(row)
            except Exception:
                import traceback
                traceback.print_exc()

    # --- status ---

    def is_running(self, session_id: str) -> bool:
        with self._lock:
            live = self._live.get(session_id)
        return bool(live and live["client"].running)

    def live_snapshot(self, session_id: str) -> list[dict]:
        return self._stream.snapshot(session_id)

    def _mark(self, session_id: str, status: str) -> None:
        """Move a session's status and tell whoever is watching, in that order."""
        current = get_session(session_id)
        if current and current.get("status") == status:
            return
        if status == "running":
            update_session(session_id, {"status": status, "last_error": None})
        else:
            update_session(session_id, {"status": status})
        self._record(session_id, "portal_status", {"status": status})

    def _record(self, session_id: str, type_: str, payload: Any) -> None:
        """Stream updates in memory; persist completed messages and lifecycle metadata."""
        if type_ in EPHEMERAL_EVENTS:
            # Still deliver it to anyone attached right now, with a negative seq so
            # it can never be confused with a stored event during replay.
            import time
            self._emit(f"session:{session_id}", {
                "seq": -int(time.time() * 1000),
                "session_id": session_id,
                "type": type_,
                "payload": json.dumps(payload),
            })
            return
        row = self._stream.record(session_id, type_, payload)
        self._emit(f"session:{session_id}", row)

    # --- client lifecycle ---

    def _ensure_client(self, session_id: str):
        """Start pi for a session, once.

        Two callers arriving on a cold session both used to get past the check
        and both launch; the second dropped the first on the floor. The
        starting map makes them share the one launch.
        """
        with self._lock:
            live = self._live.get(session_id)
            if live and live["client"].running:
                return live["client"]
            ev = self._starting.get(session_id)
            if ev is None:
                ev = threading.Event()
                self._starting[session_id] = ev
                leader = True
            else:
                leader = False
        if leader:
            try:
                self._start_result[session_id] = self._start_client(session_id)
            except Exception as e:
                self._start_result[session_id] = e
            finally:
                with self._lock:
                    self._starting.pop(session_id, None)
                ev.set()
        else:
            ev.wait()
        result = self._start_result.pop(session_id, None) if leader else self._start_result.get(session_id)
        if isinstance(result, Exception):
            raise result
        if result is None:
            raise RuntimeError(f"Failed to start pi for session {session_id}")
        return result

    def _start_client(self, session_id: str):
        session = get_session(session_id)
        if not session:
            raise RuntimeError(f"Unknown session {session_id}")

        executor = build_executor(EXECUTOR_KIND, SESSION_ROOT)
        os.makedirs(os.path.join(SESSION_ROOT, session_id), exist_ok=True)

        # The session's own choices win over the portal defaults. Without this a
        # restart relaunched pi on the default model, quietly undoing the pick.
        settings = get_settings()
        client = executor.launch({
            "sessionId": session_id,
            "workspacePath": session["workspace"],
            "provider": session.get("provider") or settings.get("provider"),
            "model": session.get("model") or settings.get("model"),
            "thinkingLevel": session.get("thinking_level") or settings.get("thinkingLevel"),
            "sessionFile": session.get("pi_session_file"),
            # Channels only. A task session works inside somebody's repository and
            # has no business rescheduling anything; a routine run is excluded too,
            # since a routine that can create routines can build a chain unwatched.
            "routineTools": session.get("kind") == "agent",
            # A routine run gets the report tool instead: it is the one kind of
            # session with nobody on the other end to read what it found.
            "routineSlug": session.get("routine_slug") if session.get("kind") == "routine" else None,
            # A routine may be exempted from the taint rules; nothing else can be.
            "enforceTaint": routine_guards(session.get("routine_slug")) if session.get("kind") == "routine" else True,
            "role": session.get("role"),
        })

        # pi writes the file lazily, so it usually does not exist yet at launch.
        # Recorded the first time it appears; from then on this exact conversation
        # is what gets reopened.
        recorded = {"file": session.get("pi_session_file")}

        def remember_session_file():
            if recorded["file"]:
                return
            file = client.session_file
            if not file:
                return
            recorded["file"] = file
            update_session(session_id, {"pi_session_file": file})

        remember_session_file()

        def on_event(msg):
            remember_session_file()
            self._record(session_id, msg.get("type"), msg)
            # Status follows pi's own run state rather than being guessed at.
            # agent_start covers a run nobody here asked for — a queued follow-up
            # picked up on its own, a routine, a message through a channel.
            if msg.get("type") == "agent_start":
                self._mark(session_id, "running")
            # agent_settled, not agent_end: agent_end fires once per agent run, and
            # a run is followed by retries, auto-compaction and any queued message,
            # all of it still the model working.
            if msg.get("type") == "agent_settled":
                self._mark(session_id, "idle")

        def on_stderr(chunk: str):
            text = chunk.strip()
            if text:
                self._record(session_id, "stderr", {"text": text})

        def on_exit(code, _signal):
            with self._lock:
                self._live.pop(session_id, None)
            self._stream.clear(session_id)
            current = get_session(session_id)
            # A clean exit after a finished run is normal; anything else is a failure
            # worth surfacing in the UI rather than leaving as a silent stall.
            if current and current.get("status") == "running":
                message = f"pi exited unexpectedly (code={code})"
                update_session(session_id, {"status": "error", "last_error": message})
                self._record(session_id, "portal_status", {"status": "error", "error": message})
            try:
                executor.cleanup(session_id)
            except Exception:
                pass

        client.on("event", on_event)
        client.on("stderr", on_stderr)
        client.on("exit", on_exit)

        with self._lock:
            self._live[session_id] = {"client": client, "executor": executor}
        return client

    # --- prompting ---

    def prompt(self, session_id: str, message: str, options: Optional[dict] = None) -> None:
        """Submit a prompt. Returns as soon as pi accepts it; the run continues
        regardless of what the browser does next."""
        self._mark(session_id, "running")
        # A session mid-compaction is detached from agent events, and a prompt
        # started there is invisible.
        self._settle_compaction(session_id)
        # The compaction published idle on its way out, after this prompt had
        # already claimed the session. Reclaim it.
        self._mark(session_id, "running")
        try:
            self._submit(session_id, message, options or {})
        except Exception as e:
            update_session(session_id, {"status": "error", "last_error": str(e)})
            self._record(session_id, "portal_status", {"status": "error", "error": str(e)})
            raise

    def _submit(self, session_id: str, message: str, options: dict) -> None:
        client = self._ensure_client(session_id)

        # A slash command is an instruction to the agent, not something said in
        # the conversation, so it should not appear as a chat message.
        is_command = self._looks_like_command(client, message)

        # Portal builtins never reach the model — they act on the session itself.
        m = re.match(r"^/([\w-]+)\s*(.*)$", message.strip())
        server_builtin = find_server_builtin(m.group(1)) if m else None
        if server_builtin:
            # Run in a thread: /compact is a model call and would hold the
            # request open. Same contract as a prompt — accept it, report
            # through the event stream.
            def run():
                try:
                    text = run_builtin(server_builtin["name"], m.group(2), client)
                    self._record(session_id, "portal_notice", {"text": text})
                except Exception as e:
                    self._record(session_id, "portal_notice", {"text": str(e), "error": True})
                finally:
                    self._mark(session_id, "idle")

            threading.Thread(target=run, daemon=True).start()
            return

        if not is_command:
            payload: dict[str, Any] = {"message": message}
            if options.get("voice"):
                payload["voice"] = True
            self._record(session_id, "portal_prompt", payload)
        client.prompt(message, options)
        # A slash command completes inside prompt() without ever starting an
        # agent turn, so no agent_settled arrives to clear the status.
        if client.is_idle():
            self._mark(session_id, "idle")

    def _looks_like_command(self, client, message: str) -> bool:
        """True when the message invokes a command pi actually knows about."""
        m = re.match(r"^/([\w:-]+)", message.strip())
        if not m:
            return False
        try:
            return any(c.get("name") == m.group(1) for c in client.get_commands())
        except Exception:
            return False

    # --- ask: prompt and wait for the answer (channels) ---

    def ask(self, session_id: str, message: str, *, timeout_ms: int = SETTLE_TIMEOUT_MS,
            on_reply: Optional[Callable[[str], None]] = None, stream_text: bool = True,
            on_ui: Optional[Callable[[dict], None]] = None) -> str:
        """Prompt and wait for the answer.

        The inverse of prompt(), which returns the moment pi accepts a message.
        A channel needs the opposite: somebody is sitting in a chat waiting for
        a reply, so this blocks until the turn finishes and hands back what the
        agent said.

        Serialised per session: two messages arriving in the same chat while
        the agent is still working would otherwise interleave.
        """
        with self._lock:
            lock = self._asking.setdefault(session_id, threading.Lock())
        with lock:
            return self._ask_now(session_id, message, timeout_ms, on_reply, stream_text, on_ui)

    def _ask_now(self, session_id: str, message: str, timeout_ms: int,
                 on_reply: Optional[Callable[[str], None]], stream_text: bool,
                 on_ui: Optional[Callable[[dict], None]]) -> str:
        self._ensure_client(session_id)

        # pi emits one assistant message per stretch of talking, broken up by
        # tool calls. Each is flushed as it closes so a channel can relay
        # progress rather than sitting silent while a long task runs.
        state = {"current": "", "all": []}
        settled = threading.Event()
        failure: list[Optional[Exception]] = [None]

        def relay(line: str):
            # Delivery is the channel's problem; a failure there must not take
            # down the run that produced it.
            if on_reply:
                try:
                    on_reply(line)
                except Exception:
                    pass

        def flush():
            done = state["current"].strip()
            state["current"] = ""
            if not done:
                return
            state["all"].append(done)
            if stream_text:
                relay(done)

        def on_event(row):
            try:
                payload = json.loads(row.get("payload") or "{}")
            except ValueError:
                return
            type_ = row.get("type")
            if type_ == "message_update":
                inner = payload.get("assistantMessageEvent") or {}
                # Thinking deltas are not the answer, and nobody in a chat wants them.
                if inner.get("type") == "text_delta" and isinstance(inner.get("delta"), str):
                    state["current"] += inner["delta"]
            elif type_ == "message_end":
                flush()
            elif type_ == "tool_execution_start":
                if not on_reply:
                    return
                # Prose first: a tool line landing mid-sentence reads badly.
                flush()
                name = str(payload.get("toolName") or payload.get("name") or "tool")
                detail = summarize_tool_input(payload)
                relay(f"⚙ {name} · {detail}" if detail else f"⚙ {name}")
            elif type_ == "portal_notice":
                # Output from a builtin like /session or /compact. It is the
                # answer as far as whoever asked is concerned.
                flush()
                text = payload.get("text")
                if isinstance(text, str) and text.strip():
                    state["all"].append(text.strip())
                    if stream_text:
                        relay(text.strip())
            elif type_ == "extension_ui_request":
                # An extension is blocking on an answer; hand it straight over.
                flush()
                if on_ui:
                    on_ui(payload)
            elif type_ == "extension_ui_cancel":
                if on_ui:
                    on_ui({**payload, "cancelled": True})
            elif type_ == "agent_end":
                # Not the end of the work — a retry, a compaction or a queued
                # message all come after it, and answering here cut them off.
                flush()
            elif type_ == "agent_settled":
                flush()
                settled.set()
            elif type_ == "portal_status":
                if payload.get("status") == "error":
                    failure[0] = RuntimeError(str(payload.get("error") or "run failed"))
                    settled.set()
                # Settled on idle, not only on agent_end: a slash command
                # completes without ever starting an agent turn, and waiting
                # for agent_end hung until the timeout.
                if payload.get("status") == "idle":
                    flush()
                    settled.set()

        # Attached before prompting: a fast reply would otherwise finish before
        # anyone was listening.
        name = f"session:{session_id}"
        self.on(name, on_event)
        try:
            self.prompt(session_id, message)
            if not settled.wait(timeout_ms / 1000):
                raise TimeoutError(f"The agent did not finish within {round(timeout_ms / 1000)}s")
            if failure[0]:
                raise failure[0]
            # Already relayed piece by piece; handing it back would post it twice.
            return "" if on_reply and stream_text else "\n\n".join(state["all"]).strip()
        finally:
            self.off(name, on_event)

    # --- speakers ---

    def set_speaker(self, session_id: str, person: dict) -> None:
        with self._lock:
            self._speaker[session_id] = person

    def speaker_role(self, session_id: str) -> str:
        """The role in force right now.

        Falls back to the conversation's own role, never to "primary". Only
        channel messages identify a speaker; a message sent through the
        portal's prompt endpoint identifies nobody, and defaulting to primary
        there handed a colleague's conversation full privileges.
        """
        with self._lock:
            live = self._speaker.get(session_id)
        if live:
            return live.get("role", "guest")
        row = get_session(session_id)
        return (row or {}).get("role") or "guest"

    def speaker_key(self, session_id: str) -> Optional[str]:
        """Who is speaking, surviving a restart via the session's own record."""
        with self._lock:
            live = self._speaker.get(session_id)
        if live:
            return live.get("key")
        return (get_session(session_id) or {}).get("last_person_key")

    def current_speaker(self, session_id: str) -> Optional[dict]:
        with self._lock:
            return self._speaker.get(session_id)

    def is_busy(self, session_id: str) -> bool:
        with self._lock:
            if session_id in self._asking and self._asking[session_id].locked():
                return True
        return (get_session(session_id) or {}).get("status") == "running"

    def client(self, session_id: str):
        """Access the live client for config reads and writes, starting pi if needed."""
        return self._ensure_client(session_id)

    def respond_ui(self, session_id: str, id_: str, response: dict) -> bool:
        """Answer an extension dialog for a live session."""
        with self._lock:
            live = self._live.get(session_id)
        return live["client"].respond_ui(id_, response) if live else False

    # --- compaction ---

    def compact(self, session_id: str) -> None:
        """Compact on request.

        Marked running for the same reason a prompt is: it takes a model call
        and a minute, and without it the composer showed nothing and there was
        no Stop to press.
        """
        with self._lock:
            if session_id in self._compacting:
                # One at a time; a second request waits for the first.
                pass
            else:
                done = threading.Event()
                self._compacting[session_id] = done
                # Before starting pi, not after: on a cold session that takes
                # seconds, and those are exactly the seconds with no activity
                # line and no Stop.
                self._mark(session_id, "running")

                def run():
                    try:
                        client = self._ensure_client(session_id)
                        # Stop pressed while pi was still starting; honoured here
                        # rather than starting a compaction already declined.
                        with self._lock:
                            cancelled = session_id in self._cancel_pending
                            self._cancel_pending.discard(session_id)
                        if not cancelled:
                            client.compact()
                    finally:
                        with self._lock:
                            self._compacting.pop(session_id, None)
                            self._cancel_pending.discard(session_id)
                        # No agent run, so no agent_settled arrives to clear it.
                        self._mark(session_id, "idle")
                        done.set()

                threading.Thread(target=run, daemon=True).start()
                # The request itself reports once the compaction is over — the
                # UI answers the POST when the thing it describes has happened.
        ev = self._compacting.get(session_id)
        if ev is not None:
            self._settle_compaction(session_id)

    def _settle_compaction(self, session_id: str, timeout_ms: int = COMPACT_SETTLE_MS) -> None:
        """Resolves once any in-flight compaction has finished unwinding.

        Bounded, and it gives up by failing rather than by carrying on: the
        compaction is still running, the session is still detached from agent
        events, and proceeding anyway would start a turn that reaches nobody.
        """
        ev = self._compacting.get(session_id)
        if ev is None:
            return
        if not ev.wait(timeout_ms / 1000):
            raise RuntimeError(
                "The conversation is still being compacted. Nothing else can run until it finishes."
            )

    def refresh_settings(self) -> int:
        """Push a change to pi's settings file into every session already open.

        Failures are swallowed per session: one client that cannot reload is
        not a reason to fail the save.
        """
        with self._lock:
            live = list(self._live.values())
        refreshed = 0
        for s in live:
            try:
                s["client"].refresh_settings()
                refreshed += 1
            except Exception:
                pass
        return refreshed

    def report_prefill(self, session_id: str, prefill: Any) -> None:
        """How far llama.cpp has got through the prompt. Straight out to the browser."""
        self._record(session_id, "portal_prefill", prefill)

    # --- stopping ---

    def abort(self, session_id: str) -> None:
        with self._lock:
            live = self._live.get(session_id)
        if not live or not live["client"].running:
            # Nothing to abort yet — but a compaction waiting on pi to start is
            # still going to run. Remembered so it is cancelled the moment it
            # could begin.
            with self._lock:
                if session_id in self._compacting:
                    self._cancel_pending.add(session_id)
            # Then waited for, like the path below.
            self._settle_compaction(session_id)
            return
        try:
            live["client"].abort()
        except Exception:
            pass
        # Cancelling a compaction does not end it: pi detaches the session from
        # agent events for the whole of compact() and reattaches in its own
        # finally. Publishing idle before that lets the next prompt start
        # against a detached session.
        self._settle_compaction(session_id)
        update_session(session_id, {"status": "idle"})
        self._record(session_id, "portal_status", {"status": "idle", "aborted": True})

    def stop(self, session_id: str) -> None:
        with self._lock:
            live = self._live.pop(session_id, None)
        if not live:
            return
        live["client"].dispose()
        self._stream.clear(session_id)
        try:
            live["executor"].cleanup(session_id)
        except Exception:
            pass

    def shutdown_session(self, session_id: str) -> None:
        """Drop the running process so the next turn rebuilds it — used when a
        session's role changes and its context files must be reloaded."""
        self.stop(session_id)

    def shutdown(self) -> None:
        with self._lock:
            ids = list(self._live.keys())
        for session_id in ids:
            self.stop(session_id)


sessions = SessionManager()
