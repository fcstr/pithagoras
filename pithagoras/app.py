"""HTTP wiring — port of index.ts.

Routes for auth, global settings, workspaces, sessions, per-session config,
the SSE event stream, and the static web UI. Runs on the small stdlib
framework in httpd.py.

Not yet ported (deferred from the TS index.ts): the channel and routine
supervisors, the skills/mcp/packages/voice/browser/terminal routers, the
browser proxy, and the llama.cpp prefill proxy.
"""

from __future__ import annotations

import json
import os
import threading
import time
from typing import Any, Optional

from . import agent_setup
from .agent import agent_home, resolve_channel_session
from .auth import AUTH_ENABLED, check_password, is_authed, issue_cookie_value, require_auth
from .canvases import (
    canvas_events,
    create_canvas,
    delete_canvas,
    edit_canvas,
    list_canvases,
    persist_canvas,
)
from .db import (
    create_session,
    delete_session,
    event_time,
    events_before,
    events_since,
    get_db,
    get_session,
    get_setting_defaults,
    get_settings,
    get_stored_settings,
    list_agent_sessions,
    list_sessions,
    replay_start,
    set_settings,
    update_session,
)
from .executors import EXECUTOR_KIND
from .httpd import App, Response, error_response, json_response, set_cookie_header
from .live_events import nanoid
from .pi_builtins import get_builtin_commands
from .pi_settings import (
    COMPACTION_DEFAULTS,
    pi_settings_path,
    read_compaction_settings,
    write_compaction_settings,
)
from .session_manager import sessions
from .slug import is_valid_slug, slugify

# WORKSPACE_ROOT is the new name; WORKSPACE_ROOT still works for existing deploys.
WORKSPACE_ROOT = os.path.abspath(os.environ.get("WORKSPACE_ROOT", "/workspaces"))
PORT = int(os.environ.get("PORT", "4100"))
# How much of a long conversation a fresh page load replays.
#
# Small on purpose. Not a correctness limit — a reconnect with a cursor still
# receives everything it missed, and older events are fetched on demand as you
# scroll back. Replaying twenty thousand meant a refresh rendered the entire
# history and then visibly scrolled through it.
REPLAY_EVENTS = 1200
# Persistent place for CLIs, kept on PATH so pi and its tools can reach them.
BIN_DIR = os.path.abspath(os.environ.get("BIN_DIR", "/data/bin"))


def _err(e: Exception) -> Response:
    return error_response(500, str(e) or e.__class__.__name__)


def _sse_headers() -> dict:
    return {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache, no-transform",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }


def build_app(web_dist: Optional[str] = None) -> App:
    app = App()

    # --- auth ---

    # Registered before the auth gate below, like the Express version: a
    # browser that cannot reach these can never log in. Middleware runs in
    # registration order, so the gate is `use()`d after these two routes.
    @app.get("/api/auth/status")
    def auth_status(req):
        return {"authRequired": AUTH_ENABLED, "authed": is_authed(req)}

    @app.post("/api/auth/login")
    def auth_login(req):
        if not AUTH_ENABLED:
            return {"ok": True}
        body = req.json or {}
        if not check_password(body.get("password")):
            return error_response(401, "Wrong password")
        name, value, max_age = issue_cookie_value()
        return json_response({"ok": True}, headers={
            "Set-Cookie": set_cookie_header(name, value, max_age=max_age),
        })

    app.use("/api", require_auth)

    # --- global settings (defaults for every new session) ---

    @app.get("/api/settings")
    def settings_get(req):
        # `stored` and `defaults` are separated so the UI can show an empty
        # field with the inherited value as a placeholder, instead of
        # pre-filling it and turning the next Save into a permanent pin.
        return {
            "settings": get_settings(),
            "stored": get_stored_settings(),
            "defaults": get_setting_defaults(),
            "piSettingsPath": pi_settings_path(),
            # pi's own, not the portal's — kept separate in the response so
            # the UI can say which file a value lives in.
            "compaction": read_compaction_settings(),
            "compactionDefaults": COMPACTION_DEFAULTS,
            "executor": EXECUTOR_KIND,
            "workspaceRoot": WORKSPACE_ROOT,
        }

    @app.put("/api/settings")
    def settings_put(req):
        body = req.json or {}
        patch = {}
        for key, field in (("provider", "provider"), ("model", "model"), ("thinkingLevel", "thinkingLevel")):
            value = body.get(key)
            if isinstance(value, str):
                patch[field] = value.strip()

        # The two live in different stores — the portal's database and pi's own
        # settings file — and there is no way to write both or neither, so a
        # request is not allowed to ask for both.
        keep = body.get("keepRecentTokens")
        wants_defaults = any(isinstance(body.get(k), str) for k in ("provider", "model", "thinkingLevel"))
        if keep is not None and wants_defaults:
            return error_response(
                400,
                "Save the session defaults and the compaction setting separately — they are stored in different files",
            )

        tokens: Optional[int] = None
        if keep is not None:
            try:
                tokens = int(keep)
            except (TypeError, ValueError):
                return error_response(400, "Keep recent must be between 1,000 and 500,000 tokens")
            if tokens < 1000 or tokens > 500_000:
                return error_response(400, "Keep recent must be between 1,000 and 500,000 tokens")

        settings = set_settings(patch)

        # Compaction lives in pi's file rather than the portal's, because pi is
        # what reads it.
        compaction = (
            write_compaction_settings({"keepRecentTokens": tokens})
            if tokens is not None
            else read_compaction_settings()
        )

        # The provider and model defaults apply to sessions started from here
        # on. Compaction is pushed into open sessions as well — the session you
        # are looking at when you change it is the one you meant it for.
        refreshed = sessions.refresh_settings() if tokens is not None else 0
        return {
            "settings": settings,
            "compaction": compaction,
            "refreshed": refreshed,
            "note": (
                f"Compaction applied to {refreshed} open session{'s' if refreshed != 1 else ''}. Model and effort apply to newly started sessions."
                if tokens is not None
                else "Applies to newly started sessions"
            ),
        }

    # --- workspaces ---

    @app.get("/api/workspaces")
    def workspaces_get(req):
        """Directories pi can be pointed at. Anything directly under WORKSPACE_ROOT."""
        if not os.path.isdir(WORKSPACE_ROOT):
            return {"root": WORKSPACE_ROOT, "workspaces": []}
        workspaces = []
        for name in sorted(os.listdir(WORKSPACE_ROOT)):
            if name.startswith("."):
                continue
            full = os.path.join(WORKSPACE_ROOT, name)
            if not os.path.isdir(full):
                continue
            workspaces.append({
                "name": name,
                "path": full,
                "isGit": os.path.exists(os.path.join(full, ".git")),
            })
        return {"root": WORKSPACE_ROOT, "workspaces": workspaces}

    @app.post("/api/workspaces")
    def workspaces_post(req):
        raw = (req.json or {}).get("name")
        if not isinstance(raw, str) or not raw.strip():
            return error_response(400, "name required")
        # "Cool Project" becomes the directory "cool-project", and that same
        # slug becomes the session title — one name drives both.
        name = slugify(raw)
        if not is_valid_slug(name):
            return error_response(400, f'"{raw}" does not produce a usable folder name')

        target = os.path.join(WORKSPACE_ROOT, name)
        if os.path.abspath(target) != target or not target.startswith(WORKSPACE_ROOT + os.sep):
            return error_response(400, "Invalid workspace name")
        if os.path.exists(target):
            return error_response(409, f'Workspace "{name}" already exists')
        try:
            os.makedirs(target, exist_ok=True)
        except OSError as e:
            return _err(e)
        return {"name": name, "path": target, "isGit": False}

    # --- sessions ---

    def to_api(s: dict) -> dict:
        """SQLite stores pinned as 0/1; the API speaks booleans."""
        return {**s, "pinned": bool(s.get("pinned")), "live": sessions.is_running(s["id"])}

    @app.get("/api/sessions")
    def sessions_get(req):
        return {"sessions": [to_api(s) for s in list_sessions()], "executor": EXECUTOR_KIND}

    @app.get("/api/agent/sessions")
    def agent_sessions_get(req):
        """Conversations reached through a channel. Each is a real session —
        same transcript, same replay, same model handling — so the Agent tab
        opens them with the ordinary chat view."""
        channels = get_db().execute("SELECT id, slug, name, kind FROM channels").fetchall()
        by_slug = {c[1]: {"id": c[0], "slug": c[1], "name": c[2], "kind": c[3]} for c in channels}
        out = []
        for s in list_agent_sessions():
            slug = s.get("channel_slug")
            channel = None
            if slug:
                c = by_slug.get(slug)
                channel = {
                    "slug": slug,
                    "name": (c or {}).get("name") or slug,
                    "kind": (c or {}).get("kind"),
                    "present": c is not None,
                }
            out.append({**to_api(s), "channel": channel})
        return {"agentHome": agent_home(), "sessions": out}

    @app.post("/api/agent/sessions")
    def agent_sessions_post(req):
        """A new agent conversation started from the browser.

        Not a channel: the portal's own UI is a better client than any channel
        could be — it streams the transcript, shows tool calls and answers
        extension dialogs — so it talks to the agent directly. "browser" is a
        reserved slug so these group together on the Agent tab.
        """
        title = (req.json or {}).get("title")
        title = title.strip() if isinstance(title, str) and title.strip() else ""
        try:
            session, _created = resolve_channel_session(
                channel_slug="browser",
                key=nanoid(8),
                title=title or f"Chat {time.strftime('%Y-%m-%d %H:%M', time.gmtime())}",
                executor=EXECUTOR_KIND,
            )
            return to_api(session)
        except Exception as e:
            return _err(e)

    # --- the agent's home directory ---

    @app.get("/api/agent/setup")
    def agent_setup_get(req):
        return agent_setup.agent_file_status()

    @app.post("/api/agent/setup")
    def agent_setup_post(req):
        """Run the wizard. Refuses to overwrite an existing MEMORY.md."""
        body = req.json or {}
        if not isinstance(body.get("agentName"), str) or not body["agentName"].strip():
            return error_response(400, "The agent needs a name")
        if not isinstance(body.get("userName"), str) or not body["userName"].strip():
            return error_response(400, "Who is it working for?")
        try:
            agent_setup.run_wizard(body)
            return agent_setup.agent_file_status()
        except Exception as e:
            return _err(e)

    @app.put("/api/agent/files/:name")
    def agent_file_put(req):
        content = (req.json or {}).get("content")
        if not isinstance(content, str):
            return error_response(400, "content required")
        try:
            agent_setup.write_agent_file(req.params["name"], content)
            return agent_setup.agent_file_status()
        except ValueError as e:
            return error_response(400, str(e))

    @app.post("/api/sessions")
    def sessions_post(req):
        body = req.json or {}
        workspace = body.get("workspace")
        if not isinstance(workspace, str) or not workspace:
            return error_response(400, "workspace required")
        # Keep pi inside the mounted workspace area — no escaping to the rest
        # of the FS.
        resolved = os.path.abspath(workspace)
        if resolved != WORKSPACE_ROOT and not resolved.startswith(WORKSPACE_ROOT + os.sep):
            return error_response(400, "workspace must be inside the workspace root")
        if not os.path.exists(resolved):
            return error_response(400, "workspace does not exist")

        id_ = nanoid(12)
        title = body.get("title")
        create_session({
            "id": id_,
            # Default the session name to the workspace folder name.
            "title": (title.strip() if isinstance(title, str) and title.strip() else os.path.basename(resolved)),
            "workspace": resolved,
            "executor": EXECUTOR_KIND,
        })
        return to_api(get_session(id_))

    @app.get("/api/sessions/:id")
    def session_get(req):
        session = get_session(req.params["id"])
        if not session:
            return error_response(404, "Not found")
        return to_api(session)

    @app.patch("/api/sessions/:id")
    def session_patch(req):
        session = get_session(req.params["id"])
        if not session:
            return error_response(404, "Not found")
        body = req.json or {}
        title = body.get("title")
        if isinstance(title, str) and title.strip():
            update_session(session["id"], {"title": title.strip()})
        pinned = body.get("pinned")
        if isinstance(pinned, bool):
            update_session(session["id"], {"pinned": 1 if pinned else 0})
        return to_api(get_session(session["id"]))

    @app.delete("/api/sessions/:id")
    def session_delete(req):
        session = get_session(req.params["id"])
        if not session:
            return error_response(404, "Not found")
        sessions.stop(session["id"])
        delete_session(session["id"])
        return {"ok": True}

    # --- prompting ---

    @app.post("/api/sessions/:id/prompt")
    def session_prompt(req):
        session = get_session(req.params["id"])
        if not session:
            return error_response(404, "Not found")
        message = (req.json or {}).get("message")
        if not isinstance(message, str) or not message.strip():
            return error_response(400, "message required")
        try:
            # Returns as soon as pi accepts the prompt. The run continues
            # server-side regardless of what this browser does next.
            sessions.prompt(session["id"], message, {"voice": (req.json or {}).get("voice") is True})
            return {"ok": True, "status": "running"}
        except Exception as e:
            return _err(e)

    @app.post("/api/sessions/:id/ui-response")
    def session_ui_response(req):
        """The browser answering a dialog an extension is waiting on."""
        session = get_session(req.params["id"])
        if not session:
            return error_response(404, "Not found")
        body = req.json or {}
        id_ = body.get("id")
        if not isinstance(id_, str):
            return error_response(400, "id required")
        delivered = sessions.respond_ui(session["id"], id_, {"value": body.get("value"), "cancelled": bool(body.get("cancelled"))})
        return {"ok": delivered, "note": None if delivered else "Request already resolved or expired"}

    @app.post("/api/sessions/:id/abort")
    def session_abort(req):
        session = get_session(req.params["id"])
        if not session:
            return error_response(404, "Not found")
        sessions.abort(session["id"])
        return {"ok": True}

    # --- per-session config (the web equivalent of the TUI's slash commands) ---

    def _offline_config(session: dict) -> dict:
        """Deliberately does not start pi. Opening a session used to boot a
        model runtime just to draw the pills under the composer — around 600ms
        for whichever session got there first. The stored model and effort are
        what those pills need, and they are right here on the row."""
        defaults = get_settings()
        return {
            "live": False,
            "state": {
                "model": {
                    "id": session.get("model") or defaults.get("model") or "default",
                    "name": session.get("model") or defaults.get("model") or "pi's default",
                    "provider": session.get("provider") or defaults.get("provider"),
                },
                "thinkingLevel": session.get("thinking_level") or defaults.get("thinkingLevel"),
            },
            # Unknowable without the session open, and a made-up zero reads as
            # "empty context" rather than "not measured yet".
            "stats": None,
            "thinking": {"levels": []},
            "models": {"models": []},
        }

    @app.get("/api/sessions/:id/config")
    def session_config_get(req):
        session = get_session(req.params["id"])
        if not session:
            return error_response(404, "Not found")
        if not sessions.is_running(session["id"]):
            return _offline_config(session)
        try:
            client = sessions.client(session["id"])
            return {
                "live": True,
                "state": client.get_state(),
                "thinking": {"levels": client.get_thinking_levels()},
                "models": {"models": client.get_models()},
                "stats": client.get_stats(),
            }
        except Exception as e:
            return _err(e)

    @app.get("/api/sessions/:id/models")
    def session_models_get(req):
        """The model catalogue and effort levels, which do need pi running.

        Split out so the cost lands when the picker is opened rather than on
        every session you glance at.
        """
        session = get_session(req.params["id"])
        if not session:
            return error_response(404, "Not found")
        try:
            client = sessions.client(session["id"])
            return {
                "live": True,
                "state": client.get_state(),
                "thinking": {"levels": client.get_thinking_levels()},
                "models": {"models": client.get_models()},
                "stats": client.get_stats(),
            }
        except Exception as e:
            return _err(e)

    @app.post("/api/sessions/:id/config")
    def session_config_post(req):
        session = get_session(req.params["id"])
        if not session:
            return error_response(404, "Not found")
        body = req.json or {}
        applied: list[str] = []
        try:
            client = sessions.client(session["id"])
            model_id = body.get("modelId")
            if isinstance(model_id, str) and model_id:
                client.set_model(body.get("provider") or get_settings().get("provider"), model_id)
                applied.append("model")
            thinking = body.get("thinkingLevel")
            if isinstance(thinking, str) and thinking:
                client.set_thinking_level(thinking)
                applied.append("thinkingLevel")
            if isinstance(body.get("autoCompaction"), bool):
                client.set_auto_compaction(body["autoCompaction"])
                applied.append("autoCompaction")
            if isinstance(body.get("autoRetry"), bool):
                client.set_auto_retry(body["autoRetry"])
                applied.append("autoRetry")
            state = client.get_state()
            # Recorded so the choice survives a restart. Taken from the
            # resolved state rather than the request: pi coerces the thinking
            # level on a non-reasoning model, and storing what was asked for
            # would reapply the rejected value on every relaunch.
            #
            # Only the fields actually changed are written. Persisting all of
            # them on any change meant that adjusting the effort while pi was
            # sitting on a fallback model wrote that fallback in as the
            # session's chosen model.
            patch: dict[str, Any] = {}
            if "model" in applied:
                patch["provider"] = state.get("model", {}).get("provider")
                patch["model"] = state.get("model", {}).get("id")
            if "thinkingLevel" in applied:
                patch["thinking_level"] = state.get("thinkingLevel")
            if patch:
                update_session(session["id"], patch)
            return {"ok": True, "applied": applied, "state": state}
        except Exception as e:
            return json_response({"error": str(e), "applied": applied}, status=500)

    @app.post("/api/sessions/:id/compact")
    def session_compact(req):
        session = get_session(req.params["id"])
        if not session:
            return error_response(404, "Not found")
        try:
            sessions.compact(session["id"])
            return {"ok": True}
        except Exception as e:
            # The message alone reaches the browser, and "Cannot read
            # properties of undefined" says nothing about where.
            print(f"[portal] compaction failed for {session['id']}: {e}")
            return _err(e)

    @app.get("/api/sessions/:id/commands")
    def session_commands(req):
        """Commands available in this session: built-ins plus anything
        contributed by installed packages."""
        session = get_session(req.params["id"])
        if not session:
            return error_response(404, "Not found")
        try:
            client = sessions.client(session["id"])
            # Builtins first: they are the ones people reach for most.
            return {"commands": [*get_builtin_commands(), *client.get_commands()]}
        except Exception as e:
            return _err(e)

    # --- canvases ---

    def _canvas_session(req) -> Optional[Response]:
        if not get_db().execute("SELECT id FROM sessions WHERE id = ?", (req.params["sessionId"],)).fetchone():
            return error_response(404, "Session not found")
        return None

    @app.get("/api/sessions/:sessionId/canvases")
    def canvases_get(req):
        not_found = _canvas_session(req)
        if not_found:
            return not_found
        return list_canvases(req.params["sessionId"])

    @app.get("/api/sessions/:sessionId/canvases/events")
    def canvases_events(req):
        not_found = _canvas_session(req)
        if not_found:
            return not_found
        session = req.params["sessionId"]
        req.handler.send_response(200)
        for k, v in _sse_headers().items():
            req.handler.send_header(k, v)
        req.handler.end_headers()

        def send(data):
            req.write(f"data: {json.dumps(data)}\n\n".encode())

        canvas_events.on(session, send)
        send({"type": "snapshot", "canvases": list_canvases(session)})
        while not req.client_gone:
            # The SSE keepalive, and the way a dead client is noticed.
            if not req.write(b": keepalive\n\n"):
                break
            time.sleep(15)
        canvas_events.off(session, send)
        return None

    @app.post("/api/sessions/:sessionId/canvases")
    def canvases_post(req):
        not_found = _canvas_session(req)
        if not_found:
            return not_found
        try:
            title = (req.json or {}).get("title")
            if not isinstance(title, str):
                raise ValueError("Title required")
            return create_canvas(req.params["sessionId"], title)
        except (ValueError, KeyError) as e:
            return error_response(400, str(e))

    @app.post("/api/sessions/:sessionId/canvases/:id/persist")
    def canvases_persist(req):
        not_found = _canvas_session(req)
        if not_found:
            return not_found
        try:
            return persist_canvas(req.params["sessionId"], req.params["id"])
        except (ValueError, KeyError) as e:
            return error_response(400, str(e))

    @app.put("/api/sessions/:sessionId/canvases/:id")
    def canvases_put(req):
        not_found = _canvas_session(req)
        if not_found:
            return not_found
        try:
            body = req.json or {}
            revision, title, content = body.get("revision"), body.get("title"), body.get("content")
            if not isinstance(revision, int) or not isinstance(title, str) or not isinstance(content, str):
                raise ValueError("Revision, title and content required")
            return edit_canvas(req.params["sessionId"], req.params["id"], revision, title, content)
        except (ValueError, KeyError) as e:
            return error_response(409, str(e))

    @app.delete("/api/sessions/:sessionId/canvases/:id")
    def canvases_delete(req):
        not_found = _canvas_session(req)
        if not_found:
            return not_found
        try:
            revision = (req.json or {}).get("revision")
            if not isinstance(revision, int):
                raise ValueError("Revision required")
            delete_canvas(req.params["sessionId"], req.params["id"], revision)
            return {"ok": True}
        except (ValueError, KeyError) as e:
            return error_response(409, str(e))

    # --- event stream ---

    @app.get("/api/sessions/:id/events/before")
    def events_before_route(req):
        """What came before a cursor: the transcript scrolling back rather than forward."""
        session = get_session(req.params["id"])
        if not session:
            return error_response(404, "Not found")
        before = int(req.query.get("before") or 0) or 0
        limit = min(int(req.query.get("limit") or 1200), 3000)
        rows = events_before(session["id"], before, limit)
        return {
            "events": [
                {"seq": r["seq"], "type": r["type"], "at": event_time(r.get("created_at")), "payload": json.loads(r["payload"])}
                for r in rows
            ],
            # Whether asking again would return anything, so the UI knows to stop.
            "more": len(rows) == limit,
        }

    @app.get("/api/sessions/:id/events")
    def events_stream(req):
        """Replay-then-tail. The client passes the last seq it saw, so
        reconnecting after minutes or days delivers exactly what was missed
        and then continues live — no gap, no duplicates."""
        session = get_session(req.params["id"])
        if not session:
            return error_response(404, "Not found")
        since = int(req.query.get("since") or 0) or 0

        req.handler.send_response(200)
        for k, v in _sse_headers().items():
            req.handler.send_header(k, v)
        req.handler.end_headers()

        def write(row):
            seq = row["seq"]
            data = json.dumps({
                "seq": seq,
                "type": row["type"],
                # What the activity line counts from, so a refresh mid-run
                # still knows how long the agent has been on this.
                "at": event_time(row.get("created_at")),
                "payload": json.loads(row["payload"]),
            })
            req.write((f"id: {seq}\n" if seq > 0 else "").encode() + f"data: {data}\n\n".encode())

        # Replace stale in-memory deltas before durable replay, then restore
        # the current snapshot.
        req.write(b"event: live-reset\ndata: {}\n\n")

        # A fresh load gets the end of the conversation, not the beginning.
        # Replaying from zero and stopping at the batch limit is how a long
        # session came back from a refresh showing its first few thousand
        # events and nothing since.
        cursor = replay_start(session["id"], REPLAY_EVENTS) if since == 0 else since

        # Paged to the end rather than one batch: a reconnect after a long run
        # has more to catch up on than a single query returns.
        last_sent = cursor
        while True:
            batch = events_since(session["id"], last_sent)
            if not batch:
                break
            for row in batch:
                write(row)
                last_sent = row["seq"]
            if len(batch) < 5000:
                break
        for row in sessions.live_snapshot(session["id"]):
            write(row)
        req.write(f"event: caught-up\ndata: {json.dumps({'seq': last_sent})}\n\n".encode())

        name = f"session:{session['id']}"
        state = {"last": last_sent}

        def on_event(row):
            # Live-only events carry a negative seq: deliver them, but never let
            # one move the replay cursor, or a reconnect would skip stored history.
            if row["seq"] < 0:
                write(row)
                return
            # Guard against double-sending anything the replay already covered.
            if row["seq"] <= state["last"]:
                return
            state["last"] = row["seq"]
            write(row)

        sessions.on(name, on_event)
        try:
            while not req.client_gone:
                # The SSE heartbeat, and the way a dead client is noticed.
                if not req.write(b": ping\n\n"):
                    break
                time.sleep(25)
        finally:
            sessions.off(name, on_event)
        return None

    # --- static web UI ---

    if web_dist and os.path.isdir(web_dist):
        import mimetypes
        index_html = os.path.join(web_dist, "index.html")

        def static_or_spa(req):
            # Anything that is not /api gets the SPA; real files are served as-is.
            rel = req.path.lstrip("/")
            candidate = os.path.abspath(os.path.join(web_dist, rel))
            if rel and candidate.startswith(os.path.abspath(web_dist) + os.sep) and os.path.isfile(candidate):
                with open(candidate, "rb") as f:
                    body = f.read()
                ctype = mimetypes.guess_type(candidate)[0] or "application/octet-stream"
                return Response(200, body, content_type=ctype)
            try:
                with open(index_html, "rb") as f:
                    return Response(200, f.read(), content_type="text/html; charset=utf-8")
            except OSError:
                raise RuntimeError("web UI not built")

        app.fallback = static_or_spa

    return app


def web_dist_path() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(here, "..", "web", "dist"))


def run() -> None:
    # On PATH via the image, but a volume that predates it has no such
    # directory — docker only seeds a volume that is empty, so an existing
    # deploy would carry a PATH entry pointing at nothing.
    os.makedirs(BIN_DIR, exist_ok=True)

    from .httpd import make_server

    app = build_app(web_dist_path())
    server = make_server(app, PORT, "0.0.0.0")

    # Canvases mid-write when the portal stopped are interrupted, not active.
    get_db().execute(
        "UPDATE canvases SET active_call = NULL, status = 'interrupted', "
        "agent_read_revision = revision WHERE active_call IS NOT NULL"
    )

    print(f"pithagoras listening on :{PORT}")
    print(f"  local bin: {BIN_DIR}")
    print(f"  executor: {EXECUTOR_KIND}")
    print(f"  workspaces: {WORKSPACE_ROOT}")
    print(f"  auth:     {'password' if AUTH_ENABLED else 'DISABLED'}")

    def shutdown(signum, _frame):
        import signal as _signal
        print(f"{_signal.Signals(signum).name} received — stopping running sessions")
        sessions.shutdown()
        # Close in a thread: serve_forever is what is running on this one.
        threading.Thread(target=server.shutdown, daemon=True).start()

    import signal
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    try:
        server.serve_forever()
    finally:
        server.server_close()
