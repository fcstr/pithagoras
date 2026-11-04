"""SQLite persistence — port of server/src/db.ts.

One module-level connection guarded by a lock. WAL mode lets the SSE replay
reads run alongside event appends. sqlite3 is stdlib; `check_same_thread=False`
because requests and the pi reader threads share it.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Any, Optional

DATA_DIR = os.environ.get("DATA_DIR", "./data")

_db: Optional[sqlite3.Connection] = None
_db_lock = threading.RLock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
  id TEXT PRIMARY KEY,
  title TEXT NOT NULL,
  workspace TEXT NOT NULL,
  executor TEXT NOT NULL DEFAULT 'host',
  status TEXT NOT NULL DEFAULT 'idle',
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  last_error TEXT,
  provider TEXT,
  model TEXT,
  thinking_level TEXT,
  pinned INTEGER NOT NULL DEFAULT 0,
  pi_session_file TEXT,
  kind TEXT NOT NULL DEFAULT 'task',
  channel_slug TEXT,
  channel_key TEXT,
  routine_slug TEXT
);
-- The index on (channel_id, channel_key) is created in migrate(), not here.
-- CREATE TABLE IF NOT EXISTS is a no-op against an existing table, so on an
-- upgrade these columns do not exist yet at this point and indexing them
-- fails — which took the server down until the migration had run.

-- Every event pi emits is appended here. This is what makes the portal
-- fire-and-forget: a browser that reconnects days later replays from its
-- last seen seq instead of having missed the run entirely.
CREATE TABLE IF NOT EXISTS canvases (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  title TEXT NOT NULL,
  content TEXT NOT NULL DEFAULT '',
  revision INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'saved',
  active_call TEXT,
  agent_read_revision INTEGER,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_canvases_session ON canvases(session_id);

CREATE TABLE IF NOT EXISTS events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  type TEXT NOT NULL,
  payload TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id, seq);

-- Two-way links into the agent session. Each row is one connection
-- (a Telegram bot, a Slack app, an inbound webhook); messages arriving on
-- any of them go to the same agent, and its replies go back the same way.
CREATE TABLE IF NOT EXISTS channels (
  id TEXT PRIMARY KEY,
  slug TEXT NOT NULL DEFAULT '',
  kind TEXT NOT NULL,
  name TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  config TEXT NOT NULL DEFAULT '{}',
  instructions TEXT NOT NULL DEFAULT '',
  relay_progress INTEGER NOT NULL DEFAULT 1,
  relay_tools INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Scheduled work. Each routine owns one session, so a run can see what the
-- last one did rather than starting blind every time.
CREATE TABLE IF NOT EXISTS routines (
  id TEXT PRIMARY KEY,
  slug TEXT NOT NULL,
  name TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  schedule TEXT NOT NULL DEFAULT '',
  run_at TEXT,
  instructions TEXT NOT NULL DEFAULT '',
  fresh_session INTEGER NOT NULL DEFAULT 0,
  guard INTEGER NOT NULL DEFAULT 1,
  report_channel TEXT,
  report_target TEXT,
  last_report_at TEXT,
  last_run TEXT,
  last_status TEXT,
  last_output TEXT,
  last_ms INTEGER,
  next_run TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Who the agent talks to. Identified by the platform's own stable id,
-- scoped by channel, because a display name is chosen by whoever types it.
CREATE TABLE IF NOT EXISTS people (
  key TEXT PRIMARY KEY,
  name TEXT NOT NULL DEFAULT '',
  role TEXT NOT NULL DEFAULT 'unknown',
  notes TEXT NOT NULL DEFAULT '',
  first_seen TEXT NOT NULL DEFAULT (datetime('now')),
  last_seen TEXT,
  announced_at TEXT
);

-- Questions a colleague's session could not answer, waiting on the primary
-- user. The id is short because a human types it back in a chat.
CREATE TABLE IF NOT EXISTS questions (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  person_key TEXT NOT NULL,
  person_name TEXT NOT NULL DEFAULT '',
  channel_slug TEXT NOT NULL,
  channel_key TEXT NOT NULL,
  question TEXT NOT NULL,
  asked_at TEXT NOT NULL DEFAULT (datetime('now')),
  answered_at TEXT,
  answer TEXT,
  action_tool TEXT,
  action TEXT
);

-- A permission granted once, for one exact action, in one conversation.
-- Not a role change: it expires, it is used up, and it authorises the thing
-- that was shown to the person who approved it.
CREATE TABLE IF NOT EXISTS grants (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL,
  tool TEXT NOT NULL,
  subject TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  expires_at TEXT NOT NULL,
  used_at TEXT
);

-- Things the portal said into a conversation while nobody was talking to
-- it: a routine's report, an answer relayed back. Held until that
-- conversation next runs, then folded into its context.
CREATE TABLE IF NOT EXISTS notes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL,
  text TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  consumed_at TEXT,
  pending_delivery INTEGER NOT NULL DEFAULT 0
);

-- Exceptions to what a non-primary role may run.
CREATE TABLE IF NOT EXISTS tool_rules (
  id TEXT PRIMARY KEY,
  role TEXT NOT NULL,
  tool TEXT NOT NULL,
  pattern TEXT NOT NULL,
  person_key TEXT,
  note TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- What the guard did, and why.
CREATE TABLE IF NOT EXISTS audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  at TEXT NOT NULL DEFAULT (datetime('now')),
  kind TEXT NOT NULL,
  tool TEXT NOT NULL DEFAULT '',
  subject TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL DEFAULT '',
  person_key TEXT,
  session_id TEXT
);

CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""


def _rows(cur: sqlite3.Cursor) -> list[dict]:
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _row(cur: sqlite3.Cursor) -> Optional[dict]:
    r = cur.fetchone()
    if r is None:
        return None
    cols = [c[0] for c in cur.description]
    return dict(zip(cols, r))


def get_db() -> sqlite3.Connection:
    global _db
    with _db_lock:
        if _db is not None:
            return _db
        os.makedirs(DATA_DIR, exist_ok=True)
        conn = sqlite3.connect(
            os.path.join(DATA_DIR, "portal.db"),
            check_same_thread=False,
            isolation_level=None,  # autocommit, like better-sqlite3
        )
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript(SCHEMA)
        _migrate(conn)
        _db = conn
        return conn


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _migrate(d: sqlite3.Connection) -> None:
    """Migrations run in place rather than recreating the table, so existing
    sessions and their event history survive an upgrade."""
    names = _table_columns(d, "sessions")
    if "project" in names and "workspace" not in names:
        d.execute("ALTER TABLE sessions RENAME COLUMN project TO workspace")
    # Model and effort used to live only in the running pi process, so a restart
    # silently reverted every session to the portal defaults.
    for col in ("provider", "model", "thinking_level"):
        if col not in names:
            d.execute(f"ALTER TABLE sessions ADD COLUMN {col} TEXT")
    if "pinned" not in names:
        d.execute("ALTER TABLE sessions ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0")
    if "pi_session_file" not in names:
        d.execute("ALTER TABLE sessions ADD COLUMN pi_session_file TEXT")
    # The lowest role this session has ever served. Ratchets down and never up:
    # once a guest has spoken in a conversation, the private context files stay
    # out of it even if the next message is from the primary user.
    if "browser" not in names:
        d.execute("ALTER TABLE sessions ADD COLUMN browser INTEGER NOT NULL DEFAULT 0")
    if "last_person_key" not in names:
        d.execute("ALTER TABLE sessions ADD COLUMN last_person_key TEXT")
    if "role" not in names:
        d.execute("ALTER TABLE sessions ADD COLUMN role TEXT NOT NULL DEFAULT 'primary'")
    if "kind" not in names:
        d.execute("ALTER TABLE sessions ADD COLUMN kind TEXT NOT NULL DEFAULT 'task'")
    # channel_id was the original link and was a mistake — see channel_slug.
    # There is no data worth migrating, so the old column and its sessions go.
    if "channel_id" in names:
        d.execute("DROP INDEX IF EXISTS idx_sessions_channel")
        d.execute("DELETE FROM sessions WHERE kind = 'agent'")
        d.execute("ALTER TABLE sessions DROP COLUMN channel_id")
    if "channel_slug" not in names:
        d.execute("ALTER TABLE sessions ADD COLUMN channel_slug TEXT")
    if "channel_key" not in names:
        d.execute("ALTER TABLE sessions ADD COLUMN channel_key TEXT")
    if "routine_slug" not in names:
        d.execute("ALTER TABLE sessions ADD COLUMN routine_slug TEXT")
    # The key already carries its channel's slug, so it is unique on its own.
    d.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_channel_key "
        "ON sessions(channel_key) WHERE channel_key IS NOT NULL"
    )

    channel_cols = _table_columns(d, "channels")
    if channel_cols and "instructions" not in channel_cols:
        d.execute("ALTER TABLE channels ADD COLUMN instructions TEXT NOT NULL DEFAULT ''")
    if channel_cols and "slug" not in channel_cols:
        d.execute("ALTER TABLE channels ADD COLUMN slug TEXT NOT NULL DEFAULT ''")
        # Nothing sensible to backfill from, and no data to lose.
        d.execute("DELETE FROM channels WHERE slug = ''")
    if channel_cols and "relay_progress" not in channel_cols:
        d.execute("ALTER TABLE channels ADD COLUMN relay_progress INTEGER NOT NULL DEFAULT 1")
    if channel_cols and "relay_tools" not in channel_cols:
        d.execute("ALTER TABLE channels ADD COLUMN relay_tools INTEGER NOT NULL DEFAULT 1")
    d.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_channels_slug ON channels(slug)")

    routine_cols = _table_columns(d, "routines")
    if routine_cols and "run_at" not in routine_cols:
        d.execute("ALTER TABLE routines ADD COLUMN run_at TEXT")
    for col in ("report_channel", "report_target", "last_report_at"):
        if routine_cols and col not in routine_cols:
            d.execute(f"ALTER TABLE routines ADD COLUMN {col} TEXT")
    if routine_cols and "guard" not in routine_cols:
        d.execute("ALTER TABLE routines ADD COLUMN guard INTEGER NOT NULL DEFAULT 1")
    if routine_cols and "browser" not in routine_cols:
        d.execute("ALTER TABLE routines ADD COLUMN browser INTEGER NOT NULL DEFAULT 0")
    d.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_routines_slug ON routines(slug)")
    d.execute("CREATE INDEX IF NOT EXISTS idx_notes_pending ON notes(session_id, consumed_at)")
    d.execute("CREATE INDEX IF NOT EXISTS idx_grants_open ON grants(session_id, tool, used_at)")
    d.execute("CREATE INDEX IF NOT EXISTS idx_audit_at ON audit(at DESC)")

    rule_cols = _table_columns(d, "tool_rules")
    if rule_cols and "person_key" not in rule_cols:
        d.execute("ALTER TABLE tool_rules ADD COLUMN person_key TEXT")
    question_cols = _table_columns(d, "questions")
    for col in ("action_tool", "action"):
        if question_cols and col not in question_cols:
            d.execute(f"ALTER TABLE questions ADD COLUMN {col} TEXT")
    note_cols = _table_columns(d, "notes")
    if note_cols and "pending_delivery" not in note_cols:
        d.execute("ALTER TABLE notes ADD COLUMN pending_delivery INTEGER NOT NULL DEFAULT 0")


def create_session(row: dict) -> None:
    with _db_lock:
        get_db().execute(
            "INSERT INTO sessions (id, title, workspace, executor, kind, channel_slug, channel_key, routine_slug)"
            " VALUES (:id, :title, :workspace, :executor, :kind, :channel_slug, :channel_key, :routine_slug)",
            {
                "kind": "task",
                "channel_slug": None,
                "channel_key": None,
                "routine_slug": None,
                **row,
            },
        )


def list_sessions() -> list[dict]:
    """The sessions you create yourself. Agent sessions have their own tab."""
    # Pinned first, then most recently touched — the order the sidebar shows.
    with _db_lock:
        return _rows(get_db().execute(
            "SELECT * FROM sessions WHERE kind = 'task' ORDER BY pinned DESC, updated_at DESC"
        ))


def list_agent_sessions() -> list[dict]:
    """Conversations reached through a channel, newest first."""
    with _db_lock:
        return _rows(get_db().execute(
            "SELECT * FROM sessions WHERE kind = 'agent' ORDER BY updated_at DESC"
        ))


def find_channel_session(key: str) -> Optional[dict]:
    with _db_lock:
        return _row(get_db().execute("SELECT * FROM sessions WHERE channel_key = ?", (key,)))


def find_routine_session(slug: str) -> Optional[dict]:
    """The session a routine owns, if it has run before."""
    with _db_lock:
        return _row(get_db().execute(
            "SELECT * FROM sessions WHERE routine_slug = ? AND kind = 'routine' ORDER BY created_at ASC",
            (slug,),
        ))


def list_routine_sessions(slug: Optional[str] = None) -> list[dict]:
    sql = (
        "SELECT * FROM sessions WHERE kind = 'routine' AND routine_slug = ? ORDER BY updated_at DESC"
        if slug
        else "SELECT * FROM sessions WHERE kind = 'routine' ORDER BY updated_at DESC"
    )
    with _db_lock:
        return _rows(get_db().execute(sql, (slug,) if slug else ()))


def count_channel_sessions(slug: str) -> int:
    """How many conversations a channel would strand if it were removed."""
    with _db_lock:
        r = get_db().execute("SELECT count(*) AS n FROM sessions WHERE channel_slug = ?", (slug,)).fetchone()
    return r[0] if r else 0


def get_session(session_id: str) -> Optional[dict]:
    with _db_lock:
        return _row(get_db().execute("SELECT * FROM sessions WHERE id = ?", (session_id,)))


_UPDATABLE = (
    "title", "status", "last_error", "provider", "model",
    "thinking_level", "pinned", "pi_session_file",
)


def update_session(session_id: str, fields: dict) -> None:
    sets, values = [], []
    for k, v in fields.items():
        if k not in _UPDATABLE:
            continue
        sets.append(f"{k} = ?")
        values.append(v)
    if not sets:
        return
    sets.append("updated_at = datetime('now')")
    with _db_lock:
        get_db().execute(
            f"UPDATE sessions SET {', '.join(sets)} WHERE id = ?", (*values, session_id)
        )


def delete_session(session_id: str) -> None:
    with _db_lock:
        d = get_db()
        d.execute("DELETE FROM canvases WHERE session_id = ?", (session_id,))
        d.execute("DELETE FROM events WHERE session_id = ?", (session_id,))
        d.execute("DELETE FROM sessions WHERE id = ?", (session_id,))


_ISO_ZONE = re.compile(r"[Zz]|[+-]\d\d:?\d\d$")


def event_time(created_at: Optional[str]) -> Optional[int]:
    """When an event happened, in epoch milliseconds.

    SQLite writes `datetime('now')` as UTC with no zone marker, which JS parses
    as local time — an hour or ten out, depending on where the portal runs. The
    live path writes a real ISO string, so both shapes turn up in the same table.
    """
    if not created_at:
        return None
    iso = created_at if _ISO_ZONE.search(created_at) else created_at.replace(" ", "T") + "Z"
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    except ValueError:
        return None


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def append_event(session_id: str, type_: str, payload: Any) -> dict:
    encoded = json.dumps(payload)
    with _db_lock:
        cur = get_db().execute(
            "INSERT INTO events (session_id, type, payload) VALUES (?, ?, ?)",
            (session_id, type_, encoded),
        )
        seq = cur.lastrowid
    return {
        "seq": seq,
        "session_id": session_id,
        "type": type_,
        "payload": encoded,
        "created_at": utcnow_iso(),
    }


def replay_start(session_id: str, keep: int) -> int:
    """Where to start replaying so a session gets its own last `keep` events.

    Counted within the session, not across the table. seq is a single sequence
    shared by every session, so "the last 20,000 seq" is "whatever this
    conversation happened to do while the portal was busy with others" — on a
    busy box that can be almost nothing.
    """
    with _db_lock:
        row = get_db().execute(
            "SELECT seq FROM events WHERE session_id = ? ORDER BY seq DESC LIMIT 1 OFFSET ?",
            (session_id, keep),
        ).fetchone()
    return row[0] if row else 0


def events_before(session_id: str, before: int, limit: int = 1500) -> list[dict]:
    """The page before a cursor, oldest first — what a transcript scrolls back into."""
    with _db_lock:
        return _rows(get_db().execute(
            "SELECT * FROM (SELECT * FROM events WHERE session_id = ? AND seq < ? "
            "ORDER BY seq DESC LIMIT ?) ORDER BY seq ASC",
            (session_id, before, limit),
        ))


def events_since(session_id: str, since: int = 0, limit: int = 5000) -> list[dict]:
    with _db_lock:
        return _rows(get_db().execute(
            "SELECT * FROM events WHERE session_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?",
            (session_id, since, limit),
        ))


def mark_orphaned_sessions_interrupted() -> int:
    """A session marked `running` at boot cannot actually be running — the process
    that owned it died with the previous server. Mark them interrupted so the UI
    can offer a resume instead of showing a spinner forever."""
    with _db_lock:
        cur = get_db().execute(
            "UPDATE sessions SET status = 'interrupted', updated_at = datetime('now') WHERE status = 'running'"
        )
        return cur.rowcount


# --- global settings ---


def _setting_defaults() -> dict:
    """Env, else pi's own settings.json, else "openrouter" as the last resort."""
    from . import pi_settings
    return {
        "provider": os.environ.get("PI_PROVIDER") or pi_settings.pi_setting("defaultProvider") or "openrouter",
        "model": os.environ.get("PI_MODEL") or pi_settings.pi_setting("defaultModel") or "",
        "thinkingLevel": os.environ.get("PI_THINKING_LEVEL")
        or pi_settings.pi_setting("defaultThinkingLevel")
        or "medium",
    }


get_setting_defaults = _setting_defaults


def get_stored_settings() -> dict:
    """Only what the portal was explicitly told; absent keys fall through."""
    with _db_lock:
        rows = get_db().execute("SELECT key, value FROM settings").fetchall()
    return {k: v for k, v in rows if v}


def get_settings() -> dict:
    """What pi is actually launched with: stored, else env, else pi's file."""
    stored = get_stored_settings()
    defaults = _setting_defaults()
    return {
        "provider": stored.get("provider") or defaults["provider"],
        "model": stored.get("model") or defaults["model"],
        "thinkingLevel": stored.get("thinkingLevel") or defaults["thinkingLevel"],
    }


def set_settings(patch: dict) -> dict:
    """An empty value clears the override rather than storing "", so a field can be
    handed back to pi's own defaults instead of being pinned forever."""
    with _db_lock:
        d = get_db()
        for k, v in patch.items():
            if not isinstance(v, str):
                continue
            if v.strip():
                d.execute(
                    "INSERT INTO settings (key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (k, v.strip()),
                )
            else:
                d.execute("DELETE FROM settings WHERE key = ?", (k,))
    return get_settings()


def get_default_report_to() -> Optional[dict]:
    """Where reports go when a routine does not name a destination of its own."""
    stored = get_stored_settings()
    channel, target = stored.get("report_channel"), stored.get("report_target")
    return {"channel": channel, "target": target} if channel and target else None


def set_default_report_to(to: Optional[dict]) -> None:
    with _db_lock:
        d = get_db()
        if not to:
            d.execute("DELETE FROM settings WHERE key = ?", ("report_channel",))
            d.execute("DELETE FROM settings WHERE key = ?", ("report_target",))
            return
        d.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            ("report_channel", to["channel"]),
        )
        d.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            ("report_target", to["target"]),
        )


def add_note(session_id: str, text: str, pending_delivery: bool = False) -> None:
    """Something the portal said into a conversation, waiting to join its context."""
    with _db_lock:
        get_db().execute(
            "INSERT INTO notes (session_id, text, pending_delivery) VALUES (?, ?, ?)",
            (session_id, text, 1 if pending_delivery else 0),
        )


def take_deliveries(session_id: str) -> list[str]:
    """Messages the person has not seen, because their channel cannot be spoken to.

    Reading them hands over responsibility for delivering them, so they are only
    taken at the point they are about to go out with a reply.
    """
    with _db_lock:
        d = get_db()
        rows = d.execute(
            "SELECT id, text FROM notes WHERE session_id = ? AND pending_delivery = 1 ORDER BY id ASC",
            (session_id,),
        ).fetchall()
        for rid, _ in rows:
            d.execute("UPDATE notes SET pending_delivery = 0 WHERE id = ?", (rid,))
    return [t for _, t in rows]


def take_notes(session_id: str) -> list[str]:
    """Take the pending notes for a conversation. Reading them consumes them."""
    with _db_lock:
        d = get_db()
        rows = d.execute(
            "SELECT id, text FROM notes WHERE session_id = ? AND consumed_at IS NULL ORDER BY id ASC",
            (session_id,),
        ).fetchall()
        for rid, _ in rows:
            d.execute("UPDATE notes SET consumed_at = datetime('now') WHERE id = ?", (rid,))
    return [t for _, t in rows]


def list_tool_rules() -> list[dict]:
    with _db_lock:
        return _rows(get_db().execute("SELECT * FROM tool_rules ORDER BY tool, pattern"))


def add_tool_rule(rule: dict) -> None:
    with _db_lock:
        get_db().execute(
            "INSERT INTO tool_rules (id, role, tool, pattern, note, person_key) VALUES (?, ?, ?, ?, ?, ?)",
            (rule["id"], rule["role"], rule["tool"], rule["pattern"],
             rule.get("note", ""), rule.get("person_key")),
        )


def delete_tool_rule(rule_id: str) -> None:
    with _db_lock:
        get_db().execute("DELETE FROM tool_rules WHERE id = ?", (rule_id,))


# How long an approval stays good. Long enough to act on, short enough to forget.
GRANT_MINUTES = 15


def add_grant(grant_id: str, session_id: str, tool: str, subject: str) -> None:
    expires = datetime.fromtimestamp(
        time.time() + GRANT_MINUTES * 60, timezone.utc
    ).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    with _db_lock:
        get_db().execute(
            "INSERT INTO grants (id, session_id, tool, subject, expires_at) VALUES (?, ?, ?, ?, ?)",
            (grant_id, session_id, tool, subject, expires),
        )


def use_grant(session_id: str, tool: str, subject: str) -> bool:
    """Spend a matching approval, if one is open.

    Matched on the exact subject that was shown to whoever approved it: they said
    yes to a command they read, so a different command is a different question.
    Marked used in the same breath, because an approval is for one act.
    """
    now = utcnow_iso()
    with _db_lock:
        d = get_db()
        row = d.execute(
            "SELECT id FROM grants WHERE session_id = ? AND tool = ? AND subject = ? "
            "AND used_at IS NULL AND expires_at > ? ORDER BY created_at ASC LIMIT 1",
            (session_id, tool, subject, now),
        ).fetchone()
        if not row:
            return False
        d.execute("UPDATE grants SET used_at = ? WHERE id = ?", (now, row[0]))
    return True


# Keeps the log from growing without bound; old entries are not evidence.
AUDIT_KEEP = 2000


def record_audit(entry: dict) -> None:
    with _db_lock:
        d = get_db()
        d.execute(
            "INSERT INTO audit (kind, tool, subject, reason, person_key, session_id) VALUES (?, ?, ?, ?, ?, ?)",
            (
                entry["kind"],
                entry.get("tool", ""),
                (entry.get("subject") or "")[:2000],
                entry.get("reason", ""),
                entry.get("personKey"),
                entry.get("sessionId"),
            ),
        )
        d.execute("DELETE FROM audit WHERE id <= (SELECT MAX(id) FROM audit) - ?", (AUDIT_KEEP,))


def list_audit(limit: int = 200) -> list[dict]:
    with _db_lock:
        return _rows(get_db().execute("SELECT * FROM audit ORDER BY id DESC LIMIT ?", (limit,)))


def routine_guards(slug: Optional[str]) -> bool:
    """Does this routine's runs get the guard's blocking rules? Unknown means yes."""
    if not slug:
        return True
    with _db_lock:
        row = get_db().execute("SELECT guard FROM routines WHERE slug = ?", (slug,)).fetchone()
    return row[0] == 1 if row else True


def browser_allowed(session: dict) -> bool:
    """Does this session get the browser? Routines answer for their own runs."""
    if session.get("kind") == "routine" and session.get("routine_slug"):
        with _db_lock:
            row = get_db().execute(
                "SELECT browser FROM routines WHERE slug = ?", (session["routine_slug"],)
            ).fetchone()
        return row[0] == 1 if row else False
    return session.get("browser") == 1


def browser_allowlist() -> list[str]:
    """Domains the browser may be pointed at, as globs. Empty means no restriction —
    the on/off switch is the gate, and a list nobody filled in should not quietly
    block everything.
    """
    raw = get_stored_settings().get("browser_allowlist", "")
    return [d.strip() for d in re.split(r"[\n,]", raw) if d.strip()]


def set_browser_allowlist(domains: str) -> None:
    with _db_lock:
        get_db().execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            ("browser_allowlist", domains.strip()),
        )
