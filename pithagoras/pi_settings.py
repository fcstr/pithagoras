"""pi's own settings file — port of pi-settings.ts.

The portal edits it (Advanced), scans it for extension keys, and reads its
`default*` entries as the fallback for new sessions, so the path lives in one
place rather than being rebuilt at each call site.
"""

from __future__ import annotations

import json
import os
import threading
from typing import Any, Callable, Optional


def pi_agent_dir() -> str:
    return os.environ.get("PI_CODING_AGENT_DIR", "").strip() or os.path.join(
        os.environ.get("HOME", "/data/home"), ".pi", "agent"
    )


def pi_settings_path() -> str:
    return os.path.join(pi_agent_dir(), "settings.json")


def read_pi_settings() -> dict:
    try:
        with open(pi_settings_path(), encoding="utf-8") as f:
            parsed = json.load(f)
        return parsed if isinstance(parsed, dict) else {}
    except (OSError, ValueError):
        # Missing or malformed: callers fall back to their own defaults.
        return {}


def pi_setting(key: str) -> Optional[str]:
    """A string setting from pi's file, or None if absent or the wrong type."""
    value = read_pi_settings().get(key)
    return value if isinstance(value, str) and value else None


# pi's own defaults, repeated here so the UI can show what it is inheriting.
COMPACTION_DEFAULTS = {"enabled": True, "keepRecentTokens": 20_000}


def read_compaction_settings() -> dict:
    """How compaction is tuned, from pi's own file rather than the portal's.

    `keepRecentTokens` is the floor a compaction cannot go below: the most
    recent stretch of conversation is kept verbatim and only what is older gets
    summarised.
    """
    stored = read_pi_settings().get("compaction")
    c = stored if isinstance(stored, dict) else {}
    keep = c.get("keepRecentTokens")
    return {
        "enabled": c.get("enabled") is not False,
        "keepRecentTokens": keep
        if isinstance(keep, (int, float)) and keep > 0
        else COMPACTION_DEFAULTS["keepRecentTokens"],
    }


# Serialises the portal's own writes, so a slider released at the same moment as
# a Save cannot interleave and drop one of the two changes.
_write_lock = threading.Lock()


def update_pi_settings(mutate: Callable[[dict], None]) -> dict:
    """Change pi's settings file without losing what else is in it.

    Read-modify-write on a file pi also owns, so two precautions. The write is a
    temp file and a rename, which is atomic on the same filesystem — a reader
    arriving mid-write sees the old file whole rather than half of the new one.
    """
    with _write_lock:
        all_settings = read_pi_settings()
        mutate(all_settings)
        file = pi_settings_path()
        os.makedirs(os.path.dirname(file), exist_ok=True)
        temp = f"{file}.{os.getpid()}.tmp"
        with open(temp, "w", encoding="utf-8") as f:
            f.write(json.dumps(all_settings, indent=2) + "\n")
        os.replace(temp, file)
        return all_settings


def write_compaction_settings(patch: dict) -> dict:
    """Merged, never replaced: the portal has no business dropping a key it does not know."""

    def mutate(all_settings: dict) -> None:
        current = all_settings.get("compaction")
        if not isinstance(current, dict):
            current = {}
        all_settings["compaction"] = {**current, **patch}

    update_pi_settings(mutate)
    return read_compaction_settings()
