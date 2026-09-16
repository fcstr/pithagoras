"""Channel conversation resolution — port of agent.ts."""

from __future__ import annotations

import os
from typing import Optional

from .db import create_session, find_channel_session
from .live_events import nanoid


def agent_home() -> str:
    """The agent's fixed working directory, separate from the per-task workspaces.

    Kept out of the workspace root deliberately: it is not a project you would
    start a session against, and listing it as one would be misleading.
    """
    directory = os.path.abspath(os.environ.get("AGENT_HOME", "/data/agent-home"))
    os.makedirs(directory, exist_ok=True)
    return directory


# Keys come from outside, so they are bounded before touching the database.
MAX_KEY = 200


def scope_key(channel_slug: str, key: str) -> str:
    """The key a package supplies is namespaced by its channel's slug.

    The slug and not the channel's id: ids are regenerated when a channel is
    deleted and recreated, which silently orphaned every conversation it had.
    A slug is stable and yours to choose, so re-adding under the same one picks
    the conversations back up.
    """
    return f"{channel_slug}:{key}"


def unscope_key(channel_slug: str, stored: str) -> str:
    """The channel's own key, with the prefix taken back off."""
    prefix = f"{channel_slug}:"
    return stored[len(prefix):] if stored.startswith(prefix) else stored


def resolve_channel_session(
    *, channel_slug: str, key: str, title: Optional[str] = None, executor: str
) -> tuple[dict, bool]:
    """Find or create the session for one conversation on one channel.

    Returns (session, created).
    """
    key = str(key or "").strip()[:MAX_KEY]
    if not key:
        raise ValueError("A channel must supply a session key for each conversation")

    scoped = scope_key(channel_slug, key)

    existing = find_channel_session(scoped)
    if existing:
        return existing, False

    create_session({
        "id": nanoid(12),
        "title": (title or "").strip()[:120] or key,
        "workspace": agent_home(),
        "executor": executor,
        "kind": "agent",
        "channel_slug": channel_slug,
        "channel_key": scoped,
    })

    # Re-read rather than construct: the row carries defaults this does not set.
    session = find_channel_session(scoped)
    if not session:
        raise RuntimeError("Failed to create the session for this conversation")
    return session, True
