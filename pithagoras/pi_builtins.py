"""pi's built-in slash commands, portal-side — port of pi/builtins.ts.

pi's built-in slash commands are implemented by whichever mode is running, so
`get_commands()` returns only extensions, templates and skills — the TUI draws
its own. The portal has to supply them too.

`where` decides who handles a command — the server acts on the session, the
client opens a piece of UI. Commands not listed here are TUI concerns (`/quit`,
`/hotkeys`, `/trust`, the auth pair) and are left out rather than offered as a
menu of things that quietly do nothing.
"""

from __future__ import annotations

from typing import Optional

PORTAL_SUPPORTED = {
    "compact": "server",
    "session": "server",
    "export": "server",
    "reload": "server",
    "model": "client",
    "settings": "client",
    "new": "client",
    "name": "client",
}

# The JS portal reads names and descriptions from the SDK's own
# BUILTIN_SLASH_COMMANDS so they track pi's releases; that module is not
# importable from Python, so the descriptions live here.
DESCRIPTIONS = {
    "compact": "Manually compact the session context",
    "session": "Show session info and stats",
    "export": "Export session (HTML default, or specify path: .html/.jsonl)",
    "reload": "Reload extensions, skills, prompts and settings",
    "model": "Select model",
    "settings": "Open settings",
    "new": "Start a new session",
    "name": "Set session display name",
}


def get_builtin_commands() -> list[dict]:
    """pi's builtins, filtered to the ones the portal actually implements."""
    return [
        {
            "name": name,
            "description": DESCRIPTIONS[name],
            "source": "builtin",
            "where": where,
        }
        for name, where in PORTAL_SUPPORTED.items()
    ]


def find_server_builtin(name: str) -> Optional[dict]:
    return next((c for c in get_builtin_commands() if c["name"] == name and c["where"] == "server"), None)


def run_builtin(name: str, args: str, client) -> str:
    """Run a server-side builtin, returning the notice to show in the transcript."""
    if name == "compact":
        client.compact()
        return "Context compacted."

    if name == "reload":
        client.reload()
        return "Reloaded extensions, skills, prompts and settings."

    if name == "session":
        state = client.get_state()
        stats = client.get_stats()
        context = stats.get("contextUsage", {})
        tokens = stats.get("tokens", {})
        return "\n".join([
            f"Model: {state.get('model', {}).get('name')}",
            f"Effort: {state.get('thinkingLevel')}",
            f"Context: {context.get('tokens', 0):,} / {context.get('contextWindow', 0):,} ({context.get('percent', 0):.1f}%)",
            f"Tokens: {tokens.get('input', 0):,} in, {tokens.get('output', 0):,} out",
            f"Cost: ${stats.get('cost', 0):.4f}",
            f"Tool calls: {stats.get('toolCalls', 0)}",
        ])

    if name == "export":
        file = client.export_session(args.strip() or None)
        return f"Exported to {file}"

    raise ValueError(f"'{name}' is not a server-side command")
