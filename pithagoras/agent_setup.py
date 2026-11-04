"""The agent's home directory setup — port of agent-setup.ts.

These three files are handed to pi as context files when a session starts.
Nothing is generated from them: the SDK takes an explicit list, which puts
MEMORY.md genuinely in context rather than relying on the agent to go and
read it.
"""

from __future__ import annotations

import os
from typing import Optional

from .agent import agent_home

AGENT_FILES = ("SOUL.md", "PrimaryUser.md", "MEMORY.md")


def _file_path(name: str) -> str:
    return os.path.join(agent_home(), name)


def is_initialised() -> bool:
    return all(os.path.exists(_file_path(f)) for f in AGENT_FILES)


def read_agent_file(name: str) -> str:
    try:
        with open(_file_path(name), encoding="utf-8") as f:
            return f.read()
    except OSError:
        return ""


def agent_file_status() -> dict:
    return {
        "home": agent_home(),
        "initialised": is_initialised(),
        "files": [
            {"name": name, "exists": os.path.exists(_file_path(name)), "content": read_agent_file(name)}
            for name in AGENT_FILES
        ],
    }


def write_agent_file(name: str, content: str) -> None:
    if name not in AGENT_FILES:
        raise ValueError(f'"{name}" is not one of the agent\'s files')
    with open(_file_path(name), "w", encoding="utf-8") as f:
        f.write(content if content.endswith("\n") else f"{content}\n")


def run_wizard(input_: dict) -> None:
    """Write the three files from the wizard's answers.

    The templates are opinionated on purpose: an empty SOUL.md produces a
    characterless agent, and someone setting this up for the first time has no
    reason to know what belongs in one.
    """
    name = (input_.get("agentName") or "").strip() or "the agent"
    vibe = (input_.get("vibe") or "").strip()
    principles = (input_.get("principles") or "").strip()
    user_name = (input_.get("userName") or "").strip() or "the primary user"
    user_about = (input_.get("userAbout") or "").strip()
    user_prefers = (input_.get("userPrefers") or "").strip()

    # Every file opens by saying what it is and what to do with it. Content on
    # its own is ambiguous — handed the same words with no instruction, the model
    # read SOUL.md as notes about a third party and answered as itself.
    default_principles = "\n".join([
        "- Answer the question. No preamble, no restating what was asked.",
        "- Have a view. If something is a bad idea, say so and say why.",
        "- Be brief. A sentence that does the job beats a paragraph that also does the job.",
        "- Say when you are unsure, and say what would settle it.",
        "- You are often reached from a phone. Long replies are hard to read there.",
    ])
    soul = f"""# SOUL.md — who you are

**This file is your identity.** It is not notes about someone else. The name,
character and working style below are yours: answer as this, in every
conversation, on every channel. If it conflicts with a habit of yours, this
wins.

To change how you behave, edit this file.

---

# {name}

{vibe or f"You are {name}. You work for one person and you know them well."}

## How you work

{principles or default_principles}

## What you do not do

- Guess at facts you could check.
- Claim something is done when it is not.
- Pad an answer to look thorough.
"""

    user = f"""# PrimaryUser.md — who you work for

**This file describes the person you are talking to.** Assume what it says
rather than asking them to repeat it, and answer the way it describes. If you
learn something lasting about them, it belongs in MEMORY.md, not here — this
file is theirs to write.

---

# {user_name}

{user_about or "_What they work on, what they care about, what you should assume._"}

## Working with them

{user_prefers or "_How they like to be answered — length, tone, how much detail, what to skip._"}
"""

    memory = """# MEMORY.md — what you have learned

**This file is your long-term memory and you maintain it.** You are given it at
the start of every conversation, so anything written here you simply know.

Append to it when you learn something worth having next week: a decision and
the reason behind it, a preference you were corrected on, how something is set
up. Write what would not be obvious from the conversation you are in. Do not
record what you could look up, and do not restate what is already here.

Keep it in the sections below. Newest last.

---

## Decisions

_Choices that were made and why, so they are not argued twice._

## Preferences

_How things should be done, learned from being corrected._

## Context

_Names, systems, how things are set up. True and not obvious._
"""

    with open(_file_path("SOUL.md"), "w", encoding="utf-8") as f:
        f.write(soul)
    with open(_file_path("PrimaryUser.md"), "w", encoding="utf-8") as f:
        f.write(user)
    # Never clobber a memory that already exists — it is the one file here that
    # cannot be reconstructed.
    if not os.path.exists(_file_path("MEMORY.md")):
        with open(_file_path("MEMORY.md"), "w", encoding="utf-8") as f:
            f.write(memory)
