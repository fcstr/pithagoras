"""Turn a human workspace name into a directory name — port of slug.ts.

  "Cool Project"   -> "cool-project"
  "  My   App!  "  -> "my-app"

The result is also used as the session title, so one name drives both.
"""

import re
import unicodedata

_VALID = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


def slugify(value: str) -> str:
    s = unicodedata.normalize("NFKD", value)
    s = "".join(c for c in s if not unicodedata.combining(c))  # strip accents
    s = s.lower()
    s = re.sub(r"[^a-z0-9._-]+", "-", s)  # anything else (incl. spaces) becomes a hyphen
    s = re.sub(r"-{2,}", "-", s)
    s = re.sub(r"^[-._]+|[-._]+$", "", s)  # no leading/trailing separators
    return s[:64]


def is_valid_slug(slug: str) -> bool:
    """A slug is usable as a directory name and can't escape its parent."""
    return bool(_VALID.match(slug)) and slug not in (".", "..")
