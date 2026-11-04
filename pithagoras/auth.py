"""Shared-password gate — port of auth.ts.

This portal can run arbitrary code on the host, so even on a Tailscale-only
network it should not be drivable by anything that happens to reach the port.
The cookie is an HMAC of an expiry stamp — no session store needed.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import time
from typing import Optional

PASSWORD = os.environ.get("PORTAL_PASSWORD", "")
SECRET = os.environ.get("PORTAL_SECRET") or secrets.token_hex(32)
COOKIE = (
    "pi_portal_sequential_auth"
    if os.environ.get("VOICE_COMPARISON") == "true" or os.environ.get("VOICE_PIPELINE_MODE") == "sequential"
    else "pi_portal_auth"
)
MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000  # 30 days

AUTH_ENABLED = len(PASSWORD) > 0

if not AUTH_ENABLED:
    print(
        "\n  WARNING: PORTAL_PASSWORD is not set — the portal is open to anyone who\n"
        "  can reach it, and it can run arbitrary commands on this machine.\n"
        "  Set PORTAL_PASSWORD (and PORTAL_SECRET to keep logins across restarts).\n"
    )


def _sign(expiry: int) -> str:
    mac = hmac.new(SECRET.encode(), str(expiry).encode(), hashlib.sha256).hexdigest()
    return f"{expiry}.{mac}"


def _verify(token: Optional[str]) -> bool:
    if not token:
        return False
    expiry_str, _, mac = token.partition(".")
    try:
        expiry = int(expiry_str)
    except ValueError:
        return False
    if expiry < time.time() * 1000:
        return False
    expected = hmac.new(SECRET.encode(), expiry_str.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(mac, expected)


def issue_cookie_value() -> tuple[str, str, int]:
    """Returns (cookie name, cookie value, max-age seconds)."""
    return COOKIE, _sign(int(time.time() * 1000) + MAX_AGE_MS), MAX_AGE_MS // 1000


def check_password(candidate) -> bool:
    if not isinstance(candidate, str) or not AUTH_ENABLED:
        return False
    return hmac.compare_digest(candidate.encode(), PASSWORD.encode())


def is_authed(req) -> bool:
    return not AUTH_ENABLED or _verify(req.cookies.get(COOKIE))


def require_auth(req):
    """Middleware: None to continue, a 401 response to short-circuit."""
    from .httpd import error_response

    if not AUTH_ENABLED:
        return None
    if _verify(req.cookies.get(COOKIE)):
        return None
    return error_response(401, "Unauthorized")
