"""
Supabase auth + per-user config layer.

Verifies Supabase JWTs (HS256 signed with project's JWT secret), fetches
the user's config from the `user_configs` table via Supabase REST API,
returns a UserContext that endpoints can use.

Environment variables required once Supabase is wired:
    SUPABASE_URL           https://xxxxx.supabase.co
    SUPABASE_ANON_KEY      eyJ...  (safe to ship in frontend too)
    SUPABASE_JWT_SECRET    (from Supabase dashboard → Settings → API → JWT Settings)

If none are set, multi-tenant endpoints return 503 with a clear message
("Supabase not configured"). Single-tenant endpoints continue working
from CLICKUP_TOKEN env var for backward compatibility during rollout.
"""

from __future__ import annotations

import base64
import hmac
import hashlib
import json
import os
import time
from dataclasses import dataclass, field
from typing import Any

import httpx
from fastapi import Depends, Header, HTTPException


SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "").strip()
SUPABASE_JWT_SECRET = os.environ.get("SUPABASE_JWT_SECRET", "").strip()


def supabase_configured() -> bool:
    return bool(SUPABASE_URL and SUPABASE_ANON_KEY and SUPABASE_JWT_SECRET)


# ---------- JWT verification (HS256, no external deps) ----------

def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def verify_supabase_jwt(token: str) -> dict:
    """Verify HS256 JWT signed with SUPABASE_JWT_SECRET.
    Returns the payload dict, or raises HTTPException(401).
    """
    if not SUPABASE_JWT_SECRET:
        raise HTTPException(503, "SUPABASE_JWT_SECRET not set")

    try:
        header_b64, payload_b64, sig_b64 = token.split(".")
    except ValueError:
        raise HTTPException(401, "Malformed token")

    try:
        header = json.loads(_b64url_decode(header_b64))
    except Exception:
        raise HTTPException(401, "Malformed token header")

    if header.get("alg") != "HS256":
        raise HTTPException(401, f"Unsupported alg {header.get('alg')}")

    signing_input = f"{header_b64}.{payload_b64}".encode()
    expected_sig = hmac.new(
        SUPABASE_JWT_SECRET.encode(),
        signing_input,
        hashlib.sha256,
    ).digest()
    actual_sig = _b64url_decode(sig_b64)
    if not hmac.compare_digest(expected_sig, actual_sig):
        raise HTTPException(401, "Invalid signature")

    try:
        payload = json.loads(_b64url_decode(payload_b64))
    except Exception:
        raise HTTPException(401, "Malformed token payload")

    exp = payload.get("exp")
    if exp and time.time() > exp:
        raise HTTPException(401, "Token expired")

    return payload


# ---------- User context + config loader ----------

@dataclass
class UserContext:
    user_id: str
    email: str
    is_anonymous: bool
    jwt: str
    config: dict[str, Any] = field(default_factory=dict)

    # Convenience accessors for ClickUp config
    @property
    def clickup_token(self) -> str:
        return (self.config.get("clickup_token") or "").strip()

    @property
    def list_deals(self) -> str:
        return self.config.get("clickup_list_active_deals") or ""

    @property
    def list_brokers(self) -> str:
        return self.config.get("clickup_list_brokers") or ""

    @property
    def list_followups(self) -> str:
        return self.config.get("clickup_list_followups") or ""

    @property
    def list_touchpoints(self) -> str:
        return self.config.get("clickup_list_touchpoints") or ""

    @property
    def wizard_completed(self) -> bool:
        return bool(self.config.get("wizard_completed"))


async def fetch_user_config(user_id: str, user_jwt: str) -> dict[str, Any]:
    """Read the user's row from user_configs via Supabase REST.
    Uses user's JWT so RLS enforces isolation.
    Returns {} if no config row exists yet.
    """
    if not supabase_configured():
        return {}

    url = f"{SUPABASE_URL}/rest/v1/user_configs"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {user_jwt}",
        "Accept": "application/json",
    }
    params = {"user_id": f"eq.{user_id}", "select": "*", "limit": "1"}

    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(url, headers=headers, params=params)
        if r.status_code != 200:
            return {}
        rows = r.json()
        return rows[0] if rows else {}


async def upsert_user_config(user_id: str, user_jwt: str, updates: dict) -> dict:
    """Update the user's config row (RLS-protected)."""
    if not supabase_configured():
        raise HTTPException(503, "Supabase not configured")

    url = f"{SUPABASE_URL}/rest/v1/user_configs"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {user_jwt}",
        "Content-Type": "application/json",
        "Prefer": "return=representation,resolution=merge-duplicates",
    }
    body = {"user_id": user_id, **updates}

    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(url, headers=headers, json=body)
        if r.status_code not in (200, 201):
            raise HTTPException(r.status_code, f"Supabase upsert failed: {r.text}")
        rows = r.json()
        return rows[0] if rows else {}


# ---------- FastAPI dependency ----------

async def get_current_user(
    authorization: str | None = Header(default=None),
) -> UserContext:
    """Dependency: extracts Bearer JWT, verifies, loads user config.
    Raises 401 if invalid, 503 if Supabase not configured.
    """
    if not supabase_configured():
        raise HTTPException(503, "Supabase not configured on server")

    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "Missing bearer token")

    token = authorization.split(" ", 1)[1].strip()
    payload = verify_supabase_jwt(token)

    user_id = payload.get("sub")
    if not user_id:
        raise HTTPException(401, "Token missing sub claim")

    email = payload.get("email", "")
    is_anon = payload.get("is_anonymous", False) or payload.get("role") == "anon"

    config = await fetch_user_config(user_id, token)

    return UserContext(
        user_id=user_id,
        email=email,
        is_anonymous=is_anon,
        jwt=token,
        config=config,
    )


async def get_optional_user(
    authorization: str | None = Header(default=None),
) -> UserContext | None:
    """Like get_current_user but returns None instead of raising.
    Used by endpoints that work with or without auth during transition.
    """
    if not supabase_configured() or not authorization:
        return None
    try:
        return await get_current_user(authorization=authorization)
    except HTTPException:
        return None


# ---------- Per-user ClickUp client ----------
# These replace the global cu_get/cu_post from broker_flow.py for multi-tenant endpoints.

CLICKUP_API = "https://api.clickup.com/api/v2"


def _cu_headers(user: UserContext) -> dict[str, str]:
    if not user.clickup_token:
        raise HTTPException(
            400,
            "ClickUp not connected for this user. Complete the setup wizard.",
        )
    return {"Authorization": user.clickup_token, "Content-Type": "application/json"}


async def user_cu_get(user: UserContext, path: str, params: dict | None = None) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(
            f"{CLICKUP_API}{path}", headers=_cu_headers(user), params=params or {}
        )
        r.raise_for_status()
        return r.json()


async def user_cu_post(user: UserContext, path: str, body: dict) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            f"{CLICKUP_API}{path}", headers=_cu_headers(user), json=body
        )
        r.raise_for_status()
        return r.json()


async def user_cu_put(user: UserContext, path: str, body: dict) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.put(
            f"{CLICKUP_API}{path}", headers=_cu_headers(user), json=body
        )
        r.raise_for_status()
        return r.json()


async def user_cu_delete(user: UserContext, path: str) -> None:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.delete(f"{CLICKUP_API}{path}", headers=_cu_headers(user))
        r.raise_for_status()
