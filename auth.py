"""
Supabase auth + per-user config layer.

Verifies Supabase JWTs signed with either:
- ES256 (ECC P-256, the modern default for new Supabase projects) — public
  key fetched from {SUPABASE_URL}/auth/v1/.well-known/jwks.json and cached.
- HS256 (the legacy shared-secret format) — uses SUPABASE_JWT_SECRET.

Fetches the user's config from the `user_configs` table via Supabase REST API,
returns a UserContext that endpoints can use.

Environment variables:
    SUPABASE_URL           https://xxxxx.supabase.co   (required)
    SUPABASE_ANON_KEY      eyJ...  (required — safe to ship in frontend too)
    SUPABASE_JWT_SECRET    optional — only needed if your project still issues
                           HS256 tokens (legacy). New projects use ES256 and
                           this can be left empty.

If SUPABASE_URL or SUPABASE_ANON_KEY are missing, multi-tenant endpoints return
503 ("Supabase not configured"). Single-tenant /legacy endpoints keep working
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
import jwt
from jwt import PyJWKClient
from fastapi import Depends, Header, HTTPException


SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "").strip()
SUPABASE_JWT_SECRET = os.environ.get("SUPABASE_JWT_SECRET", "").strip()


def supabase_configured() -> bool:
    # JWT_SECRET is now optional (only needed for HS256 fallback)
    return bool(SUPABASE_URL and SUPABASE_ANON_KEY)


# ---------- JWT verification (ES256 via JWKS + HS256 fallback) ----------

def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


# JWKS client — caches keys, refreshes automatically when a new kid appears
_jwks_client: PyJWKClient | None = None


def _get_jwks_client() -> PyJWKClient:
    global _jwks_client
    if _jwks_client is None:
        jwks_url = f"{SUPABASE_URL}/auth/v1/.well-known/jwks.json"
        _jwks_client = PyJWKClient(jwks_url, cache_keys=True, lifespan=3600)
    return _jwks_client


def verify_supabase_jwt(token: str) -> dict:
    """Verify a Supabase JWT. Supports ES256 (via JWKS) and HS256 (via shared
    secret). Returns the payload dict, or raises HTTPException(401).
    """
    # Peek at the header to decide which algorithm to use
    try:
        unverified_header = jwt.get_unverified_header(token)
    except jwt.InvalidTokenError as e:
        raise HTTPException(401, f"Malformed token: {e}")

    alg = unverified_header.get("alg")

    # Common verify options — Supabase JWTs have aud="authenticated"
    # We don't verify aud/iss strictly because they differ slightly between
    # anonymous and authenticated tokens; exp check is the critical one.
    options = {"verify_aud": False, "verify_iss": False}

    try:
        if alg == "ES256":
            signing_key = _get_jwks_client().get_signing_key_from_jwt(token).key
            payload = jwt.decode(
                token, signing_key, algorithms=["ES256"], options=options
            )
        elif alg == "HS256":
            if not SUPABASE_JWT_SECRET:
                raise HTTPException(
                    401,
                    "Token is HS256 but SUPABASE_JWT_SECRET not set",
                )
            payload = jwt.decode(
                token,
                SUPABASE_JWT_SECRET,
                algorithms=["HS256"],
                options=options,
            )
        else:
            raise HTTPException(401, f"Unsupported alg {alg}")
    except jwt.ExpiredSignatureError:
        raise HTTPException(401, "Token expired")
    except jwt.InvalidTokenError as e:
        raise HTTPException(401, f"Invalid token: {e}")

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
