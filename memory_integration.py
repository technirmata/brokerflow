"""Zep Cloud memory integration for BrokerFlow — per-user isolated memory.

Every broker signed into BrokerFlow gets their own Zep Cloud graph, scoped by
``brokerflow.<supabase_user_id>``. Add/recall calls here are safe to import
and call from any FastAPI endpoint — errors are caught, a Zep outage never
breaks a user-facing request.

Env vars (set in Render → BrokerFlow service → Environment):

    ZEP_API_KEY              z_...  (from app.getzep.com → API Keys)
    ZEP_API_URL              https://api.getzep.com   (optional, default)
    MEMORY_ZEP_ENABLED       1      (set to 0 to no-op everything — safe
                                    to deploy this file without the key)
    MEMORY_SAAS_NAMESPACE    brokerflow

Usage from an endpoint:

    from memory_integration import remember, recall

    @router.post("/api/v2/memory/remember")
    async def _remember(body: dict, user: UserContext = Depends(get_current_user)):
        remember(user_id=user.user_id, body=body["text"])
        return {"ok": True}

    @router.get("/api/v2/memory/recall")
    async def _recall(q: str, user: UserContext = Depends(get_current_user)):
        return {"hits": recall(user_id=user.user_id, query=q, limit=5)}
"""
from __future__ import annotations

import logging
import os
from typing import Any

log = logging.getLogger("memory_integration")

_client: Any = None  # memoized Zep client


def _enabled() -> bool:
    return os.environ.get("MEMORY_ZEP_ENABLED", "0") == "1" and bool(
        os.environ.get("ZEP_API_KEY")
    )


def _get() -> Any | None:
    global _client
    if not _enabled():
        return None
    if _client is None:
        try:
            from zep_cloud.client import Zep  # type: ignore[import-not-found]
            _client = Zep(api_key=os.environ["ZEP_API_KEY"])
        except Exception as e:  # noqa: BLE001
            log.warning("zep init failed: %s", e)
            return None
    return _client


def tenant_id(user_id: str | None) -> str:
    """Namespace a raw Supabase user_id into a Zep tenant id.

    Multiple SaaS products sharing one Zep account stay isolated via the
    namespace. BrokerFlow uses ``brokerflow.<uuid>``.
    """
    if not user_id:
        raise ValueError("user_id is required")
    ns = os.environ.get("MEMORY_SAAS_NAMESPACE", "brokerflow")
    return f"{ns}.{user_id}"


def _ensure_user(tid: str) -> None:
    client = _get()
    if client is None:
        return
    try:
        client.user.get(user_id=tid)
        return  # already exists
    except Exception:
        pass
    try:
        client.user.add(user_id=tid)
    except Exception as e:  # noqa: BLE001
        log.warning("zep user.add %s failed: %s", tid, e)


def remember(
    *,
    user_id: str,
    body: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    """Record a memory episode for this SaaS user. Safe to call unconditionally."""
    if not user_id:
        raise ValueError("user_id is required")
    client = _get()
    if client is None:
        return
    tid = tenant_id(user_id)
    try:
        _ensure_user(tid)
        client.graph.add(
            user_id=tid,
            type="text",
            data=body,
            **({"metadata": metadata} if metadata else {}),
        )
    except Exception as e:  # noqa: BLE001
        log.warning("zep graph.add %s failed: %s", tid, e)


def recall(
    *,
    user_id: str,
    query: str,
    limit: int = 5,
) -> list[dict[str, Any]]:
    """Hybrid search scoped to this user.

    Returns a list of dicts with keys: kind (edge/episode/node), content,
    score, metadata. Safe to inline into an LLM prompt.
    """
    if not user_id:
        raise ValueError("user_id is required")
    client = _get()
    if client is None:
        return []
    tid = tenant_id(user_id)
    try:
        resp = client.graph.search(user_id=tid, query=query, limit=limit)
    except Exception as e:  # noqa: BLE001
        log.warning("zep graph.search %s failed: %s", tid, e)
        return []

    out: list[dict[str, Any]] = []
    for edge in (getattr(resp, "edges", None) or []):
        out.append({
            "kind": "edge",
            "content": getattr(edge, "fact", "") or getattr(edge, "name", ""),
            "score": getattr(edge, "score", None),
            "metadata": {
                "valid_at": str(getattr(edge, "valid_at", "") or ""),
                "invalid_at": str(getattr(edge, "invalid_at", "") or ""),
            },
        })
    for ep in (getattr(resp, "episodes", None) or []):
        out.append({
            "kind": "episode",
            "content": getattr(ep, "content", "") or "",
            "score": getattr(ep, "score", None),
            "metadata": {"created_at": str(getattr(ep, "created_at", "") or "")},
        })
    for node in (getattr(resp, "nodes", None) or []):
        out.append({
            "kind": "node",
            "content": getattr(node, "summary", "") or getattr(node, "name", ""),
            "score": getattr(node, "score", None),
            "metadata": {"labels": getattr(node, "labels", None) or []},
        })
    return out


def forget(user_id: str | None) -> None:
    """Hard-delete all memory for this user. GDPR / account deletion path."""
    if not user_id:
        raise ValueError("user_id is required")
    client = _get()
    if client is None:
        return
    tid = tenant_id(user_id)
    try:
        client.user.delete(user_id=tid)
    except Exception as e:  # noqa: BLE001
        log.warning("zep user.delete %s failed: %s", tid, e)
