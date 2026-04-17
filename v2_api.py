"""
BrokerFlow v2 API — multi-tenant endpoints.

Every endpoint requires a Supabase JWT and uses the user's stored
ClickUp token + other API keys from user_configs.

Mounted into the main FastAPI app as an APIRouter with prefix /api/v2.
"""

from __future__ import annotations

import json
import os
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException

from auth import (
    UserContext,
    get_current_user,
    upsert_user_config,
    supabase_configured,
    user_cu_get,
    user_cu_post,
    user_cu_put,
    user_cu_delete,
    SUPABASE_URL,
    SUPABASE_ANON_KEY,
)


router = APIRouter(prefix="/api/v2")


# =============================================================
# Masked key utility
# =============================================================

SENSITIVE_KEYS = {
    "clickup_token",
    "gmail_refresh_token",
    "smtp_pass",
    "twilio_auth_token",
    "anthropic_api_key",
}


def _mask(value: str | None) -> str | None:
    if not value:
        return None
    if len(value) <= 8:
        return "•" * len(value)
    return "•" * (len(value) - 4) + value[-4:]


def _mask_config(config: dict) -> dict:
    out = dict(config)
    for k in SENSITIVE_KEYS:
        if out.get(k):
            out[k] = _mask(out[k])
            out[f"{k}_set"] = True
        else:
            out[f"{k}_set"] = False
    return out


# =============================================================
# Config endpoints (Settings page)
# =============================================================

@router.get("/config")
async def get_config(user: UserContext = Depends(get_current_user)):
    """Return user's config with sensitive fields masked."""
    return _mask_config(user.config)


@router.put("/config")
async def update_config(
    payload: dict,
    user: UserContext = Depends(get_current_user),
):
    """Update any subset of user config fields.
    Empty string "" clears a field; omitted fields stay unchanged.
    Masked values (contain •) are ignored so Settings page re-save doesn't wipe keys.
    """
    allowed = {
        "clickup_token",
        "clickup_workspace_id",
        "clickup_space_id",
        "clickup_folder_id",
        "clickup_list_active_deals",
        "clickup_list_brokers",
        "clickup_list_followups",
        "clickup_list_templates",
        "clickup_list_touchpoints",
        "gmail_refresh_token",
        "gmail_email",
        "smtp_host",
        "smtp_port",
        "smtp_user",
        "smtp_pass",
        "smtp_from",
        "twilio_account_sid",
        "twilio_auth_token",
        "twilio_from_number",
        "anthropic_api_key",
        "wizard_step",
        "wizard_completed",
    }
    updates = {}
    for k, v in payload.items():
        if k not in allowed:
            continue
        if isinstance(v, str) and "•" in v:
            continue  # masked value, skip
        updates[k] = v

    if not updates:
        return _mask_config(user.config)

    row = await upsert_user_config(user.user_id, user.jwt, updates)
    return _mask_config(row)


# =============================================================
# Wizard: ClickUp
# =============================================================

@router.post("/wizard/clickup/test")
async def wizard_clickup_test(
    payload: dict,
    user: UserContext = Depends(get_current_user),
):
    """Verify a ClickUp token + return workspaces. Doesn't save yet."""
    token = (payload.get("token") or user.clickup_token or "").strip()
    if not token:
        raise HTTPException(400, "No token provided")

    headers = {"Authorization": token, "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get("https://api.clickup.com/api/v2/team", headers=headers)
        if r.status_code == 401:
            raise HTTPException(401, "ClickUp token invalid")
        if r.status_code != 200:
            raise HTTPException(r.status_code, f"ClickUp error: {r.text[:200]}")
        teams = r.json().get("teams", [])

    workspaces = [
        {"id": t["id"], "name": t.get("name", ""), "color": t.get("color", "")}
        for t in teams
    ]
    return {"ok": True, "workspaces": workspaces}


@router.post("/wizard/clickup/spaces")
async def wizard_clickup_spaces(
    payload: dict,
    user: UserContext = Depends(get_current_user),
):
    """List spaces in a workspace so user can pick where to put BrokerFlow lists."""
    token = (payload.get("token") or user.clickup_token or "").strip()
    workspace_id = payload.get("workspace_id", "").strip()
    if not token or not workspace_id:
        raise HTTPException(400, "token and workspace_id required")

    headers = {"Authorization": token, "Content-Type": "application/json"}
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(
            f"https://api.clickup.com/api/v2/team/{workspace_id}/space",
            headers=headers,
        )
        r.raise_for_status()
        spaces = r.json().get("spaces", [])
    return {"spaces": [{"id": s["id"], "name": s.get("name", "")} for s in spaces]}


@router.post("/wizard/clickup/setup")
async def wizard_clickup_setup(
    payload: dict,
    user: UserContext = Depends(get_current_user),
):
    """Create the BrokerFlow folder + 5 lists in the chosen space.
    Idempotent-ish: skips if folder named 'BrokerFlow' already exists.
    """
    token = (payload.get("token") or user.clickup_token or "").strip()
    workspace_id = payload.get("workspace_id", "").strip()
    space_id = payload.get("space_id", "").strip()
    if not all([token, workspace_id, space_id]):
        raise HTTPException(400, "token, workspace_id, space_id required")

    headers = {"Authorization": token, "Content-Type": "application/json"}
    folder_name = "BrokerFlow"
    list_specs = [
        ("clickup_list_active_deals", "Active Deals"),
        ("clickup_list_brokers", "Broker Directory"),
        ("clickup_list_followups", "Follow-ups Queue"),
        ("clickup_list_templates", "Message Templates"),
        ("clickup_list_touchpoints", "Touchpoints Log"),
    ]

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Look for existing folder
        r = await client.get(
            f"https://api.clickup.com/api/v2/space/{space_id}/folder", headers=headers
        )
        r.raise_for_status()
        folders = r.json().get("folders", [])
        folder = next((f for f in folders if f.get("name") == folder_name), None)

        if not folder:
            r = await client.post(
                f"https://api.clickup.com/api/v2/space/{space_id}/folder",
                headers=headers,
                json={"name": folder_name},
            )
            r.raise_for_status()
            folder = r.json()

        folder_id = folder["id"]

        # Existing lists in folder
        r = await client.get(
            f"https://api.clickup.com/api/v2/folder/{folder_id}/list", headers=headers
        )
        r.raise_for_status()
        existing_lists = {l.get("name"): l for l in r.json().get("lists", [])}

        created_ids: dict[str, str] = {}
        for config_key, list_name in list_specs:
            if list_name in existing_lists:
                created_ids[config_key] = existing_lists[list_name]["id"]
                continue
            r = await client.post(
                f"https://api.clickup.com/api/v2/folder/{folder_id}/list",
                headers=headers,
                json={"name": list_name},
            )
            r.raise_for_status()
            created_ids[config_key] = r.json()["id"]

    updates = {
        "clickup_token": token,
        "clickup_workspace_id": workspace_id,
        "clickup_space_id": space_id,
        "clickup_folder_id": folder_id,
        **created_ids,
        "wizard_step": 2,
    }
    row = await upsert_user_config(user.user_id, user.jwt, updates)
    return {"ok": True, "folder_id": folder_id, "lists": created_ids, "config": _mask_config(row)}


# =============================================================
# Wizard: Anthropic
# =============================================================

@router.post("/wizard/anthropic/test")
async def wizard_anthropic_test(
    payload: dict,
    user: UserContext = Depends(get_current_user),
):
    """Verify an Anthropic API key by making a cheap messages call."""
    key = (payload.get("api_key") or user.config.get("anthropic_api_key") or "").strip()
    if not key:
        raise HTTPException(400, "No api_key provided")

    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": key,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 10,
                "messages": [{"role": "user", "content": "ping"}],
            },
        )
        if r.status_code == 401:
            raise HTTPException(401, "Anthropic key invalid")
        if r.status_code >= 400:
            raise HTTPException(r.status_code, f"Anthropic error: {r.text[:200]}")

    await upsert_user_config(user.user_id, user.jwt, {"anthropic_api_key": key})
    return {"ok": True}


# =============================================================
# Wizard: Twilio
# =============================================================

@router.post("/wizard/twilio/test")
async def wizard_twilio_test(
    payload: dict,
    user: UserContext = Depends(get_current_user),
):
    """Verify Twilio creds by fetching the account record."""
    sid = (payload.get("account_sid") or user.config.get("twilio_account_sid") or "").strip()
    token = (payload.get("auth_token") or user.config.get("twilio_auth_token") or "").strip()
    from_number = (payload.get("from_number") or user.config.get("twilio_from_number") or "").strip()
    if not sid or not token:
        raise HTTPException(400, "account_sid and auth_token required")

    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(
            f"https://api.twilio.com/2010-04-01/Accounts/{sid}.json",
            auth=(sid, token),
        )
        if r.status_code == 401:
            raise HTTPException(401, "Twilio creds invalid")
        if r.status_code >= 400:
            raise HTTPException(r.status_code, f"Twilio error: {r.text[:200]}")
        acct = r.json()

    updates = {
        "twilio_account_sid": sid,
        "twilio_auth_token": token,
    }
    if from_number:
        updates["twilio_from_number"] = from_number
    await upsert_user_config(user.user_id, user.jwt, updates)
    return {"ok": True, "account_name": acct.get("friendly_name", "")}


# =============================================================
# Wizard: SMTP (Gmail app-password / Outlook / custom)
# =============================================================

@router.post("/wizard/smtp/test")
async def wizard_smtp_test(
    payload: dict,
    user: UserContext = Depends(get_current_user),
):
    """Verify SMTP config by attempting auth (no message sent)."""
    import smtplib
    host = (payload.get("host") or user.config.get("smtp_host") or "").strip()
    port = int(payload.get("port") or user.config.get("smtp_port") or 587)
    smtp_user = (payload.get("user") or user.config.get("smtp_user") or "").strip()
    pw = (payload.get("password") or user.config.get("smtp_pass") or "").strip()
    smtp_from = (payload.get("from") or user.config.get("smtp_from") or smtp_user).strip()

    if not all([host, port, smtp_user, pw]):
        raise HTTPException(400, "host, port, user, password required")

    try:
        with smtplib.SMTP(host, port, timeout=15) as s:
            s.starttls()
            s.login(smtp_user, pw)
    except smtplib.SMTPAuthenticationError:
        raise HTTPException(401, "SMTP auth failed")
    except Exception as e:
        raise HTTPException(400, f"SMTP error: {e}")

    await upsert_user_config(
        user.user_id,
        user.jwt,
        {
            "smtp_host": host,
            "smtp_port": port,
            "smtp_user": smtp_user,
            "smtp_pass": pw,
            "smtp_from": smtp_from,
        },
    )
    return {"ok": True}


# =============================================================
# Wizard: complete
# =============================================================

@router.post("/wizard/complete")
async def wizard_complete(user: UserContext = Depends(get_current_user)):
    row = await upsert_user_config(
        user.user_id, user.jwt, {"wizard_completed": True, "wizard_step": 99}
    )
    return _mask_config(row)


# =============================================================
# v2 data endpoints (per-user ClickUp)
# =============================================================

# These are thin shims that delegate into broker_flow's mappers but use
# the user's token + lists. They're defined here to avoid circular imports
# by importing broker_flow lazily.

def _lazy_mappers():
    from broker_flow import task_to_deal, task_to_broker, task_to_followup
    return task_to_deal, task_to_broker, task_to_followup


BROKER_LIST_HINTS = ("broker", "contact", "people", "directory", "vendor", "agent")
SKIP_LIST_HINTS = ("sop", "template", "process", "playbook")


async def _fetch_space_lists(user: UserContext) -> list[dict]:
    """Return all lists in the user's configured ClickUp space, across all folders and folderless.

    Each item: {id, name, folder_id, folder_name, source}
    """
    space_id = (user.config.get("clickup_space_id") or "").strip()
    if not space_id:
        return []

    out: list[dict] = []
    # Folderless lists
    try:
        r = await user_cu_get(user, f"/space/{space_id}/list", params={"archived": "false"})
        for l in r.get("lists", []):
            out.append({
                "id": l.get("id"),
                "name": l.get("name", ""),
                "folder_id": None,
                "folder_name": None,
                "source": "folderless",
            })
    except Exception:
        pass

    # Folders + lists inside each folder
    try:
        fr = await user_cu_get(user, f"/space/{space_id}/folder", params={"archived": "false"})
        for folder in fr.get("folders", []):
            fid = folder.get("id")
            fname = folder.get("name", "")
            for l in folder.get("lists", []) or []:
                out.append({
                    "id": l.get("id"),
                    "name": l.get("name", ""),
                    "folder_id": fid,
                    "folder_name": fname,
                    "source": "folder",
                })
    except Exception:
        pass

    return [l for l in out if l.get("id")]


def _classify_list(name: str) -> str:
    n = (name or "").lower()
    if any(h in n for h in SKIP_LIST_HINTS):
        return "skip"
    if any(h in n for h in BROKER_LIST_HINTS):
        return "brokers"
    return "deals"


async def _fetch_tasks_for_list(user: UserContext, list_id: str) -> list[dict]:
    try:
        r = await user_cu_get(
            user,
            f"/list/{list_id}/task",
            params={"include_closed": "true", "subtasks": "true"},
        )
        return r.get("tasks", []) or []
    except Exception:
        return []


@router.get("/clickup/space-lists")
async def v2_space_lists(user: UserContext = Depends(get_current_user)):
    """Return every list in the connected space with classification hint.

    Lets the UI show users exactly which lists feed Deals vs Brokers.
    """
    lists = await _fetch_space_lists(user)
    for l in lists:
        l["classification"] = _classify_list(l.get("name", ""))
    return {"space_id": user.config.get("clickup_space_id"), "lists": lists}


@router.get("/deals")
async def v2_list_deals(user: UserContext = Depends(get_current_user)):
    """Return all deals from the connected space.

    Sourcing rules (in order):
      1. If user configured a specific `clickup_list_active_deals` AND it has tasks, use only that.
      2. Else, union every list in the configured space whose name does NOT look like
         brokers/contacts/sops/templates. Each task is tagged with its source list name.
    """
    task_to_deal, _, _ = _lazy_mappers()

    # Try configured list first
    if user.list_deals:
        try:
            r = await user_cu_get(
                user,
                f"/list/{user.list_deals}/task",
                params={"include_closed": "true", "subtasks": "true"},
            )
            tasks = r.get("tasks", []) or []
            if tasks:
                return {"deals": [task_to_deal(t) for t in tasks], "source": "configured_list"}
        except Exception:
            pass

    # Fall back to space-wide union
    lists = await _fetch_space_lists(user)
    deals: list[dict] = []
    for l in lists:
        if _classify_list(l.get("name", "")) != "deals":
            continue
        list_id = l["id"]
        list_name = l.get("name", "")
        folder_name = l.get("folder_name")
        tasks = await _fetch_tasks_for_list(user, list_id)
        for t in tasks:
            d = task_to_deal(t)
            d["source_list_id"] = list_id
            d["source_list_name"] = list_name
            d["source_folder_name"] = folder_name
            deals.append(d)
    return {"deals": deals, "source": "space_wide"}


@router.get("/brokers")
async def v2_list_brokers(user: UserContext = Depends(get_current_user)):
    """Return all brokers from the connected space.

    Sourcing rules (in order):
      1. If user configured a specific `clickup_list_brokers` AND it has tasks, use only that.
      2. Else, union every list in the configured space whose name looks like brokers/contacts/people.
    """
    _, task_to_broker, _ = _lazy_mappers()

    if user.list_brokers:
        try:
            r = await user_cu_get(
                user, f"/list/{user.list_brokers}/task", params={"include_closed": "true"}
            )
            tasks = r.get("tasks", []) or []
            if tasks:
                return {"brokers": [task_to_broker(t) for t in tasks], "source": "configured_list"}
        except Exception:
            pass

    lists = await _fetch_space_lists(user)
    brokers: list[dict] = []
    for l in lists:
        if _classify_list(l.get("name", "")) != "brokers":
            continue
        list_id = l["id"]
        list_name = l.get("name", "")
        folder_name = l.get("folder_name")
        tasks = await _fetch_tasks_for_list(user, list_id)
        for t in tasks:
            b = task_to_broker(t)
            b["source_list_id"] = list_id
            b["source_list_name"] = list_name
            b["source_folder_name"] = folder_name
            brokers.append(b)
    return {"brokers": brokers, "source": "space_wide"}


@router.get("/followups")
async def v2_list_followups(user: UserContext = Depends(get_current_user)):
    if not user.list_followups:
        return {"followups": [], "source": "not_configured"}
    _, _, task_to_followup = _lazy_mappers()
    try:
        resp = await user_cu_get(
            user, f"/list/{user.list_followups}/task", params={"include_closed": "true"}
        )
        return {"followups": [task_to_followup(t) for t in resp.get("tasks", [])], "source": "configured_list"}
    except Exception:
        return {"followups": [], "source": "error"}


# =============================================================
# Relationship view: deal + broker + recent touchpoints
# =============================================================

@router.get("/deals/{deal_id}/full")
async def v2_deal_full(deal_id: str, user: UserContext = Depends(get_current_user)):
    """Deal + linked broker(s) + recent touchpoints."""
    task_to_deal, task_to_broker, _ = _lazy_mappers()
    task = await user_cu_get(user, f"/task/{deal_id}")
    deal = task_to_deal(task)

    brokers = []
    broker_ids: list[str] = []
    if deal.get("broker_id"):
        broker_ids.append(deal["broker_id"])

    # Also query deal_broker_links for many-to-many
    link_rows = await _supabase_rows(
        user,
        "deal_broker_links",
        params={"user_id": f"eq.{user.user_id}", "deal_id": f"eq.{deal_id}"},
    )
    for row in link_rows:
        bid = row.get("broker_id")
        if bid and bid not in broker_ids:
            broker_ids.append(bid)

    for bid in broker_ids:
        try:
            btask = await user_cu_get(user, f"/task/{bid}")
            brokers.append(task_to_broker(btask))
        except Exception:
            continue

    touchpoints = await _supabase_rows(
        user,
        "touchpoints",
        params={
            "user_id": f"eq.{user.user_id}",
            "deal_id": f"eq.{deal_id}",
            "order": "occurred_at.desc",
            "limit": "50",
        },
    )

    return {"deal": deal, "brokers": brokers, "touchpoints": touchpoints}


@router.get("/brokers/{broker_id}/full")
async def v2_broker_full(broker_id: str, user: UserContext = Depends(get_current_user)):
    """Broker + associated deals + full activity timeline."""
    task_to_deal, task_to_broker, _ = _lazy_mappers()
    task = await user_cu_get(user, f"/task/{broker_id}")
    broker = task_to_broker(task)

    # All deals with this broker_id
    deals: list[dict] = []
    if user.list_deals:
        resp = await user_cu_get(
            user, f"/list/{user.list_deals}/task", params={"include_closed": "true"}
        )
        for t in resp.get("tasks", []):
            d = task_to_deal(t)
            if d.get("broker_id") == broker_id:
                deals.append(d)

    # Plus from deal_broker_links
    link_rows = await _supabase_rows(
        user,
        "deal_broker_links",
        params={"user_id": f"eq.{user.user_id}", "broker_id": f"eq.{broker_id}"},
    )
    linked_deal_ids = {r["deal_id"] for r in link_rows}
    for did in linked_deal_ids:
        if any(d["id"] == did for d in deals):
            continue
        try:
            t = await user_cu_get(user, f"/task/{did}")
            deals.append(task_to_deal(t))
        except Exception:
            continue

    touchpoints = await _supabase_rows(
        user,
        "touchpoints",
        params={
            "user_id": f"eq.{user.user_id}",
            "broker_id": f"eq.{broker_id}",
            "order": "occurred_at.desc",
            "limit": "200",
        },
    )

    last_contact = touchpoints[0]["occurred_at"] if touchpoints else None
    return {
        "broker": broker,
        "deals": deals,
        "touchpoints": touchpoints,
        "last_contact": last_contact,
        "touchpoint_count": len(touchpoints),
    }


# =============================================================
# Touchpoints — log + query
# =============================================================

@router.post("/touchpoints")
async def v2_log_touchpoint(
    payload: dict,
    user: UserContext = Depends(get_current_user),
):
    """Log a manual or auto touchpoint.
    payload: { broker_id, broker_name?, broker_email?, broker_phone?,
               deal_id?, deal_name?,
               channel: email|sms|call|note,
               direction: outbound|inbound|note,
               subject?, body?, duration_seconds?,
               source?: manual|dashboard|gmail_scan|twilio_webhook|smtp,
               occurred_at?: iso timestamp }
    """
    required = ["broker_id", "channel", "direction"]
    for k in required:
        if not payload.get(k):
            raise HTTPException(400, f"Missing field: {k}")

    row = await _supabase_insert(user, "touchpoints", payload)
    return row


@router.get("/touchpoints")
async def v2_query_touchpoints(
    broker_id: str | None = None,
    deal_id: str | None = None,
    limit: int = 100,
    user: UserContext = Depends(get_current_user),
):
    params: dict[str, str] = {
        "user_id": f"eq.{user.user_id}",
        "order": "occurred_at.desc",
        "limit": str(limit),
    }
    if broker_id:
        params["broker_id"] = f"eq.{broker_id}"
    if deal_id:
        params["deal_id"] = f"eq.{deal_id}"
    rows = await _supabase_rows(user, "touchpoints", params=params)
    return {"touchpoints": rows}


@router.delete("/touchpoints/{touchpoint_id}")
async def v2_delete_touchpoint(
    touchpoint_id: str,
    user: UserContext = Depends(get_current_user),
):
    await _supabase_delete(
        user,
        "touchpoints",
        params={"id": f"eq.{touchpoint_id}", "user_id": f"eq.{user.user_id}"},
    )
    return {"ok": True}


# =============================================================
# AI drafting (Claude)
# =============================================================

@router.post("/draft/email")
async def v2_draft_email(
    payload: dict,
    user: UserContext = Depends(get_current_user),
):
    """Generate a warm, JPIG-voiced doc-request / reminder email.
    payload: { broker_id, deal_id?, cadence_day?, purpose?, extra_context? }
    """
    key = user.config.get("anthropic_api_key")
    if not key:
        raise HTTPException(400, "Anthropic key not set. Add it in Settings.")

    broker_id = payload.get("broker_id")
    if not broker_id:
        raise HTTPException(400, "broker_id required")

    _, task_to_broker, _ = _lazy_mappers()
    broker_task = await user_cu_get(user, f"/task/{broker_id}")
    broker = task_to_broker(broker_task)

    deal_ctx = ""
    if payload.get("deal_id"):
        task_to_deal, _, _ = _lazy_mappers()
        try:
            deal_task = await user_cu_get(user, f"/task/{payload['deal_id']}")
            deal = task_to_deal(deal_task)
            deal_ctx = (
                f"Deal: {deal.get('name','')}\n"
                f"Asset: {deal.get('asset_class','')}\n"
                f"Location: {deal.get('city','')}, {deal.get('state','')}\n"
                f"Ask: {deal.get('ask_price','')}\n"
                f"Stage: {deal.get('status','')}\n"
                f"Docs outstanding: {deal.get('docs_outstanding', [])}\n"
            )
        except Exception:
            pass

    # Recent conversation context
    recent_tps = await _supabase_rows(
        user,
        "touchpoints",
        params={
            "user_id": f"eq.{user.user_id}",
            "broker_id": f"eq.{broker_id}",
            "order": "occurred_at.desc",
            "limit": "5",
        },
    )
    history = "\n".join(
        f"- {t['occurred_at'][:10]} · {t['channel']} · {t['direction']}: {(t.get('subject') or t.get('body') or '')[:120]}"
        for t in recent_tps
    )

    purpose = payload.get("purpose", "doc request follow-up")
    cadence = payload.get("cadence_day", "")
    extra = payload.get("extra_context", "")

    system = (
        "You are writing a warm, professional email from Parth Patel at JP Investment "
        "Group (JPIG), an acquisitions shop. Voice: warm, relationship-first, "
        "low-pressure, brief. Never pushy. Prefer short paragraphs. End with a clear "
        "ask or next step. No marketing clichés. No 'I hope this email finds you well'. "
        "Sign 'Parth'."
    )
    user_msg = f"""Write an email to this broker.

BROKER
Name: {broker.get('name','')}
Firm: {broker.get('firm','')}
Relationship: {broker.get('relationship_strength','')}
Email: {broker.get('email','')}

DEAL CONTEXT
{deal_ctx or '(no specific deal)'}

PURPOSE: {purpose}
CADENCE DAY: {cadence or '(ad-hoc)'}
EXTRA: {extra}

RECENT TOUCHPOINTS
{history or '(none)'}

Return JSON: {{"subject": "...", "body": "..."}}"""

    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": key,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-5",
                "max_tokens": 800,
                "system": system,
                "messages": [{"role": "user", "content": user_msg}],
            },
        )
        if r.status_code >= 400:
            raise HTTPException(r.status_code, f"Anthropic error: {r.text[:200]}")
        data = r.json()
        text = data["content"][0]["text"]

    # Try to parse JSON response
    try:
        # Claude may wrap in ```json
        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("```")[1]
            if cleaned.startswith("json"):
                cleaned = cleaned[4:]
            cleaned = cleaned.strip()
        parsed = json.loads(cleaned)
        subject = parsed.get("subject", "")
        body = parsed.get("body", "")
    except Exception:
        # Fallback: heuristic split
        lines = text.split("\n", 1)
        subject = lines[0].replace("Subject:", "").strip() if lines else ""
        body = lines[1].strip() if len(lines) > 1 else text

    return {"subject": subject, "body": body, "to": broker.get("email", "")}


@router.post("/draft/sms")
async def v2_draft_sms(
    payload: dict,
    user: UserContext = Depends(get_current_user),
):
    """Generate a short SMS. payload: { broker_id, deal_id?, purpose? }"""
    key = user.config.get("anthropic_api_key")
    if not key:
        raise HTTPException(400, "Anthropic key not set.")

    broker_id = payload.get("broker_id")
    if not broker_id:
        raise HTTPException(400, "broker_id required")

    _, task_to_broker, _ = _lazy_mappers()
    broker_task = await user_cu_get(user, f"/task/{broker_id}")
    broker = task_to_broker(broker_task)

    purpose = payload.get("purpose", "quick check-in")

    system = (
        "Write a single SMS text message, max 160 chars, warm, casual, first-name "
        "basis. From Parth at JPIG. No links, no signature, no emoji unless natural."
    )
    user_msg = f"To: {broker.get('name','')} at {broker.get('firm','')}. Purpose: {purpose}."

    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": key,
                "anthropic-version": "2023-06-01",
                "Content-Type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 200,
                "system": system,
                "messages": [{"role": "user", "content": user_msg}],
            },
        )
        r.raise_for_status()
        text = r.json()["content"][0]["text"].strip()

    return {"body": text, "to": broker.get("phone", "")}


# =============================================================
# Send: Email (SMTP) + SMS (Twilio)
# =============================================================

@router.post("/send/email")
async def v2_send_email(
    payload: dict,
    user: UserContext = Depends(get_current_user),
):
    """Send an email via user's SMTP config. Logs a touchpoint.
    payload: { to, subject, body, broker_id?, deal_id? }
    """
    import smtplib
    from email.mime.text import MIMEText
    from email.utils import formatdate, make_msgid

    to = payload.get("to", "").strip()
    subject = payload.get("subject", "").strip()
    body = payload.get("body", "").strip()
    if not all([to, subject, body]):
        raise HTTPException(400, "to, subject, body required")

    host = user.config.get("smtp_host")
    port = user.config.get("smtp_port") or 587
    smtp_user = user.config.get("smtp_user")
    pw = user.config.get("smtp_pass")
    smtp_from = user.config.get("smtp_from") or smtp_user
    if not all([host, smtp_user, pw]):
        raise HTTPException(400, "SMTP not configured. Run wizard.")

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = smtp_from
    msg["To"] = to
    msg["Date"] = formatdate(localtime=True)
    msg["Message-ID"] = make_msgid()

    try:
        with smtplib.SMTP(host, int(port), timeout=30) as s:
            s.starttls()
            s.login(smtp_user, pw)
            s.sendmail(smtp_from, [to], msg.as_string())
    except Exception as e:
        raise HTTPException(502, f"SMTP send failed: {e}")

    # Log touchpoint
    tp_data = {
        "user_id": user.user_id,
        "channel": "email",
        "direction": "outbound",
        "source": "smtp",
        "subject": subject,
        "body": body,
        "external_id": msg["Message-ID"],
    }
    if payload.get("broker_id"):
        tp_data["broker_id"] = payload["broker_id"]
    else:
        tp_data["broker_id"] = f"email:{to}"
    if payload.get("deal_id"):
        tp_data["deal_id"] = payload["deal_id"]
    tp_data["broker_email"] = to

    touchpoint = await _supabase_insert(user, "touchpoints", tp_data)
    return {"ok": True, "touchpoint": touchpoint}


@router.post("/send/sms")
async def v2_send_sms(
    payload: dict,
    user: UserContext = Depends(get_current_user),
):
    """Send SMS via Twilio. Logs a touchpoint.
    payload: { to, body, broker_id?, deal_id? }
    """
    to = payload.get("to", "").strip()
    body = payload.get("body", "").strip()
    if not all([to, body]):
        raise HTTPException(400, "to, body required")

    sid = user.config.get("twilio_account_sid")
    token = user.config.get("twilio_auth_token")
    from_number = user.config.get("twilio_from_number")
    if not all([sid, token, from_number]):
        raise HTTPException(400, "Twilio not configured.")

    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.post(
            f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json",
            auth=(sid, token),
            data={"From": from_number, "To": to, "Body": body},
        )
        if r.status_code >= 400:
            raise HTTPException(r.status_code, f"Twilio error: {r.text[:200]}")
        msg = r.json()

    tp_data = {
        "user_id": user.user_id,
        "channel": "sms",
        "direction": "outbound",
        "source": "twilio_webhook",
        "body": body,
        "external_id": msg.get("sid"),
        "broker_phone": to,
    }
    if payload.get("broker_id"):
        tp_data["broker_id"] = payload["broker_id"]
    else:
        tp_data["broker_id"] = f"sms:{to}"
    if payload.get("deal_id"):
        tp_data["deal_id"] = payload["deal_id"]

    touchpoint = await _supabase_insert(user, "touchpoints", tp_data)
    return {"ok": True, "sid": msg.get("sid"), "touchpoint": touchpoint}


# =============================================================
# Supabase REST helpers (RLS-enforced via user JWT)
# =============================================================

async def _supabase_rows(
    user: UserContext,
    table: str,
    params: dict[str, str] | None = None,
) -> list[dict]:
    if not supabase_configured():
        return []
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {user.jwt}",
        "Accept": "application/json",
    }
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(url, headers=headers, params=params or {})
        if r.status_code != 200:
            return []
        return r.json()


async def _supabase_insert(
    user: UserContext, table: str, data: dict
) -> dict:
    if not supabase_configured():
        raise HTTPException(503, "Supabase not configured")
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {user.jwt}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    data = {"user_id": user.user_id, **data}
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.post(url, headers=headers, json=data)
        if r.status_code not in (200, 201):
            raise HTTPException(r.status_code, f"Supabase insert failed: {r.text[:200]}")
        rows = r.json()
        return rows[0] if rows else {}


async def _supabase_delete(
    user: UserContext, table: str, params: dict[str, str]
) -> None:
    if not supabase_configured():
        raise HTTPException(503, "Supabase not configured")
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {user.jwt}",
    }
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.delete(url, headers=headers, params=params)
        if r.status_code not in (200, 204):
            raise HTTPException(r.status_code, f"Supabase delete failed: {r.text[:200]}")


# =============================================================
# Deal ↔ Broker explicit links (many-to-many)
# =============================================================

@router.post("/deal-broker-links")
async def v2_link_deal_broker(
    payload: dict,
    user: UserContext = Depends(get_current_user),
):
    """Link a deal to a broker (or multiple brokers).
    payload: { deal_id, broker_id, role? }
    """
    for k in ("deal_id", "broker_id"):
        if not payload.get(k):
            raise HTTPException(400, f"Missing {k}")
    row = await _supabase_insert(
        user,
        "deal_broker_links",
        {
            "deal_id": payload["deal_id"],
            "broker_id": payload["broker_id"],
            "role": payload.get("role", "primary"),
        },
    )
    return row


@router.get("/deal-broker-links")
async def v2_list_links(
    deal_id: str | None = None,
    broker_id: str | None = None,
    user: UserContext = Depends(get_current_user),
):
    params: dict[str, str] = {"user_id": f"eq.{user.user_id}"}
    if deal_id:
        params["deal_id"] = f"eq.{deal_id}"
    if broker_id:
        params["broker_id"] = f"eq.{broker_id}"
    rows = await _supabase_rows(user, "deal_broker_links", params=params)
    return {"links": rows}


@router.delete("/deal-broker-links/{link_id}")
async def v2_unlink(link_id: str, user: UserContext = Depends(get_current_user)):
    await _supabase_delete(
        user,
        "deal_broker_links",
        params={"id": f"eq.{link_id}", "user_id": f"eq.{user.user_id}"},
    )
    return {"ok": True}


# =============================================================
# Meta / bootstrap
# =============================================================

@router.get("/meta")
async def v2_meta(user: UserContext = Depends(get_current_user)):
    """What's configured, what's not. Drives dashboard gating."""
    cfg = user.config
    return {
        "user_id": user.user_id,
        "email": user.email,
        "is_anonymous": user.is_anonymous,
        "wizard_completed": user.wizard_completed,
        "wizard_step": cfg.get("wizard_step", 1),
        "has_clickup": bool(cfg.get("clickup_token")),
        "has_lists": bool(user.list_deals and user.list_brokers),
        "has_gmail": bool(cfg.get("gmail_refresh_token")),
        "has_smtp": bool(cfg.get("smtp_host") and cfg.get("smtp_user")),
        "has_twilio": bool(cfg.get("twilio_account_sid")),
        "has_anthropic": bool(cfg.get("anthropic_api_key")),
        "lists": {
            "active_deals": user.list_deals,
            "brokers": user.list_brokers,
            "followups": user.list_followups,
            "touchpoints": user.list_touchpoints,
        },
    }


@router.get("/public/meta")
async def v2_public_meta():
    """Public endpoint — tells frontend if Supabase is wired.
    Used by the landing page before anyone logs in.
    """
    return {
        "supabase_configured": supabase_configured(),
        "supabase_url": SUPABASE_URL if supabase_configured() else None,
        "supabase_anon_key": SUPABASE_ANON_KEY if supabase_configured() else None,
    }


# =============================================================
# Helpers for write endpoints
# =============================================================

def _pack_helpers():
    """Lazy import pack/strip/extract so we don't bloat module load."""
    from broker_flow import pack_data, strip_data, extract_data
    return pack_data, strip_data, extract_data


async def _resolve_deals_list_id(user: UserContext) -> str:
    """Return the ClickUp list id to write new deals into.
    Priority: configured list, then first space list classified as 'deals'.
    """
    if user.list_deals:
        return user.list_deals
    lists = await _fetch_space_lists(user)
    for l in lists:
        if _classify_list(l.get("name", "")) == "deals":
            return l["id"]
    raise HTTPException(400, "No deals list found. Run the setup wizard or configure a list.")


async def _resolve_brokers_list_id(user: UserContext) -> str:
    if user.list_brokers:
        return user.list_brokers
    lists = await _fetch_space_lists(user)
    for l in lists:
        if _classify_list(l.get("name", "")) == "brokers":
            return l["id"]
    raise HTTPException(400, "No brokers list found. Run the setup wizard or configure a list.")


PRIORITY_MAP = {"urgent": 1, "high": 2, "normal": 3, "low": 4}


# =============================================================
# Deals — CRUD
# =============================================================

@router.post("/deals")
async def v2_create_deal(
    payload: dict,
    user: UserContext = Depends(get_current_user),
):
    """Create a deal as a ClickUp task. Packs structured fields into description JSON block.
    payload: any of {name, status, priority, tags, deal_id, asset_class, city, state,
                     units, ask_price, noi, cap_rate, broker_id, doc_status,
                     docs_received, docs_outstanding, next_action, next_action_date,
                     source, stage_entered, description_prose, list_id?}
    """
    pack_data, _, _ = _pack_helpers()
    task_to_deal, _, _ = _lazy_mappers()

    list_id = payload.get("list_id") or await _resolve_deals_list_id(user)
    name = (
        payload.get("name")
        or f"{payload.get('deal_id','JPIG-???')} · {payload.get('city','')}, {payload.get('state','')}"
    ).strip(" ·,")
    reserved = {"name", "status", "priority", "tags", "description_prose", "list_id"}
    data = {k: v for k, v in payload.items() if k not in reserved}
    prose = payload.get("description_prose", "")
    body: dict[str, Any] = {
        "name": name,
        "description": pack_data(prose, data),
    }
    if "status" in payload:
        body["status"] = payload["status"]
    if "priority" in payload:
        body["priority"] = PRIORITY_MAP.get(payload["priority"], 3)
    if "tags" in payload:
        body["tags"] = payload["tags"]

    task = await user_cu_post(user, f"/list/{list_id}/task", body)
    deal = task_to_deal(task)
    deal["source_list_id"] = list_id
    return deal


@router.put("/deals/{task_id}")
async def v2_update_deal(
    task_id: str,
    payload: dict,
    user: UserContext = Depends(get_current_user),
):
    """Patch a deal. Merges structured fields; preserves prose unless overridden."""
    pack_data, strip_data, extract_data = _pack_helpers()
    task_to_deal, _, _ = _lazy_mappers()

    existing = await user_cu_get(user, f"/task/{task_id}")
    existing_desc = existing.get("description", "") or ""
    current_data = extract_data(existing_desc)

    reserved = {"name", "status", "priority", "tags", "description_prose"}
    merged = {**current_data, **{k: v for k, v in payload.items() if k not in reserved}}
    prose = payload.get("description_prose", strip_data(existing_desc))

    body: dict[str, Any] = {"description": pack_data(prose, merged)}
    if "name" in payload:
        body["name"] = payload["name"]
    if "status" in payload:
        body["status"] = payload["status"]
    if "priority" in payload:
        body["priority"] = PRIORITY_MAP.get(payload["priority"], 3)

    task = await user_cu_put(user, f"/task/{task_id}", body)
    return task_to_deal(task)


@router.delete("/deals/{task_id}")
async def v2_delete_deal(
    task_id: str,
    user: UserContext = Depends(get_current_user),
):
    await user_cu_delete(user, f"/task/{task_id}")
    return {"ok": True}


# =============================================================
# Brokers — CRUD
# =============================================================

@router.post("/brokers")
async def v2_create_broker(
    payload: dict,
    user: UserContext = Depends(get_current_user),
):
    """Create a broker record in ClickUp. Structured fields go in JSON block.
    payload: {name, firm?, region?, email?, phone?, relationship_strength?,
              preferred_assets?, notes?, list_id?}
    """
    pack_data, _, _ = _pack_helpers()
    _, task_to_broker, _ = _lazy_mappers()

    list_id = payload.get("list_id") or await _resolve_brokers_list_id(user)
    name = (payload.get("name") or payload.get("firm") or "Unnamed broker").strip()
    reserved = {"name", "notes", "list_id"}
    data = {k: v for k, v in payload.items() if k not in reserved}
    prose = payload.get("notes", "")
    body = {"name": name, "description": pack_data(prose, data)}

    task = await user_cu_post(user, f"/list/{list_id}/task", body)
    broker = task_to_broker(task)
    broker["source_list_id"] = list_id
    return broker


@router.put("/brokers/{task_id}")
async def v2_update_broker(
    task_id: str,
    payload: dict,
    user: UserContext = Depends(get_current_user),
):
    pack_data, strip_data, extract_data = _pack_helpers()
    _, task_to_broker, _ = _lazy_mappers()

    existing = await user_cu_get(user, f"/task/{task_id}")
    existing_desc = existing.get("description", "") or ""
    current_data = extract_data(existing_desc)

    reserved = {"name", "notes"}
    merged = {**current_data, **{k: v for k, v in payload.items() if k not in reserved}}
    prose = payload.get("notes", strip_data(existing_desc))

    body: dict[str, Any] = {"description": pack_data(prose, merged)}
    if "name" in payload:
        body["name"] = payload["name"]

    task = await user_cu_put(user, f"/task/{task_id}", body)
    return task_to_broker(task)


@router.delete("/brokers/{task_id}")
async def v2_delete_broker(
    task_id: str,
    user: UserContext = Depends(get_current_user),
):
    await user_cu_delete(user, f"/task/{task_id}")
    return {"ok": True}


# =============================================================
# Outreach compose URLs (Gmail / Outlook / mailto)
# =============================================================

@router.post("/outreach/draft")
async def v2_outreach_draft(
    payload: dict,
    user: UserContext = Depends(get_current_user),
):
    """Build compose-deeplink URLs for Gmail, Outlook, and mailto.
    payload: {to, subject, body, cc?, bcc?}
    """
    from urllib.parse import quote
    to = (payload.get("to") or "").strip()
    subject = payload.get("subject", "")
    body = payload.get("body", "")
    cc = payload.get("cc", "")
    bcc = payload.get("bcc", "")

    q = lambda s: quote(s or "", safe="")
    mailto = f"mailto:{to}?subject={q(subject)}&body={q(body)}"
    if cc:
        mailto += f"&cc={q(cc)}"
    if bcc:
        mailto += f"&bcc={q(bcc)}"

    gmail = (
        f"https://mail.google.com/mail/?view=cm&fs=1"
        f"&to={q(to)}&su={q(subject)}&body={q(body)}"
    )
    if cc:
        gmail += f"&cc={q(cc)}"
    if bcc:
        gmail += f"&bcc={q(bcc)}"

    outlook = (
        f"https://outlook.office.com/mail/deeplink/compose"
        f"?to={q(to)}&subject={q(subject)}&body={q(body)}"
    )
    if cc:
        outlook += f"&cc={q(cc)}"

    return {"mailto": mailto, "gmail": gmail, "outlook": outlook}


# =============================================================
# Document intake — PDF parse (T-12, P&L, OM)
# =============================================================

from fastapi import File, UploadFile, Form


@router.post("/docs/parse")
async def v2_docs_parse(
    file: UploadFile = File(...),
    user: UserContext = Depends(get_current_user),
):
    """Upload a T-12 / P&L / OM PDF. Extracts text and runs heuristic regex
    to pull NOI, gross income, expenses, units, ask price, cap rate.
    Returns {filename, char_count, extracted, raw_text_preview}.
    """
    try:
        import pdfplumber
    except ImportError:
        raise HTTPException(500, "pdfplumber not installed")

    from broker_flow import heuristic_extract
    import tempfile, os as _os

    raw = await file.read()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tf:
        tf.write(raw)
        tmp_path = tf.name

    text = ""
    try:
        with pdfplumber.open(tmp_path) as pdf:
            for page in pdf.pages:
                text += (page.extract_text() or "") + "\n"
    finally:
        try:
            _os.unlink(tmp_path)
        except Exception:
            pass

    return {
        "filename": file.filename,
        "char_count": len(text),
        "extracted": heuristic_extract(text),
        "raw_text_preview": text[:3000],
    }


# =============================================================
# Seed sample brokers (one-tap onboarding helper)
# =============================================================

@router.post("/seed/sample-brokers")
async def v2_seed_sample_brokers(user: UserContext = Depends(get_current_user)):
    """Create 5 sample brokers in the user's broker list. Idempotent by name."""
    pack_data, _, _ = _pack_helpers()
    _, task_to_broker, _ = _lazy_mappers()

    list_id = await _resolve_brokers_list_id(user)

    # Check existing names to avoid dupes
    existing = await user_cu_get(
        user, f"/list/{list_id}/task", params={"include_closed": "true"}
    )
    existing_names = {t.get("name", "").lower() for t in existing.get("tasks", []) or []}

    samples = [
        {"name": "Mark Reynolds", "firm": "Marcus & Millichap", "region": "Southwest",
         "email": "mreynolds@mmreis.com", "phone": "+1-214-555-0101",
         "relationship_strength": "Warm", "preferred_assets": ["MF", "Hotel"]},
        {"name": "Jennifer Tran", "firm": "CBRE", "region": "Southeast",
         "email": "jen.tran@cbre.com", "phone": "+1-407-555-0155",
         "relationship_strength": "Hot", "preferred_assets": ["RV", "MHP"]},
        {"name": "David Park", "firm": "JLL", "region": "Southwest",
         "email": "dpark@jll.com", "phone": "+1-915-555-0199",
         "relationship_strength": "Cold", "preferred_assets": ["Hotel", "Industrial"]},
        {"name": "Sarah Nguyen", "firm": "Newmark", "region": "Southeast",
         "email": "snguyen@newmark.com", "phone": "+1-305-555-0177",
         "relationship_strength": "Trusted", "preferred_assets": ["Self-Storage", "MF"]},
        {"name": "Tom O'Brien", "firm": "Colliers", "region": "Midwest",
         "email": "tobrien@colliers.com", "phone": "+1-312-555-0122",
         "relationship_strength": "Warm", "preferred_assets": ["Industrial", "MF"]},
    ]
    created, skipped = [], []
    for s in samples:
        if s["name"].lower() in existing_names:
            skipped.append(s["name"])
            continue
        data = {k: v for k, v in s.items() if k != "name"}
        body = {"name": s["name"], "description": pack_data("", data)}
        task = await user_cu_post(user, f"/list/{list_id}/task", body)
        created.append(task_to_broker(task))
    return {"created": created, "skipped": skipped, "list_id": list_id}


# =============================================================
# Dashboard analytics — rollups for Reports / Heatmap / Scorecard
# =============================================================

@router.get("/analytics/summary")
async def v2_analytics_summary(user: UserContext = Depends(get_current_user)):
    """Single-call payload for dashboard cards, heatmap, velocity, broker scorecard.
    Keeps the frontend to one request for the Reports / Analytics view."""
    import time as _time
    task_to_deal, task_to_broker, _ = _lazy_mappers()

    # Pull deals
    deals_resp = await v2_list_deals(user)
    deals = deals_resp.get("deals", [])
    brokers_resp = await v2_list_brokers(user)
    brokers = brokers_resp.get("brokers", [])

    # Pipeline stage counts
    stage_counts: dict[str, int] = {}
    for d in deals:
        s = (d.get("status") or "unknown").lower()
        stage_counts[s] = stage_counts.get(s, 0) + 1

    # Asset-class × stage heatmap
    heatmap: dict[str, dict[str, int]] = {}
    for d in deals:
        ac = d.get("asset_class") or "Other"
        st = (d.get("status") or "unknown").lower()
        heatmap.setdefault(ac, {})
        heatmap[ac][st] = heatmap[ac].get(st, 0) + 1

    # State-based map tally
    by_state: dict[str, int] = {}
    for d in deals:
        st = (d.get("state") or "").upper()
        if st:
            by_state[st] = by_state.get(st, 0) + 1

    # Broker scorecard: deals + complete deals + conversion
    broker_deal_count: dict[str, int] = {}
    broker_complete: dict[str, int] = {}
    for d in deals:
        bid = d.get("broker_id") or ""
        if not bid:
            continue
        broker_deal_count[bid] = broker_deal_count.get(bid, 0) + 1
        if (d.get("status") or "").lower() in ("docs complete", "underwriting", "loi", "under contract", "closed"):
            broker_complete[bid] = broker_complete.get(bid, 0) + 1

    scorecard = []
    for b in brokers:
        bid = b.get("id")
        dc = broker_deal_count.get(bid, 0)
        cc = broker_complete.get(bid, 0)
        conv = round((cc / dc) * 100, 1) if dc else 0.0
        scorecard.append({
            "broker_id": bid,
            "name": b.get("name"),
            "firm": b.get("firm"),
            "deal_count": dc,
            "complete_count": cc,
            "conversion_pct": conv,
            "score": dc * cc,
        })
    scorecard.sort(key=lambda x: x["score"], reverse=True)

    # Velocity — deals by month (date_created ms)
    velocity: dict[str, int] = {}
    for d in deals:
        dc = d.get("date_created")
        if not dc:
            continue
        try:
            ts = int(dc) / 1000
            mo = _time.strftime("%Y-%m", _time.gmtime(ts))
            velocity[mo] = velocity.get(mo, 0) + 1
        except Exception:
            pass

    return {
        "totals": {
            "deals": len(deals),
            "brokers": len(brokers),
        },
        "stage_counts": stage_counts,
        "heatmap": heatmap,
        "by_state": by_state,
        "broker_scorecard": scorecard,
        "velocity": velocity,
    }


# =============================================================
# v2 UPGRADE — deep intake parser, geocode, graph/map/reports data
# =============================================================

@router.post("/intake/deep-parse")
async def v2_intake_deep_parse(
    files: list[UploadFile] = File(default=[]),
    text: str | None = Form(default=None),
    user: UserContext = Depends(get_current_user),
):
    """Advanced intake parser. Accepts PDF, XLSX, CSV, TXT, EML, DOCX or pasted
    text. Uses Claude (if user's Anthropic key set) to extract 40+ CRE fields
    with confidence scores and source attribution. Falls back to heuristic
    regex extraction when no AI key is configured.
    """
    import io as _io
    import re as _re
    import json as _json

    cfg = await _fetch_user_cfg(user)
    anthropic_key = cfg.get("anthropic_api_key")

    sources: list[dict] = []
    for uf in files or []:
        name = uf.filename or "upload"
        raw = await uf.read()
        ext = (name.rsplit(".", 1)[-1].lower() if "." in name else "")

        content = ""
        kind = ext or "bin"
        err = None

        try:
            if ext == "pdf":
                import pdfplumber
                with pdfplumber.open(_io.BytesIO(raw)) as pdf:
                    content = "\n\n".join((p.extract_text() or "") for p in pdf.pages)
            elif ext in ("txt", "md", "eml", "log"):
                content = raw.decode("utf-8", errors="replace")
            elif ext == "csv":
                content = raw.decode("utf-8", errors="replace")
            elif ext in ("xlsx", "xls"):
                try:
                    import openpyxl
                    wb = openpyxl.load_workbook(_io.BytesIO(raw), data_only=True)
                    parts = []
                    for sh in wb.worksheets:
                        parts.append(f"=== Sheet: {sh.title} ===")
                        for row in sh.iter_rows(values_only=True):
                            parts.append("\t".join("" if c is None else str(c) for c in row))
                    content = "\n".join(parts)
                except Exception as _e:
                    err = f"xlsx parse error: {_e}"
            elif ext in ("docx",):
                try:
                    import zipfile as _zf
                    from xml.etree import ElementTree as _ET
                    with _zf.ZipFile(_io.BytesIO(raw)) as zf:
                        xml = zf.read("word/document.xml").decode("utf-8", errors="replace")
                    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
                    root = _ET.fromstring(xml)
                    paras = ["".join(t.text or "" for t in p.findall(".//w:t", ns))
                            for p in root.findall(".//w:p", ns)]
                    content = "\n".join(paras)
                except Exception as _e:
                    err = f"docx parse error: {_e}"
            else:
                err = f"Unsupported file type: .{ext}"
        except Exception as _e:
            err = f"Extract error: {_e}"

        sources.append({
            "name": name,
            "kind": kind,
            "bytes": len(raw),
            "chars": len(content),
            "text": content,
            "error": err,
        })

    if text:
        sources.append({
            "name": "pasted-text",
            "kind": "text",
            "bytes": len(text.encode("utf-8")),
            "chars": len(text),
            "text": text,
            "error": None,
        })

    if not sources:
        raise HTTPException(400, "No files or text provided")

    total_chars = sum(s["chars"] for s in sources)
    if total_chars == 0:
        return {
            "sources": [{k: v for k, v in s.items() if k != "text"} for s in sources],
            "extracted": {},
            "mode": "empty",
            "message": "No readable text in sources.",
        }

    if not anthropic_key:
        # Heuristic-only fallback
        from broker_flow import heuristic_extract
        combined = "\n\n".join(s["text"] for s in sources if s["text"])
        basic = heuristic_extract(combined)
        # Wrap each scalar in our rich schema shape for frontend consistency
        def _wrap(v):
            return {"value": v, "confidence": 0.5 if v else 0.0, "source": "regex heuristic"}
        extracted = {
            "property": {
                "noi": _wrap(basic.get("noi")),
                "ask_price": _wrap(basic.get("ask_price")),
                "units": _wrap(basic.get("units")),
                "cap_rate": _wrap(basic.get("cap_rate")),
            },
            "financials": {
                "gross_revenue": _wrap(basic.get("gross_income")),
                "operating_expenses": _wrap(basic.get("expenses")),
            },
        }
        return {
            "sources": [{k: v for k, v in s.items() if k != "text"} for s in sources],
            "extracted": extracted,
            "mode": "heuristic",
            "message": "No Anthropic key configured. Using regex fallback. Add key in Admin → AI for deep parse.",
        }

    # Deep parse via Claude
    source_blob = "\n\n".join(
        f"=== FILE: {s['name']} ({s['kind']}, {s['chars']} chars) ==="
        f"\n{s['text'][:35000]}"
        for s in sources if s["text"]
    )
    source_blob = source_blob[:140000]  # hard cap

    schema_hint = """{
  "property": {
    "name": {"value": null, "confidence": 0, "source": ""},
    "address": {"value": null, "confidence": 0, "source": ""},
    "city": {"value": null, "confidence": 0, "source": ""},
    "state": {"value": null, "confidence": 0, "source": ""},
    "zip": {"value": null, "confidence": 0, "source": ""},
    "asset_class": {"value": null, "confidence": 0, "source": ""},
    "asset_subtype": {"value": null, "confidence": 0, "source": ""},
    "year_built": {"value": null, "confidence": 0, "source": ""},
    "year_renovated": {"value": null, "confidence": 0, "source": ""},
    "units": {"value": null, "confidence": 0, "source": ""},
    "rooms": {"value": null, "confidence": 0, "source": ""},
    "pads": {"value": null, "confidence": 0, "source": ""},
    "sqft": {"value": null, "confidence": 0, "source": ""},
    "occupancy_pct": {"value": null, "confidence": 0, "source": ""},
    "avg_rent": {"value": null, "confidence": 0, "source": ""}
  },
  "financials": {
    "ask_price": {"value": null, "confidence": 0, "source": ""},
    "noi": {"value": null, "confidence": 0, "source": ""},
    "cap_rate": {"value": null, "confidence": 0, "source": ""},
    "gross_revenue": {"value": null, "confidence": 0, "source": ""},
    "operating_expenses": {"value": null, "confidence": 0, "source": ""},
    "expense_ratio_pct": {"value": null, "confidence": 0, "source": ""},
    "price_per_unit": {"value": null, "confidence": 0, "source": ""},
    "price_per_sqft": {"value": null, "confidence": 0, "source": ""},
    "debt_in_place": {"value": null, "confidence": 0, "source": ""},
    "debt_rate_pct": {"value": null, "confidence": 0, "source": ""},
    "debt_maturity": {"value": null, "confidence": 0, "source": ""},
    "seller_financing_offered": {"value": null, "confidence": 0, "source": ""}
  },
  "market": {
    "msa": {"value": null, "confidence": 0, "source": ""},
    "submarket": {"value": null, "confidence": 0, "source": ""},
    "population_trend": {"value": null, "confidence": 0, "source": ""},
    "market_cap_rate_range": {"value": null, "confidence": 0, "source": ""}
  },
  "rent_roll": {
    "unit_mix": {"value": null, "confidence": 0, "source": ""},
    "avg_in_place_rent": {"value": null, "confidence": 0, "source": ""},
    "lease_exp_next_12mo_pct": {"value": null, "confidence": 0, "source": ""},
    "concessions": {"value": null, "confidence": 0, "source": ""}
  },
  "t12": {
    "trailing_noi_trend": {"value": null, "confidence": 0, "source": ""},
    "seasonality_notes": {"value": null, "confidence": 0, "source": ""},
    "anomalies": {"value": null, "confidence": 0, "source": ""}
  },
  "om_highlights": {
    "value_add_angle": {"value": null, "confidence": 0, "source": ""},
    "seller_motivation": {"value": null, "confidence": 0, "source": ""},
    "exclusive_listing": {"value": null, "confidence": 0, "source": ""},
    "call_for_offers_date": {"value": null, "confidence": 0, "source": ""},
    "timeline_to_close": {"value": null, "confidence": 0, "source": ""}
  },
  "broker": {
    "firm": {"value": null, "confidence": 0, "source": ""},
    "name": {"value": null, "confidence": 0, "source": ""},
    "direct_dial": {"value": null, "confidence": 0, "source": ""},
    "email": {"value": null, "confidence": 0, "source": ""},
    "license_state": {"value": null, "confidence": 0, "source": ""}
  },
  "red_flags": {
    "deferred_maintenance": {"value": null, "confidence": 0, "source": ""},
    "environmental_issues": {"value": null, "confidence": 0, "source": ""},
    "litigation": {"value": null, "confidence": 0, "source": ""},
    "unusual_clauses": {"value": null, "confidence": 0, "source": ""},
    "other_concerns": {"value": null, "confidence": 0, "source": ""}
  },
  "summary_narrative": {"value": null, "confidence": 0, "source": ""}
}"""

    prompt = (
        "You are a commercial real estate acquisitions analyst at JP Investment Group. "
        "Extract every detail from these intake documents for a new deal. "
        "For each field, return {\"value\": ..., \"confidence\": 0.0-1.0, \"source\": \"<filename>: '<exact ≤80 char quote>'\"}. "
        "Use null for fields you cannot find. Numbers should be raw numeric values (no $ or commas). "
        "Asset classes: MF, Hotel, RV, MHP, Self-Storage, Industrial, Office, Retail, Mixed-Use. "
        "summary_narrative: one-paragraph investment thesis summary.\n\n"
        "Return ONLY the JSON object, no preamble, no code fences. Schema:\n\n"
        f"{schema_hint}\n\nSOURCES:\n{source_blob}"
    )

    try:
        async with httpx.AsyncClient(timeout=90.0) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": anthropic_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-5",
                    "max_tokens": 8000,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            if resp.status_code != 200:
                raise HTTPException(502, f"Anthropic error {resp.status_code}: {resp.text[:500]}")
            data = resp.json()
            raw_out = data.get("content", [{}])[0].get("text", "")

        m = _re.search(r"\{[\s\S]*\}", raw_out)
        if not m:
            return {
                "sources": [{k: v for k, v in s.items() if k != "text"} for s in sources],
                "extracted": {},
                "mode": "anthropic",
                "error": "Model returned no JSON",
                "raw_preview": raw_out[:500],
            }
        try:
            extracted = _json.loads(m.group(0))
        except _json.JSONDecodeError as _e:
            return {
                "sources": [{k: v for k, v in s.items() if k != "text"} for s in sources],
                "extracted": {},
                "mode": "anthropic",
                "error": f"JSON decode: {_e}",
                "raw_preview": raw_out[:500],
            }
    except HTTPException:
        raise
    except Exception as _e:
        raise HTTPException(502, f"Deep parse failed: {_e}")

    return {
        "sources": [{k: v for k, v in s.items() if k != "text"} for s in sources],
        "extracted": extracted,
        "mode": "anthropic",
        "model": "claude-sonnet-4-5",
    }


async def _fetch_user_cfg(user: UserContext) -> dict:
    """Small helper — pulls user_configs row, returns dict (or empty)."""
    if not supabase_configured():
        return {}
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(
                f"{SUPABASE_URL}/rest/v1/user_configs",
                params={"user_id": f"eq.{user.supabase_user_id}", "select": "*"},
                headers={
                    "apikey": SUPABASE_ANON_KEY,
                    "Authorization": f"Bearer {user.supabase_jwt}",
                },
            )
            if r.status_code == 200:
                rows = r.json()
                return rows[0] if rows else {}
    except Exception:
        pass
    return {}


@router.post("/geocode")
async def v2_geocode(payload: dict, user: UserContext = Depends(get_current_user)):
    """Geocode a single address via Nominatim (OpenStreetMap). Free, no key.
    Body: {"q": "123 Main St, Austin, TX"}. Returns {lat, lng, display_name}.
    """
    q = (payload or {}).get("q") or ""
    q = q.strip()
    if not q:
        raise HTTPException(400, "Address required")
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": q, "format": "json", "limit": 1, "addressdetails": 1},
                headers={"User-Agent": "BrokerFlow/2.0 (acquisitions tool)"},
            )
            if r.status_code != 200:
                return {"error": f"Nominatim {r.status_code}", "q": q}
            arr = r.json()
            if not arr:
                return {"lat": None, "lng": None, "q": q, "error": "Not found"}
            hit = arr[0]
            return {
                "q": q,
                "lat": float(hit.get("lat", 0)),
                "lng": float(hit.get("lon", 0)),
                "display_name": hit.get("display_name"),
                "country": (hit.get("address") or {}).get("country"),
                "state": (hit.get("address") or {}).get("state"),
                "city": (hit.get("address") or {}).get("city") or (hit.get("address") or {}).get("town"),
            }
    except Exception as _e:
        return {"error": str(_e), "q": q}


@router.get("/analytics/graph")
async def v2_analytics_graph(user: UserContext = Depends(get_current_user)):
    """Network graph data — nodes + edges for broker ↔ deal ↔ market viz.
    Returns D3-ready {nodes: [{id,type,label,value,meta}], links: [{source,target,weight}]}.
    """
    deals_resp = await v2_list_deals(user)
    brokers_resp = await v2_list_brokers(user)
    deals = deals_resp.get("deals", [])
    brokers = brokers_resp.get("brokers", [])

    # Touchpoint counts per (deal,broker)
    tp_counts: dict[tuple, int] = {}
    try:
        tp_resp = await v2_query_touchpoints(user=user)
        for tp in tp_resp.get("touchpoints", []):
            key = (tp.get("deal_id") or "", tp.get("broker_id") or "")
            tp_counts[key] = tp_counts.get(key, 0) + 1
    except Exception:
        pass

    nodes = []
    for b in brokers:
        nodes.append({
            "id": f"b:{b['id']}",
            "type": "broker",
            "label": b.get("name") or "",
            "sublabel": b.get("firm") or "",
            "group": b.get("relationship_strength") or "Cold",
            "value": max(1, sum(1 for d in deals if d.get("broker_id") == b["id"])),
        })
    markets: dict[str, int] = {}
    for d in deals:
        nodes.append({
            "id": f"d:{d['id']}",
            "type": "deal",
            "label": d.get("name") or "",
            "sublabel": d.get("status") or "",
            "group": (d.get("status") or "unknown").lower(),
            "value": 1,
        })
        mk = (d.get("state") or "").upper()
        if mk:
            markets[mk] = markets.get(mk, 0) + 1
    for mk, n in markets.items():
        nodes.append({
            "id": f"m:{mk}",
            "type": "market",
            "label": mk,
            "sublabel": f"{n} deal(s)",
            "group": "market",
            "value": max(1, n),
        })

    links = []
    for d in deals:
        bid = d.get("broker_id")
        if bid:
            links.append({
                "source": f"b:{bid}",
                "target": f"d:{d['id']}",
                "weight": 1 + tp_counts.get((d["id"], bid), 0),
                "kind": "broker-deal",
            })
        mk = (d.get("state") or "").upper()
        if mk:
            links.append({
                "source": f"d:{d['id']}",
                "target": f"m:{mk}",
                "weight": 1,
                "kind": "deal-market",
            })

    return {"nodes": nodes, "links": links, "counts": {"brokers": len(brokers), "deals": len(deals), "markets": len(markets)}}


@router.get("/analytics/heatmap-activity")
async def v2_analytics_heatmap_activity(user: UserContext = Depends(get_current_user)):
    """Broker × week activity heatmap — touchpoints per broker per ISO week."""
    import datetime as _dt

    tp_resp = await v2_query_touchpoints(user=user)
    tps = tp_resp.get("touchpoints", [])
    brokers_resp = await v2_list_brokers(user)
    brokers = brokers_resp.get("brokers", [])
    bmap = {b["id"]: b for b in brokers}

    weeks = []
    today = _dt.date.today()
    monday = today - _dt.timedelta(days=today.weekday())
    for i in range(11, -1, -1):  # last 12 weeks
        wk = monday - _dt.timedelta(weeks=i)
        weeks.append(wk.isoformat())

    grid: dict[str, dict[str, int]] = {b["id"]: {w: 0 for w in weeks} for b in brokers}
    for tp in tps:
        bid = tp.get("broker_id")
        ts = tp.get("sent_at") or tp.get("created_at")
        if not bid or not ts or bid not in grid:
            continue
        try:
            dt = _dt.datetime.fromisoformat(ts.replace("Z", "+00:00"))
            wk_monday = (dt.date() - _dt.timedelta(days=dt.date().weekday())).isoformat()
            if wk_monday in grid[bid]:
                grid[bid][wk_monday] = grid[bid][wk_monday] + 1
        except Exception:
            continue

    rows = []
    for b in brokers:
        bid = b["id"]
        row = {
            "broker_id": bid,
            "name": b.get("name"),
            "firm": b.get("firm"),
            "tier": b.get("relationship_strength") or "Cold",
            "cells": [grid[bid][w] for w in weeks],
            "total": sum(grid[bid].values()),
        }
        rows.append(row)
    rows.sort(key=lambda r: r["total"], reverse=True)
    return {"weeks": weeks, "rows": rows}


@router.get("/analytics/reports")
async def v2_analytics_reports(user: UserContext = Depends(get_current_user)):
    """Extended reporting rollup: stage funnel, avg days per stage, broker
    leaderboard, source attribution, cadence conversion rate.
    """
    import time as _time
    import datetime as _dt

    deals_resp = await v2_list_deals(user)
    deals = deals_resp.get("deals", [])
    brokers_resp = await v2_list_brokers(user)
    brokers = brokers_resp.get("brokers", [])
    bmap = {b["id"]: b for b in brokers}

    stages = ["incoming leads", "docs requested", "docs complete", "underwriting", "loi", "under contract", "closed", "dead"]
    funnel = {s: 0 for s in stages}
    for d in deals:
        s = (d.get("status") or "").lower()
        if s in funnel:
            funnel[s] = funnel[s] + 1

    # Avg time in stage — rough, from date_created to now for non-terminal deals
    now_ms = int(_time.time() * 1000)
    days_in_stage: dict[str, list[int]] = {s: [] for s in stages}
    for d in deals:
        dc = d.get("date_created")
        s = (d.get("status") or "").lower()
        if not dc or s not in days_in_stage:
            continue
        try:
            age = (now_ms - int(dc)) / (1000 * 86400)
            days_in_stage[s].append(age)
        except Exception:
            pass
    avg_days = {s: (round(sum(v) / len(v), 1) if v else 0) for s, v in days_in_stage.items()}

    # Broker leaderboard — richer
    leaderboard = []
    for b in brokers:
        bid = b["id"]
        bdeals = [d for d in deals if d.get("broker_id") == bid]
        closed = sum(1 for d in bdeals if (d.get("status") or "").lower() == "closed")
        active = sum(1 for d in bdeals if (d.get("status") or "").lower() not in ("closed", "dead"))
        docs_in = sum(1 for d in bdeals if (d.get("status") or "").lower() in ("docs complete", "underwriting", "loi", "under contract", "closed"))
        leaderboard.append({
            "broker_id": bid,
            "name": b.get("name"),
            "firm": b.get("firm"),
            "tier": b.get("relationship_strength") or "Cold",
            "total_deals": len(bdeals),
            "active_deals": active,
            "closed_deals": closed,
            "docs_in_rate": round((docs_in / len(bdeals)) * 100, 1) if bdeals else 0,
            "close_rate": round((closed / len(bdeals)) * 100, 1) if bdeals else 0,
            "score": len(bdeals) * 10 + closed * 50 + docs_in * 5,
        })
    leaderboard.sort(key=lambda x: x["score"], reverse=True)

    # Source attribution — asset class mix per broker tier
    tier_x_asset: dict[str, dict[str, int]] = {}
    for d in deals:
        bid = d.get("broker_id")
        b = bmap.get(bid) if bid else None
        tier = (b.get("relationship_strength") if b else None) or "Unknown"
        ac = d.get("asset_class") or "Other"
        tier_x_asset.setdefault(tier, {})
        tier_x_asset[tier][ac] = tier_x_asset[tier].get(ac, 0) + 1

    # Cadence conversion — deals moved past "incoming leads" vs total
    total = len(deals)
    moved = sum(1 for d in deals if (d.get("status") or "").lower() not in ("incoming leads", "dead"))
    cadence_conv = round((moved / total) * 100, 1) if total else 0

    return {
        "funnel": funnel,
        "avg_days_in_stage": avg_days,
        "leaderboard": leaderboard,
        "tier_x_asset": tier_x_asset,
        "cadence_conversion_pct": cadence_conv,
        "totals": {"deals": total, "brokers": len(brokers), "moved_past_incoming": moved},
    }


# =============================================================================
# USER CONFIG — save integration keys (writes to Supabase user_configs table)
# =============================================================================

def _mask(val: str | None) -> str | None:
    if not val:
        return None
    s = str(val)
    if len(s) <= 8:
        return "••••"
    return s[:4] + "••••" + s[-4:]


@router.get("/user-config")
async def v2_get_user_config(user: UserContext = Depends(get_current_user)):
    """Returns current user config with sensitive fields masked."""
    cfg = await _fetch_user_cfg(user)
    masked = dict(cfg)
    for k in ("clickup_token", "smtp_password", "twilio_token", "twilio_sid", "anthropic_api_key"):
        if masked.get(k):
            masked[f"{k}_mask"] = _mask(masked.pop(k))
    return masked


@router.put("/user-config")
async def v2_put_user_config(payload: dict, user: UserContext = Depends(get_current_user)):
    """Merge-update user_configs row for the current user.

    Only fields present in the payload are touched. Secrets containing '•' are
    treated as 'unchanged' (the client is sending back the mask) and skipped.
    """
    if not supabase_configured():
        raise HTTPException(500, "Supabase not configured")

    clean: dict = {}
    for k, v in (payload or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and "•" in v:
            continue  # skip masked values
        clean[k] = v

    if not clean:
        return {"ok": True, "updated": 0}

    clean["user_id"] = user.supabase_user_id

    # Upsert via PostgREST. Conflict on user_id; prefer-resolution=merge-duplicates.
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.post(
                f"{SUPABASE_URL}/rest/v1/user_configs",
                params={"on_conflict": "user_id"},
                headers={
                    "apikey": SUPABASE_ANON_KEY,
                    "Authorization": f"Bearer {user.supabase_jwt}",
                    "Content-Type": "application/json",
                    "Prefer": "resolution=merge-duplicates,return=representation",
                },
                json=clean,
            )
            if r.status_code not in (200, 201):
                raise HTTPException(r.status_code, f"Supabase error: {r.text[:300]}")
            return {"ok": True, "updated": len(clean), "row": (r.json() or [None])[0]}
    except HTTPException:
        raise
    except Exception as _e:
        raise HTTPException(502, f"user-config write failed: {_e}")


# =============================================================================
# ADMIN — health, integration tests, danger zone, team invites
# =============================================================================

@router.get("/admin/health")
async def v2_admin_health(user: UserContext = Depends(get_current_user)):
    """Lightweight env health — reports which integrations are wired up per user."""
    cfg = await _fetch_user_cfg(user)
    status: dict[str, str] = {}
    status["supabase"] = "ok" if supabase_configured() else "fail"
    status["clickup"] = "ok" if cfg.get("clickup_token") else "unknown"
    status["smtp"] = "ok" if cfg.get("smtp_password") and cfg.get("smtp_host") else "unknown"
    status["twilio"] = "ok" if cfg.get("twilio_sid") and cfg.get("twilio_token") else "unknown"
    status["anthropic"] = "ok" if cfg.get("anthropic_api_key") else "unknown"
    status["nominatim"] = "ok"  # always available
    return status


@router.post("/admin/test")
async def v2_admin_test(payload: dict, user: UserContext = Depends(get_current_user)):
    """Run a live integration test for the named kind.
    Body: {"kind": "clickup" | "smtp" | "sms" | "anthropic"}
    """
    kind = (payload or {}).get("kind")
    cfg = await _fetch_user_cfg(user)
    try:
        if kind == "clickup":
            tok = cfg.get("clickup_token")
            if not tok:
                return {"ok": False, "error": "no token saved"}
            async with httpx.AsyncClient(timeout=15.0) as client:
                r = await client.get("https://api.clickup.com/api/v2/user", headers={"Authorization": tok})
                if r.status_code == 200:
                    return {"ok": True, "detail": r.json().get("user", {}).get("username", "ok")}
                return {"ok": False, "error": f"clickup {r.status_code}"}
        if kind == "anthropic":
            key = cfg.get("anthropic_api_key")
            if not key:
                return {"ok": False, "error": "no key saved"}
            async with httpx.AsyncClient(timeout=20.0) as client:
                r = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                    json={"model": cfg.get("anthropic_model") or "claude-sonnet-4-5", "max_tokens": 10, "messages": [{"role": "user", "content": "ping"}]},
                )
                return {"ok": r.status_code == 200, "error": None if r.status_code == 200 else f"anthropic {r.status_code}"}
        if kind == "smtp":
            if not cfg.get("smtp_host"):
                return {"ok": False, "error": "no smtp host"}
            # Light TCP-level check — don't send an actual email here.
            import socket
            try:
                with socket.create_connection((cfg["smtp_host"], int(cfg.get("smtp_port") or 587)), timeout=6) as s:
                    return {"ok": True, "detail": "smtp reachable"}
            except Exception as _e:
                return {"ok": False, "error": str(_e)[:120]}
        if kind == "sms":
            if not cfg.get("twilio_sid") or not cfg.get("twilio_token"):
                return {"ok": False, "error": "twilio creds missing"}
            async with httpx.AsyncClient(timeout=15.0, auth=(cfg["twilio_sid"], cfg["twilio_token"])) as client:
                r = await client.get(f"https://api.twilio.com/2010-04-01/Accounts/{cfg['twilio_sid']}.json")
                return {"ok": r.status_code == 200, "error": None if r.status_code == 200 else f"twilio {r.status_code}"}
        return {"ok": False, "error": f"unknown kind: {kind}"}
    except Exception as _e:
        return {"ok": False, "error": str(_e)[:200]}


@router.post("/admin/danger")
async def v2_admin_danger(payload: dict, user: UserContext = Depends(get_current_user)):
    """Danger-zone actions. Body: {"action": "revoke-clickup" | "clear-analytics"}"""
    action = (payload or {}).get("action")
    if action == "revoke-clickup":
        # Overwrite the ClickUp token with NULL.
        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                r = await client.patch(
                    f"{SUPABASE_URL}/rest/v1/user_configs",
                    params={"user_id": f"eq.{user.supabase_user_id}"},
                    headers={
                        "apikey": SUPABASE_ANON_KEY,
                        "Authorization": f"Bearer {user.supabase_jwt}",
                        "Content-Type": "application/json",
                        "Prefer": "return=minimal",
                    },
                    json={"clickup_token": None, "clickup_space_id": None},
                )
                return {"ok": r.status_code in (200, 204), "detail": "clickup revoked"}
        except Exception as _e:
            return {"ok": False, "error": str(_e)}
    if action == "clear-analytics":
        # No server-side cache to clear in this deploy — signal the client.
        return {"ok": True, "detail": "client should refresh"}
    return {"ok": False, "error": f"unknown action: {action}"}


@router.post("/admin/invite")
async def v2_admin_invite(payload: dict, user: UserContext = Depends(get_current_user)):
    """Log a team-invite request. Future: email the invite via Supabase auth."""
    email = (payload or {}).get("email")
    role = (payload or {}).get("role") or "Analyst"
    if not email or "@" not in email:
        raise HTTPException(400, "valid email required")
    # For now just echo — wiring to Supabase auth invite is a follow-up.
    return {"ok": True, "email": email, "role": role, "status": "queued"}
