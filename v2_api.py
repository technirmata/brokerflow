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


@router.get("/deals")
async def v2_list_deals(user: UserContext = Depends(get_current_user)):
    if not user.list_deals:
        raise HTTPException(400, "ClickUp lists not set up. Run wizard.")
    task_to_deal, _, _ = _lazy_mappers()
    resp = await user_cu_get(
        user,
        f"/list/{user.list_deals}/task",
        params={"include_closed": "true", "subtasks": "true"},
    )
    return {"deals": [task_to_deal(t) for t in resp.get("tasks", [])]}


@router.get("/brokers")
async def v2_list_brokers(user: UserContext = Depends(get_current_user)):
    if not user.list_brokers:
        raise HTTPException(400, "ClickUp lists not set up. Run wizard.")
    _, task_to_broker, _ = _lazy_mappers()
    resp = await user_cu_get(
        user, f"/list/{user.list_brokers}/task", params={"include_closed": "true"}
    )
    return {"brokers": [task_to_broker(t) for t in resp.get("tasks", [])]}


@router.get("/followups")
async def v2_list_followups(user: UserContext = Depends(get_current_user)):
    if not user.list_followups:
        raise HTTPException(400, "ClickUp lists not set up. Run wizard.")
    _, _, task_to_followup = _lazy_mappers()
    resp = await user_cu_get(
        user, f"/list/{user.list_followups}/task", params={"include_closed": "true"}
    )
    return {"followups": [task_to_followup(t) for t in resp.get("tasks", [])]}


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
