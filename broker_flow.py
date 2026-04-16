"""
BrokerFlow backend — single-file FastAPI server.

Run:
    pip install fastapi uvicorn httpx python-multipart pdfplumber python-dotenv
    export CLICKUP_TOKEN=pk_xxx   (or put in .env)
    python broker_flow.py

Opens dashboard on http://localhost:8787
"""

from __future__ import annotations

import json
import os
import re
import sys
import webbrowser
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse, Response

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ---------- Config ----------
HERE = Path(__file__).resolve().parent
CLICKUP_TOKEN = os.environ.get("CLICKUP_TOKEN", "").strip()
CLICKUP_API = "https://api.clickup.com/api/v2"

# JPIG workspace IDs (discovered 2026-04-16)
WS_ID = "90141092332"
SPACE_ID = "90144894869"          # All Deals and Stages
FOLDER_ID = "90148667708"         # JPIG — Broker Deal Pipeline
LIST_ACTIVE_DEALS = "901415550982"
LIST_BROKERS = "901415550973"
LIST_FOLLOWUPS = "901415550975"
LIST_TEMPLATES = "901415550978"

DATA_MARKER_START = "<!-- BROKERFLOW-DATA-START -->"
DATA_MARKER_END = "<!-- BROKERFLOW-DATA-END -->"

# ---------- ClickUp client ----------

def cu_headers() -> dict[str, str]:
    if not CLICKUP_TOKEN:
        raise HTTPException(500, "CLICKUP_TOKEN not set. Put it in .env or env var.")
    return {"Authorization": CLICKUP_TOKEN, "Content-Type": "application/json"}

async def cu_get(path: str, params: dict | None = None) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(f"{CLICKUP_API}{path}", headers=cu_headers(), params=params or {})
        r.raise_for_status()
        return r.json()

async def cu_post(path: str, body: dict) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.post(f"{CLICKUP_API}{path}", headers=cu_headers(), json=body)
        r.raise_for_status()
        return r.json()

async def cu_put(path: str, body: dict) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.put(f"{CLICKUP_API}{path}", headers=cu_headers(), json=body)
        r.raise_for_status()
        return r.json()

async def cu_delete(path: str) -> None:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.delete(f"{CLICKUP_API}{path}", headers=cu_headers())
        r.raise_for_status()

# ---------- Structured deal data in description ----------
# ClickUp REST API can't create custom fields — only the UI can.
# Workaround: pack structured fields into the task description inside
# JSON markers that we parse and write. Humans see normal markdown; we
# see structured data.

def pack_data(description: str, data: dict) -> str:
    prose = strip_data(description or "")
    block = f"\n\n{DATA_MARKER_START}\n```json\n{json.dumps(data, indent=2)}\n```\n{DATA_MARKER_END}\n"
    return (prose.rstrip() + block).strip()

def strip_data(description: str) -> str:
    if not description:
        return ""
    pattern = re.compile(
        re.escape(DATA_MARKER_START) + r".*?" + re.escape(DATA_MARKER_END),
        re.DOTALL,
    )
    return pattern.sub("", description).strip()

def extract_data(description: str) -> dict:
    if not description:
        return {}
    m = re.search(
        re.escape(DATA_MARKER_START) + r".*?```json\s*(.*?)\s*```.*?" + re.escape(DATA_MARKER_END),
        description,
        re.DOTALL,
    )
    if not m:
        return {}
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError:
        return {}

# ---------- Domain mapping ----------

def task_to_deal(task: dict) -> dict:
    data = extract_data(task.get("description", "") or task.get("text_content", ""))
    tags = [t.get("name", "") for t in task.get("tags", [])]
    return {
        "id": task["id"],
        "name": task.get("name", ""),
        "url": task.get("url", ""),
        "status": (task.get("status") or {}).get("status", ""),
        "priority": ((task.get("priority") or {}) or {}).get("priority", "") if task.get("priority") else "",
        "tags": tags,
        "due_date": task.get("due_date"),
        "date_created": task.get("date_created"),
        "date_updated": task.get("date_updated"),
        "description_prose": strip_data(task.get("description", "") or ""),
        # structured fields
        "deal_id": data.get("deal_id", ""),
        "asset_class": data.get("asset_class", ""),
        "city": data.get("city", ""),
        "state": data.get("state", ""),
        "units": data.get("units"),
        "ask_price": data.get("ask_price"),
        "noi": data.get("noi"),
        "cap_rate": data.get("cap_rate"),
        "broker_id": data.get("broker_id", ""),
        "doc_status": data.get("doc_status", "Requested"),
        "docs_received": data.get("docs_received", []),
        "docs_outstanding": data.get("docs_outstanding", []),
        "next_action_date": data.get("next_action_date", ""),
        "next_action": data.get("next_action", ""),
        "source": data.get("source", "Broker"),
        "stage_entered": data.get("stage_entered", ""),
    }

def task_to_broker(task: dict) -> dict:
    data = extract_data(task.get("description", "") or task.get("text_content", ""))
    return {
        "id": task["id"],
        "name": task.get("name", ""),
        "url": task.get("url", ""),
        "firm": data.get("firm", ""),
        "region": data.get("region", ""),
        "email": data.get("email", ""),
        "phone": data.get("phone", ""),
        "last_contact": data.get("last_contact", ""),
        "relationship_strength": data.get("relationship_strength", "Cold"),
        "preferred_assets": data.get("preferred_assets", []),
        "deal_count": data.get("deal_count", 0),
        "notes": strip_data(task.get("description", "") or ""),
    }

def task_to_followup(task: dict) -> dict:
    data = extract_data(task.get("description", "") or task.get("text_content", ""))
    return {
        "id": task["id"],
        "name": task.get("name", ""),
        "url": task.get("url", ""),
        "status": (task.get("status") or {}).get("status", ""),
        "due_date": task.get("due_date"),
        "linked_deal_id": data.get("linked_deal_id", ""),
        "linked_broker_id": data.get("linked_broker_id", ""),
        "cadence_day": data.get("cadence_day", ""),
        "touchpoint_type": data.get("touchpoint_type", "Email"),
        "draft_body": data.get("draft_body", ""),
        "state": data.get("state", "Queued"),
    }

# ---------- FastAPI app ----------

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("BrokerFlow backend up on http://localhost:8787")
    if not CLICKUP_TOKEN:
        print("WARNING: CLICKUP_TOKEN not set — ClickUp calls will fail.")
    yield

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Meta / health ----------

@app.get("/api/health")
async def health():
    return {
        "ok": True,
        "clickup_token_present": bool(CLICKUP_TOKEN),
        "workspace_id": WS_ID,
    }

@app.get("/api/meta")
async def meta():
    return {
        "workspace_id": WS_ID,
        "space_id": SPACE_ID,
        "folder_id": FOLDER_ID,
        "lists": {
            "active_deals": LIST_ACTIVE_DEALS,
            "brokers": LIST_BROKERS,
            "followups": LIST_FOLLOWUPS,
            "templates": LIST_TEMPLATES,
        },
        "pipeline_stages": [
            "incoming leads",
            "docs requested",
            "docs complete",
            "underwriting",
            "loi",
            "under contract",
            "closed",
            "dead",
        ],
        "asset_classes": ["MF", "Hotel", "RV", "MHP", "Self-Storage", "Industrial"],
        "regions": ["Southeast", "Southwest", "Midwest", "Northeast", "West", "Mid-Atlantic"],
    }

# ---------- Deals ----------

@app.get("/api/deals")
async def list_deals():
    resp = await cu_get(f"/list/{LIST_ACTIVE_DEALS}/task", params={"include_closed": "true", "subtasks": "true"})
    return {"deals": [task_to_deal(t) for t in resp.get("tasks", [])]}

@app.get("/api/deals/{task_id}")
async def get_deal(task_id: str):
    task = await cu_get(f"/task/{task_id}")
    return task_to_deal(task)

@app.post("/api/deals")
async def create_deal(payload: dict):
    name = payload.get("name") or f"{payload.get('deal_id','JPIG-???')} · {payload.get('city','')}, {payload.get('state','')}"
    data = {k: v for k, v in payload.items() if k not in ("name", "status", "priority", "tags")}
    body = {
        "name": name,
        "status": payload.get("status", "incoming leads"),
        "priority": {"urgent": 1, "high": 2, "normal": 3, "low": 4}.get(payload.get("priority", "normal"), 3),
        "description": pack_data("", data),
        "tags": payload.get("tags", []),
    }
    task = await cu_post(f"/list/{LIST_ACTIVE_DEALS}/task", body)
    return task_to_deal(task)

@app.put("/api/deals/{task_id}")
async def update_deal(task_id: str, payload: dict):
    # Fetch existing to preserve prose + merge data
    existing = await cu_get(f"/task/{task_id}")
    current_data = extract_data(existing.get("description", "") or "")
    new_data = {**current_data, **{k: v for k, v in payload.items() if k not in ("name", "status", "priority", "tags", "description_prose")}}
    prose = payload.get("description_prose", strip_data(existing.get("description", "") or ""))
    body: dict[str, Any] = {
        "description": pack_data(prose, new_data),
    }
    if "name" in payload:
        body["name"] = payload["name"]
    if "status" in payload:
        body["status"] = payload["status"]
    if "priority" in payload:
        body["priority"] = {"urgent": 1, "high": 2, "normal": 3, "low": 4}.get(payload["priority"], 3)
    task = await cu_put(f"/task/{task_id}", body)
    return task_to_deal(task)

@app.delete("/api/deals/{task_id}")
async def delete_deal(task_id: str):
    await cu_delete(f"/task/{task_id}")
    return {"ok": True}

# ---------- Brokers ----------

@app.get("/api/brokers")
async def list_brokers():
    resp = await cu_get(f"/list/{LIST_BROKERS}/task", params={"include_closed": "true"})
    return {"brokers": [task_to_broker(t) for t in resp.get("tasks", [])]}

@app.post("/api/brokers")
async def create_broker(payload: dict):
    name = payload.get("name", payload.get("firm", "Unnamed broker"))
    data = {k: v for k, v in payload.items() if k != "name"}
    body = {
        "name": name,
        "description": pack_data("", data),
    }
    task = await cu_post(f"/list/{LIST_BROKERS}/task", body)
    return task_to_broker(task)

@app.put("/api/brokers/{task_id}")
async def update_broker(task_id: str, payload: dict):
    existing = await cu_get(f"/task/{task_id}")
    current_data = extract_data(existing.get("description", "") or "")
    new_data = {**current_data, **{k: v for k, v in payload.items() if k != "name"}}
    body: dict[str, Any] = {"description": pack_data(strip_data(existing.get("description", "") or ""), new_data)}
    if "name" in payload:
        body["name"] = payload["name"]
    task = await cu_put(f"/task/{task_id}", body)
    return task_to_broker(task)

# ---------- Follow-ups (doc chase cadence) ----------

@app.get("/api/followups")
async def list_followups():
    resp = await cu_get(f"/list/{LIST_FOLLOWUPS}/task", params={"include_closed": "true"})
    return {"followups": [task_to_followup(t) for t in resp.get("tasks", [])]}

@app.post("/api/followups")
async def create_followup(payload: dict):
    body = {
        "name": payload.get("name", "Follow-up"),
        "description": pack_data("", payload),
        "due_date": payload.get("due_date_ms"),
    }
    task = await cu_post(f"/list/{LIST_FOLLOWUPS}/task", body)
    return task_to_followup(task)

# ---------- Outreach (mailto compose) ----------

@app.post("/api/outreach/draft")
async def outreach_draft(payload: dict):
    """
    Return mailto: + Gmail compose + Outlook compose URLs for a templated outreach.
    payload: { to, subject, body, cc?, bcc? }
    """
    to = payload.get("to", "")
    subject = payload.get("subject", "")
    body = payload.get("body", "")
    cc = payload.get("cc", "")
    bcc = payload.get("bcc", "")

    q = lambda s: quote(s, safe="")
    mailto = f"mailto:{to}?subject={q(subject)}&body={q(body)}"
    if cc:
        mailto += f"&cc={q(cc)}"
    if bcc:
        mailto += f"&bcc={q(bcc)}"

    gmail = (
        f"https://mail.google.com/mail/?view=cm&fs=1"
        f"&to={q(to)}&su={q(subject)}&body={q(body)}"
    )
    if cc: gmail += f"&cc={q(cc)}"
    if bcc: gmail += f"&bcc={q(bcc)}"

    outlook = (
        f"https://outlook.office.com/mail/deeplink/compose"
        f"?to={q(to)}&subject={q(subject)}&body={q(body)}"
    )
    if cc: outlook += f"&cc={q(cc)}"

    return {"mailto": mailto, "gmail": gmail, "outlook": outlook}

# ---------- Doc intake (PDF parse) ----------

@app.post("/api/docs/parse")
async def docs_parse(file: UploadFile = File(...)):
    """
    Upload a P&L / T-12 / OM PDF. Extract text, try to detect:
    NOI, Gross Income, Total Expenses, Units, Asking Price, Cap Rate.
    """
    try:
        import pdfplumber
    except ImportError:
        raise HTTPException(500, "pdfplumber not installed. pip install pdfplumber")

    raw = await file.read()
    tmp = HERE / "_tmp_upload.pdf"
    tmp.write_bytes(raw)

    text = ""
    try:
        with pdfplumber.open(tmp) as pdf:
            for page in pdf.pages:
                text += (page.extract_text() or "") + "\n"
    finally:
        try: tmp.unlink()
        except Exception: pass

    return {
        "filename": file.filename,
        "char_count": len(text),
        "extracted": heuristic_extract(text),
        "raw_text_preview": text[:3000],
    }

def heuristic_extract(text: str) -> dict:
    """Best-effort regex extraction of common T-12 / OM fields."""
    def find_money(patterns: list[str]) -> float | None:
        for p in patterns:
            m = re.search(p, text, re.IGNORECASE)
            if m:
                raw = m.group(1).replace(",", "").replace("$", "").strip()
                try: return float(raw)
                except ValueError: continue
        return None

    def find_int(patterns: list[str]) -> int | None:
        for p in patterns:
            m = re.search(p, text, re.IGNORECASE)
            if m:
                raw = m.group(1).replace(",", "").strip()
                try: return int(raw)
                except ValueError: continue
        return None

    def find_pct(patterns: list[str]) -> float | None:
        for p in patterns:
            m = re.search(p, text, re.IGNORECASE)
            if m:
                try: return float(m.group(1))
                except ValueError: continue
        return None

    return {
        "noi": find_money([
            r"net operating income[^\$]*\$?([\d,]+\.?\d*)",
            r"\bNOI\b[^\$]*\$?([\d,]+\.?\d*)",
        ]),
        "gross_income": find_money([
            r"gross (?:potential |scheduled |rental )?income[^\$]*\$?([\d,]+\.?\d*)",
            r"effective gross income[^\$]*\$?([\d,]+\.?\d*)",
        ]),
        "total_expenses": find_money([
            r"total (?:operating )?expenses[^\$]*\$?([\d,]+\.?\d*)",
        ]),
        "units": find_int([
            r"(\d+)\s*(?:units|keys|pads|sites)\b",
            r"\b(?:units|keys|pads|sites)\s*[:\-]?\s*(\d+)",
        ]),
        "ask_price": find_money([
            r"(?:asking price|list price|price)\s*[:\-]?\s*\$?([\d,]+\.?\d*)",
        ]),
        "cap_rate": find_pct([
            r"cap rate\s*[:\-]?\s*(\d+\.?\d*)\s*%",
            r"(\d+\.?\d*)\s*%\s*cap\b",
        ]),
    }

# ---------- Seed ----------

@app.post("/api/seed/sample-brokers")
async def seed_sample_brokers():
    """One-time: seed 5 sample brokers in the Broker Directory list."""
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
    created = []
    for s in samples:
        data = {k: v for k, v in s.items() if k != "name"}
        body = {"name": s["name"], "description": pack_data("", data)}
        task = await cu_post(f"/list/{LIST_BROKERS}/task", body)
        created.append(task_to_broker(task))
    return {"created": created}

# ---------- Static dashboard ----------

@app.get("/", response_class=HTMLResponse)
async def root():
    html = HERE / "BrokerFlow_Master_Dashboard_v3.html"
    if not html.exists():
        return JSONResponse({"error": "dashboard HTML not found", "path": str(html)}, status_code=404)
    # Read as bytes and return with no preset Content-Length — Starlette computes from actual payload.
    # Avoids h11 "Too much data for declared Content-Length" seen with FileResponse on some setups.
    data = html.read_bytes()
    return Response(content=data, media_type="text/html; charset=utf-8")

@app.get("/favicon.ico")
async def favicon():
    return JSONResponse({}, status_code=204)

# ---------- Entrypoint ----------

if __name__ == "__main__":
    import uvicorn
    # Respect PORT env (Render/Railway/Fly/Heroku convention); default 8787 for local.
    port = int(os.environ.get("PORT", "8787"))
    # Bind 0.0.0.0 when hosted (PORT set by platform); 127.0.0.1 when local.
    host = "0.0.0.0" if os.environ.get("PORT") else "127.0.0.1"
    if host == "127.0.0.1" and "--no-browser" not in sys.argv and not os.environ.get("BROKERFLOW_NO_BROWSER"):
        try:
            webbrowser.open(f"http://{host}:{port}")
        except Exception:
            pass
    uvicorn.run(app, host=host, port=port, log_level="info")
