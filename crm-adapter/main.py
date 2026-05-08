from __future__ import annotations

import json
import os
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from shared.logging import configure_logging
from shared.tracing import RequestTracingMiddleware, get_request_id

from adapters.custom_rest import CustomRestAdapter
from adapters.google_sheets import GoogleSheetsAdapter
from adapters.hubspot import HubSpotAdapter
from adapters.mock import MockAdapter
from adapters.salesforce import SalesforceAdapter
from adapters.webhook import WebhookAdapter
from adapters.zoho import ZohoAdapter
from models import BulkUpdateRequest, CRMConnectionConfig, CRMHealthResponse, FetchDialableRequest, UpdateLeadRequest


logger = configure_logging("crm-adapter")
app = FastAPI(title="CRM Adapter", version="1.0.0")
app.add_middleware(RequestTracingMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)

ADAPTERS = {
    "webhook": WebhookAdapter,
    "custom_rest": CustomRestAdapter,
    "salesforce": SalesforceAdapter,
    "hubspot": HubSpotAdapter,
    "zoho": ZohoAdapter,
    "google_sheets": GoogleSheetsAdapter,
    "mock": MockAdapter,
}


def _load_default_config() -> CRMConnectionConfig:
    config_path = Path(os.getenv("CRM_ADAPTER_CONFIG_FILE", "/data/crm-config.json"))
    if config_path.exists():
        return CRMConnectionConfig(**json.loads(config_path.read_text(encoding="utf-8")))
    if os.getenv("CRM_TYPE", "custom_rest") == "mock":
        mock_path = Path(os.getenv("CRM_MOCK_LEADS_FILE", "mock-leads.json"))
        leads = []
        if mock_path.exists():
            leads = json.loads(mock_path.read_text(encoding="utf-8"))
        return CRMConnectionConfig(crm_type="mock", mock_leads=leads)
    base_url = os.getenv("CRM_BASE_URL")
    if not base_url:
        raise HTTPException(status_code=500, detail="CRM configuration not provided")
    return CRMConnectionConfig(
        crm_type=os.getenv("CRM_TYPE", "custom_rest"),
        base_url=base_url,
        auth_type=os.getenv("CRM_AUTH_TYPE", "none"),
        headers={},
        credentials={},
    )


def _adapter(config: CRMConnectionConfig | None):
    active = config or _load_default_config()
    if active.crm_type not in ADAPTERS:
        raise HTTPException(status_code=400, detail=f"Unsupported CRM type: {active.crm_type}")
    return ADAPTERS[active.crm_type](active), active


@app.post("/api/leads/fetch-dialable")
async def fetch_dialable(request: FetchDialableRequest, x_tenant_id: str | None = Header(default=None)):
    adapter, active = _adapter(request.crm_config)
    leads = await adapter.fetch_dialable_leads(request.campaign_id, request.limit, request.filters)
    logger.info(
        "Fetched dialable leads",
        extra={"request_id": get_request_id(), "tenant_id": x_tenant_id, "extra_fields": {"crm_type": active.crm_type, "count": len(leads)}},
    )
    return {"leads": [lead.model_dump(mode="json") for lead in leads], "crm_type": active.crm_type}


@app.get("/api/leads/by-phone/{phone}")
async def lead_by_phone(phone: str):
    adapter, _ = _adapter(None)
    lead = await adapter.get_lead_by_phone(phone)
    if not lead:
        raise HTTPException(status_code=404, detail="Lead not found")
    return lead.model_dump(mode="json")


@app.patch("/api/leads/{lead_id}/update")
async def update_lead(lead_id: str, request: UpdateLeadRequest):
    adapter, _ = _adapter(request.crm_config)
    updated = await adapter.update_lead(lead_id, request.outcome)
    return {"updated": updated, "lead_id": lead_id}


@app.post("/api/leads/bulk-update")
async def bulk_update(request: BulkUpdateRequest):
    adapter, _ = _adapter(request.crm_config)
    result = await adapter.bulk_update(request.updates)
    return result.model_dump()


@app.get("/api/crm/health", response_model=CRMHealthResponse)
async def health():
    try:
        adapter, active = _adapter(None)
        connected = await adapter.test_connection()
        return CRMHealthResponse(connected=connected, crm_type=active.crm_type, detail="ok" if connected else "connection failed")
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/health")
async def service_health():
    status = await health()
    return {"status": "ok" if status.connected else "degraded", **status.model_dump()}


@app.post("/api/crm/test-connection")
async def test_connection(config: CRMConnectionConfig):
    try:
        adapter, active = _adapter(config)
        connected = await adapter.test_connection()
        sample_leads = []
        detail = "Connection verified." if connected else "Connection failed."
        if connected:
            try:
                preview = await adapter.fetch_dialable_leads("preview", 5, {})
                sample_leads = [lead.model_dump(mode="json") for lead in preview[:5]]
                if sample_leads:
                    detail = f"Connected. Found {len(sample_leads)} sample lead(s)."
                else:
                    detail = "Connected, but no dialable leads were returned."
            except Exception as exc:
                logger.warning(
                    "CRM preview fetch failed",
                    extra={"request_id": get_request_id(), "extra_fields": {"crm_type": active.crm_type, "error": str(exc)}},
                )
                detail = "Connected, but sample lead preview could not be loaded."
        return {
            "connected": connected,
            "crm_type": active.crm_type,
            "detail": detail,
            "sample_leads": sample_leads,
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
