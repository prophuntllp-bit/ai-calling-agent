from __future__ import annotations

import os
import uuid
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import Session, SQLModel, create_engine, select

from shared.logging import configure_logging
from shared.tracing import RequestTracingMiddleware

from analytics import summarize_calls
from dialer import Dialer
from models import CallLogRecord, Campaign, CampaignRecord


logger = configure_logging("campaign-service")
app = FastAPI(title="Campaign Service", version="1.0.0")
app.add_middleware(RequestTracingMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("CAMPAIGN_DATABASE_URL", os.getenv("DATABASE_URL", "sqlite:///./campaigns.db"))
engine = create_engine(DATABASE_URL, echo=False)
dialer = Dialer(
    crm_adapter_url=os.getenv("CRM_ADAPTER_URL", "http://crm-adapter:8010"),
    orchestrator_url=os.getenv("ORCHESTRATOR_URL", "http://orchestrator:8000"),
)


def _to_domain(record: CampaignRecord) -> Campaign:
    return Campaign(**record.model_dump())


@app.on_event("startup")
def startup():
    SQLModel.metadata.create_all(engine)


@app.post("/campaigns")
async def create_campaign(campaign: Campaign):
    record = CampaignRecord(**campaign.model_dump())
    with Session(engine) as session:
        session.add(record)
        session.commit()
    return {"status": "created", "id": campaign.id}


@app.get("/campaigns")
async def list_campaigns():
    with Session(engine) as session:
        campaigns = session.exec(select(CampaignRecord).order_by(CampaignRecord.created_at.desc())).all()
    return {"campaigns": [campaign.model_dump(mode="json") for campaign in campaigns]}


@app.get("/campaigns/{campaign_id}")
async def get_campaign(campaign_id: str):
    with Session(engine) as session:
        campaign = session.get(CampaignRecord, campaign_id)
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")
        call_logs = session.exec(select(CallLogRecord).where(CallLogRecord.campaign_id == campaign_id)).all()
    return {"campaign": campaign.model_dump(), "metrics": summarize_calls(call_logs)}


@app.post("/campaigns/{campaign_id}/start")
async def start_campaign(campaign_id: str):
    with Session(engine) as session:
        record = session.get(CampaignRecord, campaign_id)
        if not record:
            raise HTTPException(status_code=404, detail="Campaign not found")
        campaign = _to_domain(record)
        results = await dialer.execute_campaign(campaign)
        record.status = "active"
        record.updated_at = datetime.utcnow()
        for result in results:
            session.add(
                CallLogRecord(
                    id=str(uuid.uuid4()),
                    campaign_id=campaign_id,
                    lead_id=result.get("lead_id", "unknown"),
                    phone=result.get("phone", "unknown"),
                    status=result.get("status", "queued"),
                    duration_sec=result.get("duration_sec", 0),
                    call_metadata=result,
                )
            )
        session.add(record)
        session.commit()
    return {"status": "started", "results": results}


@app.post("/campaigns/{campaign_id}/pause")
async def pause_campaign(campaign_id: str):
    with Session(engine) as session:
        campaign = session.get(CampaignRecord, campaign_id)
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")
        campaign.status = "paused"
        campaign.updated_at = datetime.utcnow()
        session.add(campaign)
        session.commit()
    return {"status": "paused", "id": campaign_id}


@app.post("/campaigns/{campaign_id}/stop")
async def stop_campaign(campaign_id: str):
    with Session(engine) as session:
        campaign = session.get(CampaignRecord, campaign_id)
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")
        campaign.status = "completed"
        campaign.updated_at = datetime.utcnow()
        session.add(campaign)
        session.commit()
    return {"status": "stopped", "id": campaign_id}


@app.get("/campaigns/{campaign_id}/results")
async def campaign_results(campaign_id: str):
    with Session(engine) as session:
        call_logs = session.exec(select(CallLogRecord).where(CallLogRecord.campaign_id == campaign_id)).all()
    return {"campaign_id": campaign_id, "results": [call.model_dump(mode="json") for call in call_logs]}


@app.post("/campaigns/{campaign_id}/schedule")
async def update_schedule(campaign_id: str, schedule: dict):
    with Session(engine) as session:
        campaign = session.get(CampaignRecord, campaign_id)
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")
        campaign.calling_schedule = schedule
        campaign.updated_at = datetime.utcnow()
        session.add(campaign)
        session.commit()
    return {"status": "updated", "id": campaign_id}


@app.get("/health")
async def health():
    with Session(engine) as session:
        session.exec(select(CampaignRecord).limit(1)).all()
    return {"status": "ok", "database": "reachable"}
