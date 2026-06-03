from __future__ import annotations

from datetime import datetime, time

import httpx
from zoneinfo import ZoneInfo

from models import Campaign


class Dialer:
    def __init__(self, crm_adapter_url: str, orchestrator_url: str):
        self.crm_adapter_url = crm_adapter_url
        self.orchestrator_url = orchestrator_url

    def within_schedule(self, campaign: Campaign, now: datetime | None = None) -> bool:
        current = (now or datetime.utcnow()).astimezone(ZoneInfo(campaign.calling_schedule.timezone))
        if current.strftime("%A") not in campaign.calling_schedule.active_days:
            return False
        current_time = current.time()
        start = time.fromisoformat(campaign.calling_schedule.start_time)
        end = time.fromisoformat(campaign.calling_schedule.end_time)
        lunch_start = time.fromisoformat(campaign.calling_schedule.lunch_break_start)
        lunch_end = time.fromisoformat(campaign.calling_schedule.lunch_break_end)
        return start <= current_time <= end and not (lunch_start <= current_time <= lunch_end)

    async def fetch_leads(self, campaign: Campaign) -> list[dict]:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{self.crm_adapter_url}/api/leads/fetch-dialable",
                json={"campaign_id": campaign.id, "limit": campaign.max_concurrent, "filters": campaign.lead_filters},
            )
            response.raise_for_status()
            return response.json().get("leads", [])

    async def dial_lead(self, campaign: Campaign, lead: dict) -> dict:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{self.orchestrator_url}/call/dial",
                json={"phone": lead["phone"], "lead": lead, "campaign": campaign.model_dump(mode="json")},
            )
            response.raise_for_status()
            return response.json()

    async def execute_campaign(self, campaign: Campaign) -> list[dict]:
        if not self.within_schedule(campaign):
            return []
        leads = await self.fetch_leads(campaign)
        results = []
        for lead in leads:
            if lead.get("dnd", False):
                results.append({"lead_id": lead["id"], "status": "skipped_dnd"})
                continue
            results.append(await self.dial_lead(campaign, lead))
        return results
