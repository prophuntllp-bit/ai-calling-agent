from __future__ import annotations

from typing import Any

import httpx

from field_mapping import map_outcome_to_record, map_record_to_lead
from models import BulkResult, CallOutcome, Lead, LeadUpdate
from adapters.base import CRMAdapter


class WebhookAdapter(CRMAdapter):
    async def fetch_dialable_leads(self, campaign_id: str, limit: int, filters: dict[str, Any] | None = None) -> list[Lead]:
        endpoint = self.config.endpoints.get("fetch_dialable", "/api/leads/fetch-dialable")
        payload = {"campaign_id": campaign_id, "limit": limit, "filters": filters or {}}
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(f"{self.config.base_url}{endpoint}", json=payload, headers=self.config.headers)
            response.raise_for_status()
        records = response.json().get("leads", response.json())
        return [map_record_to_lead(record, self.config.field_map) for record in records]

    async def get_lead_by_phone(self, phone: str) -> Lead | None:
        endpoint = self.config.endpoints.get("by_phone", f"/api/leads/by-phone/{phone}")
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(f"{self.config.base_url}{endpoint}", headers=self.config.headers)
            if response.status_code == 404:
                return None
            response.raise_for_status()
        return map_record_to_lead(response.json(), self.config.field_map)

    async def update_lead(self, lead_id: str, outcome: CallOutcome) -> bool:
        endpoint = self.config.endpoints.get("update", f"/api/leads/{lead_id}/update")
        payload = map_outcome_to_record(outcome, self.config.status_map)
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.patch(f"{self.config.base_url}{endpoint}", json=payload, headers=self.config.headers)
            response.raise_for_status()
        return True

    async def bulk_update(self, updates: list[LeadUpdate]) -> BulkResult:
        endpoint = self.config.endpoints.get("bulk_update", "/api/leads/bulk-update")
        payload = {
            "updates": [
                {"lead_id": update.lead_id, "outcome": map_outcome_to_record(update.outcome, self.config.status_map)}
                for update in updates
            ]
        }
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(f"{self.config.base_url}{endpoint}", json=payload, headers=self.config.headers)
            response.raise_for_status()
        data = response.json()
        return BulkResult(**data)

    async def test_connection(self) -> bool:
        endpoint = self.config.endpoints.get("health", "/health")
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get(f"{self.config.base_url}{endpoint}", headers=self.config.headers)
            return response.is_success
