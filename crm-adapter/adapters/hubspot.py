from __future__ import annotations

import httpx

from field_mapping import map_outcome_to_record, map_record_to_lead
from models import BulkResult, CallOutcome, Lead, LeadUpdate
from adapters.base import CRMAdapter


class HubSpotAdapter(CRMAdapter):
    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config.credentials['private_app_token']}",
            "Content-Type": "application/json",
        }

    async def fetch_dialable_leads(self, campaign_id: str, limit: int, filters=None) -> list[Lead]:
        body = {
            "filterGroups": [{"filters": [{"propertyName": "hs_lead_status", "operator": "EQ", "value": "NEW"}]}],
            "properties": ["firstname", "lastname", "phone", "hs_lead_status", "city", "budget_range", "project_interest"],
            "limit": limit,
        }
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post("https://api.hubapi.com/crm/v3/objects/contacts/search", json=body, headers=self._headers())
            response.raise_for_status()
        records = []
        for item in response.json().get("results", []):
            props = item.get("properties", {})
            records.append({
                "id": item["id"],
                "name": " ".join(filter(None, [props.get("firstname"), props.get("lastname")])),
                "phone": props.get("phone"),
                "status": props.get("hs_lead_status"),
                "location": props.get("city"),
                "budget": props.get("budget_range"),
                "project": props.get("project_interest"),
            })
        return [map_record_to_lead(record, self.config.field_map) for record in records if record.get("phone")]

    async def get_lead_by_phone(self, phone: str) -> Lead | None:
        leads = await self.fetch_dialable_leads("lookup", 100)
        for lead in leads:
            if lead.phone == phone:
                return lead
        return None

    async def update_lead(self, lead_id: str, outcome: CallOutcome) -> bool:
        payload = {"properties": map_outcome_to_record(outcome, self.config.status_map)}
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.patch(f"https://api.hubapi.com/crm/v3/objects/contacts/{lead_id}", json=payload, headers=self._headers())
            response.raise_for_status()
        return True

    async def bulk_update(self, updates: list[LeadUpdate]) -> BulkResult:
        inputs = [{"id": update.lead_id, "properties": map_outcome_to_record(update.outcome, self.config.status_map)} for update in updates]
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post("https://api.hubapi.com/crm/v3/objects/contacts/batch/update", json={"inputs": inputs}, headers=self._headers())
            response.raise_for_status()
        return BulkResult(success_count=len(updates), failure_count=0, failed_ids=[])

    async def test_connection(self) -> bool:
        async with httpx.AsyncClient(timeout=10) as client:
            response = await client.get("https://api.hubapi.com/integrations/v1/me", headers=self._headers())
            return response.is_success
