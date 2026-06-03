from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx

from field_mapping import map_outcome_to_record, map_record_to_lead
from models import BulkResult, CallOutcome, Lead, LeadUpdate
from adapters.base import CRMAdapter


class ZohoAdapter(CRMAdapter):
    def __init__(self, config):
        super().__init__(config)
        self._token: str | None = None
        self._expiry = datetime.now(timezone.utc)

    async def _access_token(self) -> str:
        if self._token and datetime.now(timezone.utc) < self._expiry:
            return self._token
        credentials = self.config.credentials
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(
                "https://accounts.zoho.in/oauth/v2/token",
                params={
                    "refresh_token": credentials["refresh_token"],
                    "client_id": credentials["client_id"],
                    "client_secret": credentials["client_secret"],
                    "grant_type": "refresh_token",
                },
            )
            response.raise_for_status()
        self._token = response.json()["access_token"]
        self._expiry = datetime.now(timezone.utc) + timedelta(minutes=50)
        return self._token

    async def _request(self, method: str, path: str, **kwargs):
        token = await self._access_token()
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Zoho-oauthtoken {token}"
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.request(method, f"https://www.zohoapis.in/crm/v2/{path}", headers=headers, **kwargs)
            response.raise_for_status()
            return response

    async def fetch_dialable_leads(self, campaign_id: str, limit: int, filters=None) -> list[Lead]:
        response = await self._request("GET", "Leads", params={"per_page": limit})
        records = response.json().get("data", [])
        normalized = [{"id": item["id"], "name": f"{item.get('First_Name', '')} {item.get('Last_Name', '')}".strip(), "phone": item.get("Phone"), "status": item.get("Lead_Status")} for item in records]
        return [map_record_to_lead(record, self.config.field_map) for record in normalized if record.get("phone")]

    async def get_lead_by_phone(self, phone: str) -> Lead | None:
        response = await self._request("GET", "Leads/search", params={"phone": phone})
        records = response.json().get("data", [])
        if not records:
            return None
        record = records[0]
        normalized = {"id": record["id"], "name": f"{record.get('First_Name', '')} {record.get('Last_Name', '')}".strip(), "phone": record.get("Phone"), "status": record.get("Lead_Status")}
        return map_record_to_lead(normalized, self.config.field_map)

    async def update_lead(self, lead_id: str, outcome: CallOutcome) -> bool:
        payload = {"data": [{"id": lead_id, **map_outcome_to_record(outcome, self.config.status_map)}]}
        await self._request("PUT", "Leads", json=payload)
        return True

    async def bulk_update(self, updates: list[LeadUpdate]) -> BulkResult:
        payload = {"data": [{"id": update.lead_id, **map_outcome_to_record(update.outcome, self.config.status_map)} for update in updates]}
        await self._request("PUT", "Leads", json=payload)
        return BulkResult(success_count=len(updates), failure_count=0, failed_ids=[])

    async def test_connection(self) -> bool:
        try:
            await self._request("GET", "org")
            return True
        except Exception:
            return False
