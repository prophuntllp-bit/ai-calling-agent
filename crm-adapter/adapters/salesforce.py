from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import jwt

from field_mapping import map_outcome_to_record, map_record_to_lead
from models import BulkResult, CallOutcome, Lead, LeadUpdate
from adapters.base import CRMAdapter


class SalesforceAdapter(CRMAdapter):
    def __init__(self, config):
        super().__init__(config)
        self._token: str | None = None
        self._instance_url: str | None = None
        self._expiry = datetime.now(timezone.utc)

    async def _ensure_token(self) -> tuple[str, str]:
        if self._token and datetime.now(timezone.utc) < self._expiry:
            return self._token, self._instance_url or self.config.base_url or ""
        credentials = self.config.credentials
        assertion = jwt.encode(
            {
                "iss": credentials["client_id"],
                "sub": credentials["username"],
                "aud": credentials.get("login_url", "https://login.salesforce.com"),
                "exp": int((datetime.now(timezone.utc) + timedelta(minutes=3)).timestamp()),
            },
            credentials["private_key"],
            algorithm="RS256",
        )
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(
                f"{credentials.get('login_url', 'https://login.salesforce.com')}/services/oauth2/token",
                data={"grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer", "assertion": assertion},
            )
            response.raise_for_status()
        data = response.json()
        self._token = data["access_token"]
        self._instance_url = data["instance_url"]
        self._expiry = datetime.now(timezone.utc) + timedelta(minutes=50)
        return self._token, self._instance_url

    async def _request(self, method: str, path: str, **kwargs):
        token, instance_url = await self._ensure_token()
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"
        headers["Content-Type"] = "application/json"
        async with httpx.AsyncClient(timeout=25) as client:
            response = await client.request(method, f"{instance_url}{path}", headers=headers, **kwargs)
            response.raise_for_status()
            return response

    async def fetch_dialable_leads(self, campaign_id: str, limit: int, filters: dict[str, Any] | None = None) -> list[Lead]:
        object_name = self.config.query.get("object", "Lead")
        where = filters.get("where") if filters else "Status = 'Open - Not Contacted'"
        query = f"SELECT Id, Name, Phone, Company, Status FROM {object_name} WHERE {where} LIMIT {limit}"
        response = await self._request("GET", "/services/data/v59.0/query", params={"q": query})
        return [map_record_to_lead(record, self.config.field_map | {"id": "Id", "phone": "Phone", "name": "Name"}) for record in response.json().get("records", [])]

    async def get_lead_by_phone(self, phone: str) -> Lead | None:
        object_name = self.config.query.get("object", "Lead")
        query = f"SELECT Id, Name, Phone, Company, Status FROM {object_name} WHERE Phone = '{phone}' LIMIT 1"
        response = await self._request("GET", "/services/data/v59.0/query", params={"q": query})
        records = response.json().get("records", [])
        return map_record_to_lead(records[0], self.config.field_map | {"id": "Id", "phone": "Phone", "name": "Name"}) if records else None

    async def update_lead(self, lead_id: str, outcome: CallOutcome) -> bool:
        payload = map_outcome_to_record(outcome, self.config.status_map)
        await self._request("PATCH", f"/services/data/v59.0/sobjects/Lead/{lead_id}", json=payload)
        return True

    async def bulk_update(self, updates: list[LeadUpdate]) -> BulkResult:
        records = [{"attributes": {"type": "Lead"}, "Id": update.lead_id, **map_outcome_to_record(update.outcome, self.config.status_map)} for update in updates]
        response = await self._request("PATCH", "/services/data/v59.0/composite/sobjects", json={"allOrNone": False, "records": records})
        failures = [item.get("id", "unknown") for item in response.json() if not item.get("success")]
        return BulkResult(success_count=len(updates) - len(failures), failure_count=len(failures), failed_ids=failures)

    async def test_connection(self) -> bool:
        try:
            await self._request("GET", "/services/data/v59.0/")
            return True
        except Exception:
            return False
