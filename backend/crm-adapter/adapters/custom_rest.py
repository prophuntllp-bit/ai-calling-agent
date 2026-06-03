from __future__ import annotations

import base64
from typing import Any

import httpx

from field_mapping import map_outcome_to_record, map_record_to_lead
from models import BulkResult, CallOutcome, Lead, LeadUpdate
from adapters.base import CRMAdapter


class CustomRestAdapter(CRMAdapter):
    def _endpoint_candidates(self, configured: str | None, *fallbacks: str) -> list[str]:
        candidates = []
        if configured:
            candidates.append(configured)
        for fallback in fallbacks:
            if fallback and fallback not in candidates:
                candidates.append(fallback)
        return candidates

    async def _request_first_ok(self, method: str, endpoints: list[str], **kwargs):
        last_exc = None
        async with httpx.AsyncClient(timeout=kwargs.pop("timeout", 20), follow_redirects=True) as client:
            for endpoint in endpoints:
                try:
                    response = await client.request(method, f"{self.config.base_url}{endpoint}", **kwargs)
                    response.raise_for_status()
                    return response
                except Exception as exc:
                    last_exc = exc
        if last_exc:
            raise last_exc
        raise RuntimeError("No endpoint candidates provided")

    def _records_from_response(self, data):
        if isinstance(data, list):
            return data
        if not isinstance(data, dict):
            return []
        return data.get("items") or data.get("leads") or data.get("data") or data.get("item") or []

    def _headers(self) -> dict[str, str]:
        headers = dict(self.config.headers)
        auth_type = self.config.auth_type.lower()
        credentials = self.config.credentials
        if auth_type == "bearer" and credentials.get("token"):
            headers["Authorization"] = f"Bearer {credentials['token']}"
        elif auth_type in {"api-key", "api_key"} and credentials.get("api_key"):
            headers[credentials.get("api_key_header", "X-API-Key")] = credentials["api_key"]
        elif auth_type == "basic":
            token = base64.b64encode(f"{credentials.get('username', '')}:{credentials.get('password', '')}".encode()).decode()
            headers["Authorization"] = f"Basic {token}"
        return headers

    async def fetch_dialable_leads(self, campaign_id: str, limit: int, filters: dict[str, Any] | None = None) -> list[Lead]:
        endpoint = self.config.endpoints.get("fetch_dialable")
        params = {"campaign_id": campaign_id, "limit": limit, **(filters or {})}
        response = await self._request_first_ok(
            "GET",
            self._endpoint_candidates(endpoint, "/api/voice/leads", "/api/leads"),
            params=params,
            headers=self._headers(),
        )
        records = self._records_from_response(response.json())
        return [map_record_to_lead(record, self.config.field_map) for record in records]

    async def get_lead_by_phone(self, phone: str) -> Lead | None:
        endpoint = self.config.endpoints.get("by_phone")
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            response = None
            last_exc = None
            for candidate in self._endpoint_candidates(endpoint, "/api/voice/leads/search", "/api/leads/search"):
                try:
                    response = await client.get(f"{self.config.base_url}{candidate}", params={"phone": phone}, headers=self._headers())
                    if response.status_code == 404:
                        continue
                    response.raise_for_status()
                    break
                except Exception as exc:
                    last_exc = exc
            else:
                if last_exc:
                    raise last_exc
                return None
        data = response.json()
        records = self._records_from_response(data)
        record = records[0] if isinstance(records, list) and records else (data.get("item") if isinstance(data, dict) else None)
        return map_record_to_lead(record, self.config.field_map) if record else None

    async def update_lead(self, lead_id: str, outcome: CallOutcome) -> bool:
        endpoint = self.config.endpoints.get("update")
        payload = map_outcome_to_record(outcome, self.config.status_map)
        response = await self._request_first_ok(
            "PATCH",
            self._endpoint_candidates(endpoint or f"/api/voice/leads/{lead_id}", f"/api/voice/leads/{lead_id}", f"/api/leads/{lead_id}"),
            json=payload,
            headers=self._headers(),
        )
        response.raise_for_status()
        return True

    async def bulk_update(self, updates: list[LeadUpdate]) -> BulkResult:
        endpoint = self.config.endpoints.get("bulk_update", "/api/leads/bulk")
        failures: list[str] = []
        async with httpx.AsyncClient(timeout=30) as client:
            for update in updates:
                response = await client.patch(
                    f"{self.config.base_url}{endpoint}",
                    json={"lead_id": update.lead_id, **map_outcome_to_record(update.outcome, self.config.status_map)},
                    headers=self._headers(),
                )
                if response.is_error:
                    failures.append(update.lead_id)
        return BulkResult(success_count=len(updates) - len(failures), failure_count=len(failures), failed_ids=failures)

    async def test_connection(self) -> bool:
        endpoint = self.config.endpoints.get("health")
        async with httpx.AsyncClient(timeout=10, follow_redirects=True) as client:
            for candidate in self._endpoint_candidates(endpoint, "/health", "/api/health"):
                response = await client.get(f"{self.config.base_url}{candidate}", headers=self._headers())
                if response.is_success:
                    return True
            return False
