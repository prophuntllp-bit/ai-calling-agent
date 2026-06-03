from __future__ import annotations

from datetime import datetime
from typing import Any

from models import BulkResult, CallOutcome, Lead, LeadUpdate
from adapters.base import CRMAdapter


class MockAdapter(CRMAdapter):
    def __init__(self, config):
        super().__init__(config)
        self._leads: dict[str, Lead] = {lead.id: lead for lead in config.mock_leads}

    async def fetch_dialable_leads(self, campaign_id: str, limit: int, filters: dict[str, Any] | None = None) -> list[Lead]:
        dialable = [lead for lead in self._leads.values() if lead.status in {"new", "callback", "interested"}]
        return dialable[:limit]

    async def get_lead_by_phone(self, phone: str) -> Lead | None:
        for lead in self._leads.values():
            if lead.phone == phone:
                return lead
        return None

    async def update_lead(self, lead_id: str, outcome: CallOutcome) -> bool:
        lead = self._leads.get(lead_id)
        if not lead:
            return False
        lead.status = outcome.status
        lead.last_called_at = datetime.utcnow()
        lead.call_attempts += 1
        lead.custom_fields["last_outcome"] = outcome.model_dump(mode="json")
        return True

    async def bulk_update(self, updates: list[LeadUpdate]) -> BulkResult:
        success_count = 0
        failed_ids: list[str] = []
        for update in updates:
            if await self.update_lead(update.lead_id, update.outcome):
                success_count += 1
            else:
                failed_ids.append(update.lead_id)
        return BulkResult(success_count=success_count, failure_count=len(failed_ids), failed_ids=failed_ids)

    async def test_connection(self) -> bool:
        return True
