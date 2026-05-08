from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from models import BulkResult, CallOutcome, CRMConnectionConfig, Lead, LeadUpdate


class CRMAdapter(ABC):
    def __init__(self, config: CRMConnectionConfig):
        self.config = config

    @abstractmethod
    async def fetch_dialable_leads(self, campaign_id: str, limit: int, filters: dict[str, Any] | None = None) -> list[Lead]:
        raise NotImplementedError

    @abstractmethod
    async def get_lead_by_phone(self, phone: str) -> Lead | None:
        raise NotImplementedError

    @abstractmethod
    async def update_lead(self, lead_id: str, outcome: CallOutcome) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def bulk_update(self, updates: list[LeadUpdate]) -> BulkResult:
        raise NotImplementedError

    @abstractmethod
    async def test_connection(self) -> bool:
        raise NotImplementedError
