from __future__ import annotations

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from field_mapping import map_outcome_to_record, map_record_to_lead
from models import BulkResult, CallOutcome, Lead, LeadUpdate
from adapters.base import CRMAdapter


class GoogleSheetsAdapter(CRMAdapter):
    def _service(self):
        creds = Credentials.from_service_account_info(self.config.credentials, scopes=["https://www.googleapis.com/auth/spreadsheets"])
        return build("sheets", "v4", credentials=creds, cache_discovery=False)

    def _sheet_values(self) -> tuple[list[str], list[dict[str, str]]]:
        service = self._service()
        spreadsheet_id = self.config.credentials["spreadsheet_id"]
        range_name = self.config.credentials.get("range", "Sheet1!A:Z")
        result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=range_name).execute()
        rows = result.get("values", [])
        headers = rows[0] if rows else []
        records = [dict(zip(headers, row)) for row in rows[1:]]
        return headers, records

    async def fetch_dialable_leads(self, campaign_id: str, limit: int, filters=None) -> list[Lead]:
        _, records = self._sheet_values()
        leads = [map_record_to_lead(record, self.config.field_map) for record in records if record.get(self.config.field_map.get("phone", "phone"))]
        return leads[:limit]

    async def get_lead_by_phone(self, phone: str) -> Lead | None:
        _, records = self._sheet_values()
        for record in records:
            lead = map_record_to_lead(record, self.config.field_map)
            if lead.phone == phone:
                return lead
        return None

    async def update_lead(self, lead_id: str, outcome: CallOutcome) -> bool:
        await self.bulk_update([LeadUpdate(lead_id=lead_id, outcome=outcome)])
        return True

    async def bulk_update(self, updates: list[LeadUpdate]) -> BulkResult:
        headers, records = self._sheet_values()
        spreadsheet_id = self.config.credentials["spreadsheet_id"]
        range_name = self.config.credentials.get("range", "Sheet1!A:Z")
        service = self._service()
        update_map = {item.lead_id: map_outcome_to_record(item.outcome, self.config.status_map, prefix="") for item in updates}
        output = [headers]
        for record in records:
            lead = map_record_to_lead(record, self.config.field_map)
            if lead.id in update_map:
                record.update(update_map[lead.id])
            output.append([record.get(header, "") for header in headers])
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption="RAW",
            body={"values": output},
        ).execute()
        return BulkResult(success_count=len(updates), failure_count=0, failed_ids=[])

    async def test_connection(self) -> bool:
        try:
            self._sheet_values()
            return True
        except Exception:
            return False
