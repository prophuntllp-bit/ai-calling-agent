from __future__ import annotations

from typing import Any

from models import CallOutcome, Lead


DEFAULT_FIELD_MAP = {
    "id": "id",
    "name": "name",
    "phone": "phone",
    "project": "project",
    "budget": "budget",
    "location": "location",
    "language_preference": "language_preference",
    "source": "source",
    "status": "status",
    "last_called_at": "last_called_at",
    "call_attempts": "call_attempts",
    "developer": "developer",
    "voice_gender": "voice_gender",
}


def map_record_to_lead(record: dict[str, Any], field_map: dict[str, str] | None = None) -> Lead:
    active_map = {**DEFAULT_FIELD_MAP, **(field_map or {})}
    consumed = {source_key for source_key in active_map.values()}
    payload = {}
    for internal_key, source_key in active_map.items():
        payload[internal_key] = record.get(source_key)
    if payload.get("call_attempts") is None:
        payload["call_attempts"] = 0
    if payload.get("status") is None:
        payload["status"] = "new"
    payload["custom_fields"] = {key: value for key, value in record.items() if key not in consumed}
    if not payload.get("phone"):
        raise ValueError("Phone field missing from CRM payload")
    return Lead(**payload)


def map_outcome_to_record(
    outcome: CallOutcome,
    status_map: dict[str, str] | None = None,
    prefix: str = "ai_",
) -> dict[str, Any]:
    mapped_status = (status_map or {}).get(outcome.status, outcome.status)
    qualification = outcome.qualification.model_dump() if hasattr(outcome.qualification, "model_dump") else outcome.qualification
    booking = "Site Visit Scheduled" if outcome.site_visit_scheduled else None
    priority = "High" if str(outcome.lead_temperature or "").lower() in {"hot", "high"} else "Medium"
    return {
        f"{prefix}status": mapped_status,
        f"{prefix}call_duration_sec": outcome.call_duration_sec,
        f"{prefix}transcript_summary": outcome.transcript_summary,
        f"{prefix}site_visit_scheduled": outcome.site_visit_scheduled,
        f"{prefix}site_visit_date": outcome.site_visit_date,
        f"{prefix}callback_date": outcome.callback_date,
        f"{prefix}lead_temperature": outcome.lead_temperature,
        f"{prefix}qualification": qualification,
        f"{prefix}full_transcript": outcome.full_transcript,
        f"{prefix}recording_url": outcome.recording_url,
        "status": mapped_status,
        "remark": outcome.transcript_summary,
        "followUpDate": outcome.callback_date,
        "priority": priority,
        "booking": booking,
    }
