from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class Lead(BaseModel):
    id: str
    name: str
    phone: str
    project: str | None = None
    budget: str | None = None
    location: str | None = None
    language_preference: str | None = None
    source: str | None = None
    status: str = "new"
    last_called_at: datetime | None = None
    call_attempts: int = 0
    custom_fields: dict[str, Any] = Field(default_factory=dict)
    developer: str | None = None
    voice_gender: Literal["male", "female", "auto"] | None = "auto"


class Qualification(BaseModel):
    bhk: str | None = None
    budget_range: str | None = None
    purpose: str | None = None
    timeline: str | None = None


class CallOutcome(BaseModel):
    status: str
    call_duration_sec: int
    transcript_summary: str
    site_visit_scheduled: bool = False
    site_visit_date: str | None = None
    callback_date: str | None = None
    lead_temperature: str
    qualification: Qualification | dict[str, Any]
    full_transcript: str | None = None
    recording_url: str | None = None


class LeadUpdate(BaseModel):
    lead_id: str
    outcome: CallOutcome


class BulkResult(BaseModel):
    success_count: int = 0
    failure_count: int = 0
    failed_ids: list[str] = Field(default_factory=list)


class CRMConnectionConfig(BaseModel):
    crm_type: str
    base_url: str | None = None
    auth_type: str = "none"
    headers: dict[str, str] = Field(default_factory=dict)
    credentials: dict[str, Any] = Field(default_factory=dict)
    field_map: dict[str, str] = Field(default_factory=dict)
    status_map: dict[str, str] = Field(default_factory=dict)
    endpoints: dict[str, str] = Field(default_factory=dict)
    query: dict[str, Any] = Field(default_factory=dict)
    mock_leads: list["Lead"] = Field(default_factory=list)


class FetchDialableRequest(BaseModel):
    campaign_id: str
    limit: int = 50
    crm_config: CRMConnectionConfig | None = None
    filters: dict[str, Any] = Field(default_factory=dict)


class UpdateLeadRequest(BaseModel):
    outcome: CallOutcome
    crm_config: CRMConnectionConfig | None = None


class BulkUpdateRequest(BaseModel):
    updates: list[LeadUpdate]
    crm_config: CRMConnectionConfig | None = None


class CRMHealthResponse(BaseModel):
    connected: bool
    crm_type: str
    detail: str
