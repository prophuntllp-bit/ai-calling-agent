from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field
from sqlmodel import Field as SQLField
from sqlmodel import JSON, Column, SQLModel


class CallingSchedule(BaseModel):
    timezone: str = "Asia/Kolkata"
    active_days: list[str] = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    start_time: str = "10:00"
    end_time: str = "19:00"
    lunch_break_start: str = "13:00"
    lunch_break_end: str = "14:00"


class Campaign(BaseModel):
    id: str
    name: str
    project_id: str
    crm_source: str
    crm_config_id: str
    voice_gender: str
    voice_id: str | None = None
    language: str
    calling_schedule: CallingSchedule
    lead_filters: dict[str, Any] = Field(default_factory=dict)
    max_concurrent: int = 10
    max_attempts: int = 3
    retry_interval_hours: int = 4
    status: str = "draft"


class CampaignRecord(SQLModel, table=True):
    id: str = SQLField(primary_key=True)
    name: str
    project_id: str
    crm_source: str
    crm_config_id: str
    voice_gender: str
    voice_id: str | None = None
    language: str
    calling_schedule: dict[str, Any] = SQLField(sa_column=Column(JSON))
    lead_filters: dict[str, Any] = SQLField(sa_column=Column(JSON))
    max_concurrent: int = 10
    max_attempts: int = 3
    retry_interval_hours: int = 4
    status: str = "draft"
    created_at: datetime = SQLField(default_factory=datetime.utcnow)
    updated_at: datetime = SQLField(default_factory=datetime.utcnow)


class CallLogRecord(SQLModel, table=True):
    id: str = SQLField(primary_key=True)
    campaign_id: str
    lead_id: str
    phone: str
    status: str
    attempt: int = 1
    started_at: datetime = SQLField(default_factory=datetime.utcnow)
    ended_at: datetime | None = None
    duration_sec: int = 0
    call_metadata: dict[str, Any] = SQLField(default_factory=dict, sa_column=Column(JSON))
