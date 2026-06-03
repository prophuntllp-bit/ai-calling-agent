from datetime import datetime
from typing import Any

from sqlmodel import Column, Field, JSON, SQLModel


class Tenant(SQLModel, table=True):
    id: str = Field(primary_key=True)
    name: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    tenant_metadata: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))


class User(SQLModel, table=True):
    id: str = Field(primary_key=True)
    tenant_id: str
    email: str
    password_hash: str
    role: str = "tenant"
    created_at: datetime = Field(default_factory=datetime.utcnow)


class CRMConnection(SQLModel, table=True):
    id: str = Field(primary_key=True)
    tenant_id: str
    crm_type: str
    config: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=datetime.utcnow)


class VoiceProfile(SQLModel, table=True):
    id: str = Field(primary_key=True)
    tenant_id: str
    label: str
    gender: str
    language: str
    file_path: str
    created_at: datetime = Field(default_factory=datetime.utcnow)


class CallLog(SQLModel, table=True):
    id: str = Field(primary_key=True)
    tenant_id: str
    campaign_id: str | None = None
    lead_id: str | None = None
    phone: str
    status: str
    call_metadata: dict[str, Any] = Field(default_factory=dict, sa_column=Column(JSON))
    created_at: datetime = Field(default_factory=datetime.utcnow)


class CampaignLink(SQLModel, table=True):
    id: str = Field(primary_key=True)
    tenant_id: str
    campaign_id: str
    name: str
    status: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
