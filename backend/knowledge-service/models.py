from typing import Any

from pydantic import BaseModel, Field


class Configuration(BaseModel):
    type: str
    carpet_area_sqft: int
    price_lakh: float
    price_per_sqft: int
    floor_plan_url: str | None = None


class LocationAdvantage(BaseModel):
    place: str
    distance_km: float
    travel_time_min: int
    category: str


class MultilingualResponse(BaseModel):
    response_hi: str | None = None
    response_en: str | None = None
    response_mr: str | None = None
    response_ta: str | None = None
    response_te: str | None = None
    response_kn: str | None = None
    response_ml: str | None = None
    response_bn: str | None = None
    response_gu: str | None = None
    response_pa: str | None = None


class ObjectionResponse(MultilingualResponse):
    objection: str


class SiteVisitInfo(BaseModel):
    address: str
    google_maps_link: str
    available_days: list[str]
    timings: str
    contact_person: str
    contact_phone: str


class PriceRange(BaseModel):
    min_lakh: float
    max_lakh: float
    currency: str = "INR"


class Project(BaseModel):
    id: str
    name: str
    developer: str
    location: str
    city: str
    rera_number: str | None = None
    project_type: str
    configurations: list[Configuration]
    price_range: PriceRange
    amenities: list[str]
    location_advantages: list[LocationAdvantage]
    construction_status: str
    possession_date: str | None = None
    usp: list[str]
    common_objections: list[ObjectionResponse]
    site_visit_info: SiteVisitInfo
    extra: dict[str, Any] = Field(default_factory=dict)


class KnowledgeChunk(BaseModel):
    id: str
    project_id: str
    section: str
    text: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    embedding: list[float] = Field(default_factory=list)


class KnowledgeAddRequest(BaseModel):
    section: str
    content: dict[str, Any] | list[Any] | str


class QueryResponse(BaseModel):
    project_id: str
    query: str
    matches: list[KnowledgeChunk]
