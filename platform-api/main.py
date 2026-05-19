from __future__ import annotations

import base64
import io
import os
import uuid
import wave

import httpx
from fastapi import FastAPI, HTTPException, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from sqlmodel import Field, Session, SQLModel, create_engine, select

from shared.logging import configure_logging
from shared.tracing import RequestTracingMiddleware

from auth import create_access_token, hash_password, verify_password
from middleware import TenantIsolationMiddleware
from models import CRMConnection, CallLog, CampaignLink, Tenant, User, VoiceProfile


logger = configure_logging("platform-api")
app = FastAPI(title="Platform API", version="1.0.0")
app.add_middleware(RequestTracingMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(TenantIsolationMiddleware)

DATABASE_URL = os.getenv("PLATFORM_DATABASE_URL", os.getenv("DATABASE_URL", "sqlite:///./platform.db"))
engine = create_engine(DATABASE_URL, echo=False)
campaign_service_url = os.getenv("CAMPAIGN_SERVICE_URL", "http://campaign-service:8012")
knowledge_service_url = os.getenv("KNOWLEDGE_SERVICE_URL", "http://knowledge-service:8011")
tts_service_url = os.getenv("TTS_SERVICE_URL", "http://tts:8003")
crm_adapter_url = os.getenv("CRM_ADAPTER_URL", "http://crm-adapter:8010")
llm_service_url = os.getenv("LLM_URL", "http://llm:11434")
orchestrator_url = os.getenv("ORCHESTRATOR_URL", "http://orchestrator:8000")


class RegisterRequest(BaseModel):
    tenant_name: str
    email: EmailStr
    password: str


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class VoiceSandboxRequest(BaseModel):
    project_id: str | None = None
    voice_id: str | None = None
    language: str = "en"
    gender: str = "female"
    lead_name: str = "Prospect"
    lead_message: str | None = None
    opening_text: str | None = None
    history: list[dict] = []


class FollowupWritebackRequest(BaseModel):
    status: str = "Follow Up"
    remark: str | None = None
    follow_up_date: str | None = None
    priority: str = "Medium"
    booking: str | None = None


class OutboundCallRequest(BaseModel):
    phone: str
    lead_name: str = "Prospect"
    language: str = "en-IN"
    provider: str = "enablex"
    opening_line: str | None = None


class InternalCallLogRequest(BaseModel):
    tenant_id: str
    campaign_id: str | None = None
    lead_id: str | None = None
    phone: str = "unknown"
    status: str = "completed"
    call_metadata: dict = {}


def _normalize_voice_language(language: str | None) -> str:
    value = (language or "").strip()
    if not value:
        return "en-IN"
    if "-" in value:
        return value
    mapping = {
        "en": "en-IN",
        "hi": "hi-IN",
        "bn": "bn-IN",
        "gu": "gu-IN",
        "kn": "kn-IN",
        "mr": "mr-IN",
        "od": "od-IN",
        "or": "od-IN",
        "pa": "pa-IN",
        "ta": "ta-IN",
        "te": "te-IN",
        "ml": "ml-IN",
    }
    return mapping.get(value.lower(), value)


def _voice_matches_language(voice: dict, target_language: str) -> bool:
    voice_language = _normalize_voice_language(voice.get("language"))
    target = _normalize_voice_language(target_language)
    if voice_language == target:
        return True
    return voice_language.split("-")[0].lower() == target.split("-")[0].lower()


async def _fetch_provider_voices(language: str | None = None) -> list[dict]:
    params = {}
    if language:
        params["language"] = language
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(f"{tts_service_url}/voices", params=params)
            response.raise_for_status()
        return response.json().get("voices", [])
    except Exception:
        return []


def _select_lead_voice(voices: list[dict], request: VoiceSandboxRequest) -> tuple[str | None, str]:
    requested_language = _normalize_voice_language(request.language)
    lead_gender = "male" if (request.gender or "female").lower() == "female" else "female"
    selected_voice = (request.voice_id or "").strip()
    candidates = [voice for voice in voices if _voice_matches_language(voice, requested_language)]
    if selected_voice:
        candidates = [voice for voice in candidates if (voice.get("path") or voice.get("file_path") or "") != selected_voice]
    preferred = next((voice for voice in candidates if (voice.get("gender") or "").lower() == lead_gender), None)
    fallback = preferred or next((voice for voice in candidates if (voice.get("gender") or "").lower() != (request.gender or "").lower()), None)
    choice = fallback or next((voice for voice in candidates), None)
    if not choice:
        return request.voice_id, lead_gender
    return choice.get("path") or choice.get("file_path") or choice.get("voice_id"), choice.get("gender", lead_gender)


async def _synthesize_segment(text: str, voice_id: str | None, language: str, gender: str, context: dict | None = None) -> bytes:
    async with httpx.AsyncClient(timeout=45) as client:
        response = await client.post(
            f"{tts_service_url}/synthesize",
            json={
                "text": text,
                "voice_id": voice_id,
                "language": language,
                "gender": gender,
                "context": context or {},
            },
        )
        if response.is_error:
            detail = response.text
            try:
                detail = response.json()
            except Exception:
                pass
            raise HTTPException(status_code=response.status_code, detail=detail)
    return response.content


def _combine_wav_segments(segments: list[bytes], pause_ms: int = 220) -> bytes:
    valid_segments = [segment for segment in segments if segment]
    if not valid_segments:
        return b""
    output = io.BytesIO()
    with wave.open(io.BytesIO(valid_segments[0]), "rb") as first:
        nchannels = first.getnchannels()
        sampwidth = first.getsampwidth()
        framerate = first.getframerate()
        comptype = first.getcomptype()
        compname = first.getcompname()
        frames = [first.readframes(first.getnframes())]
    pause_frames = b"\x00" * int(framerate * (pause_ms / 1000) * sampwidth * nchannels)
    for segment in valid_segments[1:]:
        with wave.open(io.BytesIO(segment), "rb") as current:
            if (
                current.getnchannels() != nchannels
                or current.getsampwidth() != sampwidth
                or current.getframerate() != framerate
            ):
                continue
            frames.append(pause_frames)
            frames.append(current.readframes(current.getnframes()))
    with wave.open(output, "wb") as writer:
        writer.setnchannels(nchannels)
        writer.setsampwidth(sampwidth)
        writer.setframerate(framerate)
        writer.setcomptype(comptype, compname)
        writer.writeframes(b"".join(frames))
    return output.getvalue()


async def fetch_project_context(project_id: str | None, query: str) -> str:
    if not project_id or not query.strip():
        return ""
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(
                f"{knowledge_service_url}/projects/{project_id}/query",
                params={"q": query},
            )
            response.raise_for_status()
        matches = response.json().get("matches", [])
    except Exception:
        return ""
    return "\n".join(
        f"[{match.get('section', 'context')}] {match.get('text', '')}".strip()
        for match in matches[:4]
        if match.get("text")
    )


async def generate_sandbox_reply(payload: VoiceSandboxRequest) -> tuple[str, str]:
    normalized_language = (payload.language or "en").strip() or "en"
    lead_name = (payload.lead_name or "Prospect").strip() or "Prospect"
    if not (payload.lead_message or "").strip():
        opening = (payload.opening_text or "").strip()
        if opening:
            return opening, ""
        return (
            f"Hello, this is Priya from Prophunt. I am calling regarding your interest in our project. "
            f"Is this a good time to talk for thirty seconds, {lead_name}?",
            "",
        )

    project_context = await fetch_project_context(payload.project_id, payload.lead_message or "")
    system_prompt = f"""You are Priya, an AI real-estate voice agent from Prophunt.

Your job:
- respond naturally in {normalized_language}
- keep replies under 3 short sentences
- sound warm, clear, and human
- move the conversation toward qualification, callback, or site visit
- do not invent pricing or possession details

  Lead name: {lead_name}
  Project knowledge:
  {project_context or 'No project knowledge is loaded for this turn.'}
  """
    messages = [{"role": "system", "content": system_prompt}]
    messages.extend(payload.history[-8:])
    messages.append({"role": "user", "content": payload.lead_message.strip()})

    def fallback_reply() -> str:
        latest = (payload.lead_message or "").strip().lower()
        if any(token in latest for token in ["price", "cost", "budget", "pricing"]):
            return "I can help with that. We can walk through the pricing options and shortlist the right unit based on your budget. Would you like a quick overview or a callback from the sales team?"
        if any(token in latest for token in ["location", "where", "address", "place"]):
            return "It is in a well-connected area, and I can share the location highlights in a quick summary. Would you like the exact location details or nearby landmark information first?"
        if any(token in latest for token in ["2bhk", "3bhk", "bhk", "unit", "configuration"]):
            return "We can explore the available configurations based on what fits your requirement best. Are you mainly looking for a two BHK or would you like me to include three BHK options too?"
        if any(token in latest for token in ["visit", "site visit", "schedule", "meet"]):
            return "Absolutely. I can mark you as interested for a site visit and arrange the follow-up. What day works better for you, weekday or weekend?"
        if any(token in latest for token in ["tell me more", "more", "details", "project"]):
            return "Sure. This project is positioned for buyers looking for a well-connected home with good lifestyle value. I can next walk you through the location, configurations, or pricing depending on what matters most to you."
        return "I can help with that. To guide you properly, may I know whether your main focus is location, pricing, or configuration?"

    reply = ""
    model = os.getenv("LLM_MODEL", "llama3:latest")
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            response = await client.post(
                f"{llm_service_url}/v1/chat/completions",
                json={
                    "model": model,
                    "messages": messages,
                    "temperature": 0.5,
                    "max_tokens": 180,
                    "stream": False,
                },
            )
            response.raise_for_status()
            reply = response.json().get("choices", [{}])[0].get("message", {}).get("content", "").strip()
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 404:
                raise
            try:
                response = await client.post(
                    f"{llm_service_url}/api/chat",
                    json={
                        "model": model,
                        "messages": messages,
                        "stream": False,
                        "options": {
                            "temperature": 0.5,
                        },
                    },
                )
                response.raise_for_status()
                reply = response.json().get("message", {}).get("content", "").strip()
            except httpx.HTTPError:
                reply = fallback_reply()
        except httpx.HTTPError:
            reply = fallback_reply()

    if not reply:
        reply = fallback_reply()
    return reply, project_context


@app.on_event("startup")
def startup():
    SQLModel.metadata.create_all(engine)


@app.post("/auth/register")
async def register(request: RegisterRequest):
    tenant = Tenant(id=str(uuid.uuid4()), name=request.tenant_name)
    user = User(id=str(uuid.uuid4()), tenant_id=tenant.id, email=request.email, password_hash=hash_password(request.password), role="admin")
    user_id = user.id
    tenant_id = tenant.id
    user_role = user.role
    with Session(engine) as session:
        session.add(tenant)
        session.add(user)
        session.commit()
    return {"access_token": create_access_token(user_id, tenant_id, user_role), "tenant_id": tenant_id}


@app.post("/auth/login")
async def login(request: LoginRequest):
    with Session(engine) as session:
        user = session.exec(select(User).where(User.email == request.email)).first()
    if not user or not verify_password(request.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    user_id = user.id
    tenant_id = user.tenant_id
    user_role = user.role
    return {"access_token": create_access_token(user_id, tenant_id, user_role), "tenant_id": tenant_id}


@app.post("/auth/refresh")
async def refresh(request: Request):
    return {"access_token": create_access_token(request.state.user_id, request.state.tenant_id, request.state.role)}


@app.post("/tenants")
async def create_tenant(payload: dict):
    tenant = Tenant(id=str(uuid.uuid4()), name=payload["name"], tenant_metadata=payload.get("metadata", {}))
    with Session(engine) as session:
        session.add(tenant)
        session.commit()
    return tenant.model_dump(mode="json")


@app.get("/tenants/{tenant_id}")
async def get_tenant(tenant_id: str, request: Request):
    if request.state.tenant_id != tenant_id and request.state.role != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    with Session(engine) as session:
        tenant = session.get(Tenant, tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    return tenant.model_dump(mode="json")


@app.post("/tenants/{tenant_id}/crm-connections")
async def create_crm_connection(tenant_id: str, payload: dict, request: Request):
    if request.state.tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    connection = CRMConnection(id=str(uuid.uuid4()), tenant_id=tenant_id, crm_type=payload["crm_type"], config=payload)
    with Session(engine) as session:
        session.add(connection)
        session.commit()
    return connection.model_dump(mode="json")


@app.get("/tenants/{tenant_id}/crm-connections")
async def list_crm_connections(tenant_id: str, request: Request):
    if request.state.tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    with Session(engine) as session:
        connections = session.exec(select(CRMConnection).where(CRMConnection.tenant_id == tenant_id)).all()
    return {"connections": [connection.model_dump(mode="json") for connection in connections]}


@app.get("/tenants/{tenant_id}/leads")
async def list_tenant_leads(
    tenant_id: str,
    request: Request,
    crm_connection_id: str | None = None,
    campaign_id: str = "preview",
    limit: int = 50,
):
    if request.state.tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    with Session(engine) as session:
        statement = select(CRMConnection).where(CRMConnection.tenant_id == tenant_id)
        if crm_connection_id:
            statement = statement.where(CRMConnection.id == crm_connection_id)
        connections = session.exec(statement).all()

    if not connections:
        return {"leads": [], "connection_id": None, "crm_type": None}

    connection = sorted(connections, key=lambda item: item.created_at, reverse=True)[0]
    crm_config = {
        "crm_type": connection.crm_type,
        **connection.config,
    }

    async with httpx.AsyncClient(timeout=45) as client:
        response = await client.post(
            f"{crm_adapter_url}/api/leads/fetch-dialable",
            json={
                "campaign_id": campaign_id,
                "limit": max(1, min(limit, 200)),
                "crm_config": crm_config,
                "filters": {},
            },
        )
    if response.is_error:
        detail = response.text
        try:
            detail = response.json()
        except Exception:
            pass
        raise HTTPException(status_code=response.status_code, detail=detail)
    payload = response.json()
    return {
        "leads": payload.get("leads", []),
        "connection_id": connection.id,
        "crm_type": connection.crm_type,
        "total": payload.get("total", len(payload.get("leads", []))),
    }


@app.post("/tenants/{tenant_id}/leads/{lead_id}/follow-up")
async def writeback_followup(tenant_id: str, lead_id: str, payload: FollowupWritebackRequest, request: Request):
    if request.state.tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    with Session(engine) as session:
        connections = session.exec(select(CRMConnection).where(CRMConnection.tenant_id == tenant_id)).all()

    if not connections:
        raise HTTPException(status_code=400, detail="Connect a CRM first before saving follow-up outcomes.")

    connection = sorted(connections, key=lambda item: item.created_at, reverse=True)[0]
    crm_config = {
        "crm_type": connection.crm_type,
        **connection.config,
    }
    lead_temperature = "high" if payload.priority.lower() == "high" else "warm"
    site_visit = bool(payload.booking and "visit" in payload.booking.lower())

    outcome = {
        "status": payload.status,
        "call_duration_sec": 0,
        "transcript_summary": (payload.remark or "Follow-up updated from VoiceAI Pulse").strip(),
        "site_visit_scheduled": site_visit,
        "site_visit_date": payload.follow_up_date if site_visit else None,
        "callback_date": payload.follow_up_date,
        "lead_temperature": lead_temperature,
        "qualification": {},
        "full_transcript": payload.remark,
        "recording_url": None,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.patch(
            f"{crm_adapter_url}/api/leads/{lead_id}/update",
            json={"crm_config": crm_config, "outcome": outcome},
        )
    if response.is_error:
        detail = response.text
        try:
            detail = response.json().get("detail", detail)
        except Exception:
            detail = detail or "CRM follow-up update failed."
        raise HTTPException(status_code=response.status_code, detail=detail)

    return {
        "updated": True,
        "lead_id": lead_id,
        "status": payload.status,
        "remark": payload.remark,
        "follow_up_date": payload.follow_up_date,
        "priority": payload.priority,
        "booking": payload.booking,
        "crm_connection_id": connection.id,
    }


@app.post("/tenants/{tenant_id}/crm-connections/test")
async def test_crm_connection(tenant_id: str, payload: dict, request: Request):
    if request.state.tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    async with httpx.AsyncClient(timeout=45) as client:
        response = await client.post(f"{crm_adapter_url}/api/crm/test-connection", json=payload)
    if response.is_error:
        detail = response.text
        try:
            detail = response.json()
        except Exception:
            pass
        raise HTTPException(status_code=response.status_code, detail=detail)
    return response.json()


@app.post("/projects")
async def create_project(payload: dict):
    tenant_id = payload.get("tenant_id")
    payload["extra"] = {
        **payload.get("extra", {}),
        **({"tenant_id": tenant_id} if tenant_id else {}),
    }
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(f"{knowledge_service_url}/projects", json=payload)
    if response.is_error:
        detail = response.text
        try:
            detail = response.json()
        except Exception:
            pass
        raise HTTPException(status_code=response.status_code, detail=detail)
    return response.json()


@app.get("/projects")
async def list_projects(request: Request):
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(f"{knowledge_service_url}/projects")
            response.raise_for_status()
    except Exception:
        return {"projects": []}
    tenant_projects = [
        project
        for project in response.json().get("projects", [])
        if project.get("extra", {}).get("tenant_id") in (None, request.state.tenant_id)
    ]
    return {"projects": tenant_projects}


@app.get("/projects/{project_id}")
async def get_project(project_id: str):
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(f"{knowledge_service_url}/projects/{project_id}")
        response.raise_for_status()
    return response.json()


@app.post("/campaigns")
async def create_campaign(payload: dict, request: Request):
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(f"{campaign_service_url}/campaigns", json=payload)
        response.raise_for_status()
    link = CampaignLink(id=str(uuid.uuid4()), tenant_id=request.state.tenant_id, campaign_id=payload["id"], name=payload["name"], status=payload.get("status", "draft"))
    with Session(engine) as session:
        session.add(link)
        session.commit()
    return response.json()


@app.get("/tenants/{tenant_id}/campaigns")
async def list_tenant_campaigns(tenant_id: str, request: Request):
    if request.state.tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    with Session(engine) as session:
        links = session.exec(select(CampaignLink).where(CampaignLink.tenant_id == tenant_id)).all()
    return {"campaigns": [link.model_dump(mode="json") for link in links]}


@app.get("/campaigns/{campaign_id}")
async def get_campaign(campaign_id: str):
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(f"{campaign_service_url}/campaigns/{campaign_id}")
        response.raise_for_status()
    return response.json()


@app.post("/campaigns/{campaign_id}/{action}")
async def campaign_action(campaign_id: str, action: str, request: Request):
    if action not in {"start", "pause", "stop"}:
        raise HTTPException(status_code=400, detail="Unsupported action")
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(f"{campaign_service_url}/campaigns/{campaign_id}/{action}")
        response.raise_for_status()
    status_map = {"start": "active", "pause": "paused", "stop": "completed"}
    with Session(engine) as session:
        link = session.exec(
            select(CampaignLink).where(
                CampaignLink.campaign_id == campaign_id,
                CampaignLink.tenant_id == request.state.tenant_id,
            )
        ).first()
        if link:
            link.status = status_map[action]
            session.add(link)
            session.commit()
    return response.json()


@app.post("/voices")
async def upload_voice(request: Request, file: UploadFile, gender: str, language: str, label: str):
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.post(
            f"{tts_service_url}/register-voice",
            data={"client_id": request.state.tenant_id, "voice_name": label, "gender": gender},
            files={"audio": (file.filename, await file.read(), file.content_type or "audio/wav")},
        )
        response.raise_for_status()
    profile = VoiceProfile(id=str(uuid.uuid4()), tenant_id=request.state.tenant_id, label=label, gender=gender, language=language, file_path=response.json()["voice_id"])
    with Session(engine) as session:
        session.add(profile)
        session.commit()
    return profile.model_dump(mode="json")


@app.get("/voice-catalog")
async def voice_catalog():
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.get(f"{tts_service_url}/voices")
        response.raise_for_status()
    catalog = response.json().get("voices", [])
    # Detect active provider from returned voices
    has_elevenlabs = any(str(v.get("path") or "").startswith("elevenlabs://") for v in catalog)
    voices = []
    for voice in catalog:
        path = str(voice.get("path") or voice.get("file_path") or "")
        if path.startswith("sarvam://") and has_elevenlabs:
            continue  # hide Sarvam voices when ElevenLabs is active
        if path.startswith("elevenlabs://"):
            provider = "elevenlabs"
        elif path.startswith("sarvam://"):
            provider = "sarvam"
        else:
            provider = "tts"
        voices.append(
            {
                "id": voice.get("voice_id") or voice.get("id") or path,
                "label": voice.get("label") or voice.get("voice_id") or path,
                "gender": voice.get("gender", "female"),
                "language": voice.get("language", "en-IN"),
                "file_path": path or voice.get("voice_id"),
                "provider": provider,
            }
        )
    return {"voices": voices}


@app.get("/tenants/{tenant_id}/voices")
async def list_voices(tenant_id: str, request: Request):
    if request.state.tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    with Session(engine) as session:
        voices = session.exec(select(VoiceProfile).where(VoiceProfile.tenant_id == tenant_id)).all()
    stored_voices = [voice.model_dump(mode="json") for voice in voices]
    provider_voices = []
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(f"{tts_service_url}/voices")
            response.raise_for_status()
        provider_voices = response.json().get("voices", [])
    except Exception:
        provider_voices = []

    # Detect if ElevenLabs is active — hide legacy Sarvam voices if so
    has_elevenlabs = any(str(v.get("path") or v.get("file_path") or "").startswith("elevenlabs://") for v in provider_voices)

    merged = []
    seen = set()
    for voice in [*provider_voices, *stored_voices]:  # provider first so ElevenLabs wins dedup
        identifier = voice.get("file_path") or voice.get("path") or voice.get("voice_id") or voice.get("id")
        if identifier in seen:
            continue
        seen.add(identifier)
        path = str(voice.get("file_path") or voice.get("path") or "")
        # Skip Sarvam voices when ElevenLabs is active
        if has_elevenlabs and path.startswith("sarvam://"):
            continue
        inferred_provider = voice.get("provider")
        if not inferred_provider:
            if path.startswith("elevenlabs://"):
                inferred_provider = "elevenlabs"
            elif path.startswith("sarvam://"):
                inferred_provider = "sarvam"
            else:
                inferred_provider = "uploaded" if voice.get("id") else "tts"
        merged.append(
            {
                "id": voice.get("id") or voice.get("voice_id") or identifier,
                "label": voice.get("label") or voice.get("voice_id") or identifier,
                "gender": voice.get("gender", "female"),
                "language": voice.get("language", "en-IN"),
                "file_path": path or voice.get("voice_id") or identifier,
                "provider": inferred_provider,
            }
        )
    return {"voices": merged}


@app.post("/tenants/{tenant_id}/voice-agent/sandbox")
async def voice_agent_sandbox(tenant_id: str, payload: VoiceSandboxRequest, request: Request):
    if request.state.tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    opening_text = (payload.opening_text or "").strip()
    is_first_turn = not payload.history
    reply_text, project_context = await generate_sandbox_reply(payload)
    transcript_history = list(payload.history)
    has_lead_message = bool(payload.lead_message and payload.lead_message.strip())
    should_stage_opening = bool(opening_text and is_first_turn and has_lead_message)
    if should_stage_opening:
        transcript_history.append({"role": "assistant", "content": opening_text})
    if has_lead_message:
        transcript_history.append({"role": "user", "content": payload.lead_message.strip()})
    transcript_history.append({"role": "assistant", "content": reply_text})

    conversation_segments: list[bytes] = []
    shared_context = {
        "mode": "voice_sandbox",
        "lead_name": payload.lead_name,
        "project_id": payload.project_id,
    }
    if should_stage_opening:
        conversation_segments.append(
            await _synthesize_segment(
                opening_text,
                payload.voice_id,
                payload.language,
                payload.gender,
                {**shared_context, "speaker_role": "agent_opening"},
            )
        )
    if has_lead_message:
        provider_voices = await _fetch_provider_voices(payload.language)
        lead_voice_id, lead_gender = _select_lead_voice(provider_voices, payload)
        conversation_segments.append(
            await _synthesize_segment(
                payload.lead_message.strip(),
                lead_voice_id,
                payload.language,
                lead_gender,
                {**shared_context, "speaker_role": "lead_reply"},
            )
        )
    conversation_segments.append(
        await _synthesize_segment(
            reply_text,
            payload.voice_id,
            payload.language,
            payload.gender,
            {**shared_context, "speaker_role": "agent_reply"},
        )
    )
    audio_bytes = _combine_wav_segments(conversation_segments)

    return {
        "reply_text": reply_text,
        "audio_base64": base64.b64encode(audio_bytes).decode("ascii"),
        "audio_mime": "audio/wav",
        "history": transcript_history[-12:],
        "project_context": project_context,
    }


@app.get("/tenants/{tenant_id}/calls")
async def tenant_calls(tenant_id: str, request: Request):
    if request.state.tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    with Session(engine) as session:
        calls = session.exec(select(CallLog).where(CallLog.tenant_id == tenant_id).order_by(CallLog.created_at.desc())).all()
    return {"calls": [call.model_dump(mode="json") for call in calls]}


@app.post("/internal/calls")
async def create_internal_call_log(payload: InternalCallLogRequest, request: Request):
    expected_token = os.getenv("ORCHESTRATOR_INTERNAL_TOKEN", "local-dev-internal-token")
    supplied_token = request.headers.get("X-Internal-Token", "")
    if supplied_token != expected_token:
        raise HTTPException(status_code=401, detail="Invalid internal token")
    call_log = CallLog(
        id=str(uuid.uuid4()),
        tenant_id=payload.tenant_id,
        campaign_id=payload.campaign_id,
        lead_id=payload.lead_id,
        phone=payload.phone or "unknown",
        status=payload.status,
        call_metadata=payload.call_metadata,
    )
    with Session(engine) as session:
        session.add(call_log)
        session.commit()
        session.refresh(call_log)
    return call_log.model_dump(mode="json")


@app.get("/internal/calls")
async def list_internal_calls(request: Request, limit: int = 20):
    """Dashboard endpoint — returns recent calls with recording_url flattened to top level."""
    expected_token = os.getenv("ORCHESTRATOR_INTERNAL_TOKEN", "local-dev-internal-token")
    supplied_token = request.headers.get("X-Internal-Token", "") or request.query_params.get("token", "")
    if supplied_token != expected_token:
        raise HTTPException(status_code=401, detail="Invalid internal token")
    with Session(engine) as db:
        calls = db.exec(
            select(CallLog).order_by(CallLog.created_at.desc()).limit(limit)
        ).all()
    result = []
    for c in calls:
        meta = c.call_metadata or {}
        outcome = meta.get("outcome") or {}
        recordings = meta.get("recordings") or {}
        result.append({
            "call_sid": meta.get("call_sid") or c.id,
            "phone": c.phone,
            "lead_name": meta.get("lead_name") or outcome.get("lead_name"),
            "status": c.status,
            "outcome": outcome.get("status") or c.status,
            "duration": outcome.get("call_duration_sec") or meta.get("duration_sec") or 0,
            "recording_url": meta.get("recording_url") or outcome.get("recording_url") or recordings.get("mixed_url"),
            "recording_caller_url": recordings.get("caller_url"),
            "recording_agent_url": recordings.get("agent_url"),
            "transcript": outcome.get("transcript_summary"),
            "created_at": c.created_at.isoformat() if c.created_at else None,
            "tenant_id": c.tenant_id,
        })
    return {"calls": result, "total": len(result)}


@app.post("/tenants/{tenant_id}/calls/outbound-test")
async def outbound_test_call(tenant_id: str, payload: OutboundCallRequest, request: Request):
    if request.state.tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Forbidden")

    lead_id = str(uuid.uuid4())
    lead = {
        "id": lead_id,
        "tenant_id": tenant_id,
        "name": payload.lead_name,
        "phone": payload.phone,
        "language_preference": payload.language,
        "project": "EnableX provider test",
        "developer": "Prophunt",
    }
    opening_line = payload.opening_line or (
        f"Hello, this is Priya from Prophunt. I am calling regarding your interest in our project. "
        f"Is this a good time to talk for thirty seconds, {payload.lead_name}?"
    )

    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            f"{orchestrator_url}/call/dial",
            json={
                "provider": payload.provider,
                "lead": lead,
                "campaign": {"id": "provider-test", "name": "EnableX provider test", "tenant_id": tenant_id},
                "opening_line": opening_line,
            },
        )

    if response.is_error:
        detail = response.text
        try:
            detail = response.json()
        except Exception:
            pass
        raise HTTPException(status_code=response.status_code, detail=detail)

    result = response.json()
    call_log = CallLog(
        id=str(uuid.uuid4()),
        tenant_id=tenant_id,
        lead_id=lead_id,
        phone=payload.phone,
        campaign_id="provider-test",
        status=result.get("status", "initiated"),
        call_metadata={
            "summary": f"Outbound test call via {result.get('provider', payload.provider)}",
            "provider_result": result,
        },
    )
    with Session(engine) as session:
        session.add(call_log)
        session.commit()

    return result


@app.get("/tenants/{tenant_id}/analytics")
async def tenant_analytics(tenant_id: str, request: Request):
    if request.state.tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    with Session(engine) as session:
        calls = session.exec(select(CallLog).where(CallLog.tenant_id == tenant_id)).all()
    totals = {}
    for call in calls:
        totals[call.status] = totals.get(call.status, 0) + 1
    return {"tenant_id": tenant_id, "totals": totals, "call_count": len(calls)}


@app.get("/tenants/{tenant_id}/dashboard-summary")
async def dashboard_summary(tenant_id: str, request: Request):
    if request.state.tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    with Session(engine) as session:
        crm_count = len(session.exec(select(CRMConnection).where(CRMConnection.tenant_id == tenant_id)).all())
        voice_count = len(session.exec(select(VoiceProfile).where(VoiceProfile.tenant_id == tenant_id)).all())
        campaign_count = len(session.exec(select(CampaignLink).where(CampaignLink.tenant_id == tenant_id)).all())
        call_count = len(session.exec(select(CallLog).where(CallLog.tenant_id == tenant_id)).all())
    project_count = 0
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(f"{knowledge_service_url}/projects")
            response.raise_for_status()
        project_count = len(
            [project for project in response.json().get("projects", []) if project.get("extra", {}).get("tenant_id") in (None, tenant_id)]
        )
    except Exception:
        project_count = 0
    return {
        "tenant_id": tenant_id,
        "metrics": {
            "crm_connections": crm_count,
            "voices": voice_count,
            "campaigns": campaign_count,
            "projects": project_count,
            "calls": call_count,
        },
    }


@app.post("/tenants/{tenant_id}/webhooks")
async def register_webhook(tenant_id: str, payload: dict, request: Request):
    if request.state.tenant_id != tenant_id:
        raise HTTPException(status_code=403, detail="Forbidden")
    with Session(engine) as session:
        tenant = session.get(Tenant, tenant_id)
        if not tenant:
            raise HTTPException(status_code=404, detail="Tenant not found")
        tenant.tenant_metadata = {
            **tenant.tenant_metadata,
            "webhooks": tenant.tenant_metadata.get("webhooks", []) + [payload],
        }
        session.add(tenant)
        session.commit()
    return {"status": "registered"}


@app.get("/health")
async def health():
    with Session(engine) as session:
        session.exec(select(Tenant).limit(1)).all()
    return {"status": "ok", "database": "reachable"}


# ─────────────────────────────────────────────────────────────────────────────
# KNOWLEDGE BASE  —  /internal/knowledge-bases/*
# Uses simple PostgreSQL storage. Content is injected into agent calls via
# prompt_dynamic_variables so the AI answers project-specific questions.
# ─────────────────────────────────────────────────────────────────────────────

import datetime
from typing import Optional

class KnowledgeBase(SQLModel, table=True):
    __tablename__ = "knowledge_bases"
    __table_args__ = {"extend_existing": True}
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    name: str
    description: Optional[str] = None
    tenant_id: str = Field(default="default")
    created_at: Optional[str] = Field(default_factory=lambda: datetime.datetime.utcnow().isoformat())


class KnowledgeSource(SQLModel, table=True):
    __tablename__ = "knowledge_sources"
    __table_args__ = {"extend_existing": True}
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    kb_id: str = Field(foreign_key="knowledge_bases.id")
    name: str
    type: str = Field(default="text")   # "text" | "qa"
    content: str
    char_count: int = Field(default=0)
    created_at: Optional[str] = Field(default_factory=lambda: datetime.datetime.utcnow().isoformat())


# Create tables on startup (idempotent)
try:
    SQLModel.metadata.create_all(engine)
except Exception:
    pass


def _verify_internal(request: Request):
    expected = os.getenv("ORCHESTRATOR_INTERNAL_TOKEN", "local-dev-internal-token")
    token = request.headers.get("X-Internal-Token", "") or request.query_params.get("token", "")
    if token != expected:
        raise HTTPException(status_code=401, detail="Invalid internal token")


class KBCreateRequest(BaseModel):
    name: str
    description: Optional[str] = None


class SourceCreateRequest(BaseModel):
    name: str
    content: str
    type: str = "text"


@app.get("/internal/knowledge-bases")
async def list_knowledge_bases(request: Request):
    _verify_internal(request)
    with Session(engine) as db:
        bases = db.exec(select(KnowledgeBase).order_by(KnowledgeBase.created_at.desc())).all()
        result = []
        for kb in bases:
            count = len(db.exec(select(KnowledgeSource).where(KnowledgeSource.kb_id == kb.id)).all())
            result.append({
                "id": kb.id,
                "name": kb.name,
                "description": kb.description,
                "source_count": count,
                "created_at": kb.created_at,
            })
    return {"knowledge_bases": result}


@app.post("/internal/knowledge-bases")
async def create_knowledge_base(request: Request, payload: KBCreateRequest):
    _verify_internal(request)
    kb = KnowledgeBase(name=payload.name.strip(), description=(payload.description or "").strip())
    with Session(engine) as db:
        db.add(kb)
        db.commit()
        db.refresh(kb)
    return {"id": kb.id, "name": kb.name, "description": kb.description, "source_count": 0, "created_at": kb.created_at}


@app.delete("/internal/knowledge-bases/{kb_id}")
async def delete_knowledge_base(kb_id: str, request: Request):
    _verify_internal(request)
    with Session(engine) as db:
        kb = db.get(KnowledgeBase, kb_id)
        if not kb:
            raise HTTPException(status_code=404, detail="Not found")
        # delete sources first
        sources = db.exec(select(KnowledgeSource).where(KnowledgeSource.kb_id == kb_id)).all()
        for s in sources:
            db.delete(s)
        db.delete(kb)
        db.commit()
    return {"deleted": kb_id}


@app.get("/internal/knowledge-bases/{kb_id}/sources")
async def list_kb_sources(kb_id: str, request: Request):
    _verify_internal(request)
    with Session(engine) as db:
        if not db.get(KnowledgeBase, kb_id):
            raise HTTPException(status_code=404, detail="KB not found")
        sources = db.exec(select(KnowledgeSource).where(KnowledgeSource.kb_id == kb_id).order_by(KnowledgeSource.created_at.desc())).all()
    return {"sources": [{"id": s.id, "name": s.name, "type": s.type, "content": s.content, "char_count": s.char_count, "created_at": s.created_at} for s in sources]}


@app.post("/internal/knowledge-bases/{kb_id}/sources")
async def add_kb_source(kb_id: str, request: Request, payload: SourceCreateRequest):
    _verify_internal(request)
    with Session(engine) as db:
        if not db.get(KnowledgeBase, kb_id):
            raise HTTPException(status_code=404, detail="KB not found")
        content = payload.content.strip()
        source = KnowledgeSource(
            kb_id=kb_id,
            name=payload.name.strip(),
            type=payload.type,
            content=content,
            char_count=len(content),
        )
        db.add(source)
        db.commit()
        db.refresh(source)
    return {"id": source.id, "name": source.name, "type": source.type, "char_count": source.char_count, "created_at": source.created_at}


@app.delete("/internal/knowledge-bases/{kb_id}/sources/{source_id}")
async def delete_kb_source(kb_id: str, source_id: str, request: Request):
    _verify_internal(request)
    with Session(engine) as db:
        source = db.get(KnowledgeSource, source_id)
        if not source or source.kb_id != kb_id:
            raise HTTPException(status_code=404, detail="Source not found")
        db.delete(source)
        db.commit()
    return {"deleted": source_id}


@app.get("/internal/knowledge-bases/{kb_id}/export")
async def export_kb_for_agent(kb_id: str, request: Request):
    """Returns all sources as a single text block — used by orchestrator to inject into agent prompt."""
    _verify_internal(request)
    with Session(engine) as db:
        kb = db.get(KnowledgeBase, kb_id)
        if not kb:
            raise HTTPException(status_code=404, detail="Not found")
        sources = db.exec(select(KnowledgeSource).where(KnowledgeSource.kb_id == kb_id)).all()
    text = f"=== {kb.name} ===\n\n"
    for s in sources:
        text += f"--- {s.name} ---\n{s.content}\n\n"
    return {"kb_id": kb_id, "name": kb.name, "text": text.strip(), "char_count": len(text)}
