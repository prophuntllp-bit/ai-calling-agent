from __future__ import annotations

import base64
import io
import os

import httpx
import numpy as np
import soundfile as sf
import torch
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from shared.logging import configure_logging
from shared.tracing import RequestTracingMiddleware

from emotion_mapper import map_emotion
from indic_tts import IndicTTSFallback
from voice_library import VoiceLibrary


logger = configure_logging("tts-service")
app = FastAPI(title="TTS Service", version="2.0.0")
app.add_middleware(RequestTracingMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)

voice_library = VoiceLibrary(os.getenv("VOICE_DIR", "/data/voices"))
indic_fallback = IndicTTSFallback()
TTS_PROVIDER = os.getenv("TTS_PROVIDER", "local")
TTS_API_URL = os.getenv("TTS_API_URL", "").rstrip("/")
TTS_API_TOKEN = os.getenv("TTS_API_TOKEN", "")
TTS_API_TIMEOUT_SEC = float(os.getenv("TTS_API_TIMEOUT_SEC", "180"))
SARVAM_API_URL = os.getenv("SARVAM_API_URL", "https://api.sarvam.ai").rstrip("/")
SARVAM_API_KEY = os.getenv("SARVAM_API_KEY", "")
SARVAM_TTS_MODEL = os.getenv("SARVAM_TTS_MODEL", "bulbul:v3")
SARVAM_TTS_SPEAKER = os.getenv("SARVAM_TTS_SPEAKER", "Priya")
SARVAM_TTS_LANGUAGE = os.getenv("SARVAM_TTS_LANGUAGE", "en-IN")
SARVAM_TTS_PACE = float(os.getenv("SARVAM_TTS_PACE", "1.0"))
SARVAM_TTS_SAMPLE_RATE = int(os.getenv("SARVAM_TTS_SAMPLE_RATE", "8000"))

# ── ElevenLabs ────────────────────────────────────────────────────────────────
ELEVENLABS_API_KEY = os.getenv("ELEVENLABS_API_KEY", "")
ELEVENLABS_API_URL = os.getenv("ELEVENLABS_API_URL", "https://api.elevenlabs.io").rstrip("/")
# eleven_turbo_v2_5: fastest, multilingual (Hindi supported)
ELEVENLABS_MODEL = os.getenv("ELEVENLABS_MODEL", "eleven_turbo_v2_5")
# Default Hindi female voice — Rachel works well for en-IN; set your preferred voice ID here
# Common multilingual voices: Aria (21m00Tcm4TlvDq8ikWAM), Rachel (21m00Tcm4TlvDq8ikWAM)
# For Hindi: use "Priya" equivalent or a custom cloned voice
ELEVENLABS_VOICE_ID = os.getenv("ELEVENLABS_VOICE_ID", "cgSgspJ2msm6clMCkdW9")
# pcm_8000 = raw 8kHz 16-bit little-endian PCM — zero resampling needed for EnableX
ELEVENLABS_OUTPUT_FORMAT = os.getenv("ELEVENLABS_OUTPUT_FORMAT", "pcm_8000")
ELEVENLABS_STABILITY = float(os.getenv("ELEVENLABS_STABILITY", "0.5"))
ELEVENLABS_SIMILARITY_BOOST = float(os.getenv("ELEVENLABS_SIMILARITY_BOOST", "0.75"))
ELEVENLABS_STYLE = float(os.getenv("ELEVENLABS_STYLE", "0.0"))
ELEVENLABS_SPEED = float(os.getenv("ELEVENLABS_SPEED", "1.0"))

# Voice map: gender → voice_id (override with env vars)
ELEVENLABS_VOICE_MAP = {
    "female": os.getenv("ELEVENLABS_VOICE_FEMALE", ELEVENLABS_VOICE_ID),
    "male":   os.getenv("ELEVENLABS_VOICE_MALE",   os.getenv("ELEVENLABS_VOICE_ID", "pNInz6obpgDQGcFmaJgB")),
}

SARVAM_VOICES = [
    {"voice_id": "priya", "language": "en-IN", "gender": "female", "variant": 1, "path": "sarvam://priya"},
    {"voice_id": "ritu", "language": "hi-IN", "gender": "female", "variant": 1, "path": "sarvam://ritu"},
    {"voice_id": "simran", "language": "pa-IN", "gender": "female", "variant": 1, "path": "sarvam://simran"},
    {"voice_id": "roopa", "language": "mr-IN", "gender": "female", "variant": 1, "path": "sarvam://roopa"},
    {"voice_id": "kavya", "language": "ta-IN", "gender": "female", "variant": 1, "path": "sarvam://kavya"},
    {"voice_id": "shreya", "language": "bn-IN", "gender": "female", "variant": 1, "path": "sarvam://shreya"},
    {"voice_id": "shubh", "language": "en-IN", "gender": "male", "variant": 1, "path": "sarvam://shubh"},
    {"voice_id": "rahul", "language": "hi-IN", "gender": "male", "variant": 1, "path": "sarvam://rahul"},
    {"voice_id": "anand", "language": "mr-IN", "gender": "male", "variant": 1, "path": "sarvam://anand"},
    {"voice_id": "vijay", "language": "te-IN", "gender": "male", "variant": 1, "path": "sarvam://vijay"},
]


class SynthRequest(BaseModel):
    text: str
    voice_id: str | None = None
    language: str = "hi"
    gender: str = "female"
    emotion: str | None = None
    context: dict | None = None


def generate_wave(text: str, language: str, gender: str, emotion: str) -> tuple[np.ndarray, int]:
    return indic_fallback.synthesize(text, language, gender, emotion)


def _normalize_language(language: str | None) -> str:
    value = (language or "").strip()
    if not value:
        return SARVAM_TTS_LANGUAGE
    if "-" in value:
        return value
    mapping = {
        "en": "en-IN",
        "hi": "hi-IN",
        "mr": "mr-IN",
        "bn": "bn-IN",
        "ta": "ta-IN",
        "te": "te-IN",
        "kn": "kn-IN",
        "ml": "ml-IN",
        "gu": "gu-IN",
        "pa": "pa-IN",
        "od": "od-IN",
    }
    return mapping.get(value.lower(), SARVAM_TTS_LANGUAGE)


def _select_sarvam_speaker(request: SynthRequest) -> str:
    voice_id = (request.voice_id or "").strip()
    if voice_id:
        return voice_id.split("://")[-1].split("/")[-1].lower()
    default = SARVAM_TTS_SPEAKER.strip() or "priya"
    return default.lower()


def _filter_sarvam_voices(language: str | None = None) -> list[dict]:
    normalized = _normalize_language(language)
    if not language:
        return list(SARVAM_VOICES)
    same_language = [voice for voice in SARVAM_VOICES if voice["language"] == normalized]
    return same_language or list(SARVAM_VOICES)


async def _remote_synthesize(request: SynthRequest, emotion: str):
    if not TTS_API_URL:
        raise HTTPException(status_code=500, detail="TTS_API_URL is not configured")
    headers = {"Authorization": f"Bearer {TTS_API_TOKEN}"} if TTS_API_TOKEN else {}
    payload = {
        "text": request.text[:500],
        "language": request.language,
        "gender": request.gender,
        "emotion": emotion,
        "voice_id": request.voice_id,
        "context": request.context or {},
    }
    async with httpx.AsyncClient(timeout=TTS_API_TIMEOUT_SEC) as client:
        response = await client.post(f"{TTS_API_URL}/synthesize", json=payload, headers=headers)
        response.raise_for_status()
    return response.content, response.headers.get("content-type", "audio/wav")


def _preprocess_tts_text(text: str, language: str) -> str:
    """
    Preprocess text before sending to Sarvam TTS.
    Replaces English brand names / mixed-script words with phonetic equivalents
    so the Hindi TTS voice doesn't mangle them.
    Only applied for Indic languages (not en-IN).
    """
    if language and language.startswith("en"):
        return text

    import re

    # Company / brand name fixes — phonetic Devanagari spellings
    brand_map = {
        r"\bProphunt\b": "प्रॉफ़हंट",
        r"\bprophunt\b": "प्रॉफ़हंट",
        r"\bPROPHUNT\b": "प्रॉफ़हंट",
        # Mahindra Citadel is usually fine but Citadel gets mangled
        r"\bCitadel\b": "सिटाडेल",
        r"\bcitadel\b": "सिटाडेल",
        # Common real-estate English terms in Hindi context
        r"\bsite visit\b": "साइट विज़िट",
        r"\bSite Visit\b": "साइट विज़िट",
        r"\bBHK\b": "बी एच के",
        r"\bCallback\b": "कॉलबैक",
        r"\bcallback\b": "कॉलबैक",
    }
    for pattern, replacement in brand_map.items():
        text = re.sub(pattern, replacement, text)
    return text


async def _elevenlabs_synthesize(request: SynthRequest):
    if not ELEVENLABS_API_KEY:
        raise HTTPException(status_code=500, detail="ELEVENLABS_API_KEY is not configured")

    # Pick voice by gender (or explicit voice_id)
    voice_id = (request.voice_id or "").strip()
    if voice_id and not voice_id.startswith("sarvam://"):
        selected_voice = voice_id
    else:
        selected_voice = ELEVENLABS_VOICE_MAP.get(request.gender, ELEVENLABS_VOICE_MAP["female"])

    text = request.text[:2500]
    payload = {
        "text": text,
        "model_id": ELEVENLABS_MODEL,
        "voice_settings": {
            "stability": ELEVENLABS_STABILITY,
            "similarity_boost": ELEVENLABS_SIMILARITY_BOOST,
            "style": ELEVENLABS_STYLE,
            "use_speaker_boost": True,
        },
        "speed": ELEVENLABS_SPEED,
    }
    headers = {
        "xi-api-key": ELEVENLABS_API_KEY,
        "Content-Type": "application/json",
        "Accept": "audio/mpeg" if ELEVENLABS_OUTPUT_FORMAT == "mp3_44100_128" else "application/octet-stream",
    }
    url = f"{ELEVENLABS_API_URL}/v1/text-to-speech/{selected_voice}?output_format={ELEVENLABS_OUTPUT_FORMAT}"

    async with httpx.AsyncClient(timeout=TTS_API_TIMEOUT_SEC) as client:
        response = await client.post(url, json=payload, headers=headers)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            detail = response.text
            raise HTTPException(status_code=exc.response.status_code, detail=detail) from exc

    audio_bytes = response.content
    if not audio_bytes:
        raise HTTPException(status_code=502, detail="ElevenLabs returned no audio data")

    # pcm_8000 → wrap in WAV header so downstream code can parse it uniformly
    if ELEVENLABS_OUTPUT_FORMAT == "pcm_8000":
        audio_bytes = _wrap_pcm_as_wav(audio_bytes, sample_rate=8000, channels=1, bits=16)
        content_type = "audio/wav"
    elif ELEVENLABS_OUTPUT_FORMAT.startswith("pcm_"):
        sr = int(ELEVENLABS_OUTPUT_FORMAT.split("_")[1])
        audio_bytes = _wrap_pcm_as_wav(audio_bytes, sample_rate=sr, channels=1, bits=16)
        content_type = "audio/wav"
    else:
        content_type = "audio/mpeg"

    return audio_bytes, content_type, selected_voice


def _wrap_pcm_as_wav(pcm_bytes: bytes, sample_rate: int = 8000, channels: int = 1, bits: int = 16) -> bytes:
    """Wrap raw PCM bytes in a minimal RIFF/WAV header."""
    import struct
    data_size = len(pcm_bytes)
    byte_rate = sample_rate * channels * bits // 8
    block_align = channels * bits // 8
    buf = io.BytesIO()
    buf.write(b"RIFF")
    buf.write(struct.pack("<I", 36 + data_size))
    buf.write(b"WAVE")
    buf.write(b"fmt ")
    buf.write(struct.pack("<I", 16))           # PCM chunk size
    buf.write(struct.pack("<H", 1))            # AudioFormat = PCM
    buf.write(struct.pack("<H", channels))
    buf.write(struct.pack("<I", sample_rate))
    buf.write(struct.pack("<I", byte_rate))
    buf.write(struct.pack("<H", block_align))
    buf.write(struct.pack("<H", bits))
    buf.write(b"data")
    buf.write(struct.pack("<I", data_size))
    buf.write(pcm_bytes)
    return buf.getvalue()


async def _sarvam_synthesize(request: SynthRequest):
    if not SARVAM_API_KEY:
        raise HTTPException(status_code=500, detail="SARVAM_API_KEY is not configured")
    lang = _normalize_language(request.language)
    processed_text = _preprocess_tts_text(request.text[:2500], lang)
    payload = {
        "inputs": [processed_text],
        "target_language_code": lang,
        "speaker": _select_sarvam_speaker(request),
        "model": SARVAM_TTS_MODEL,
        "pace": SARVAM_TTS_PACE,
        "sample_rate": SARVAM_TTS_SAMPLE_RATE,
        "enable_preprocessing": True,
    }
    headers = {
        "api-subscription-key": SARVAM_API_KEY,
        "Content-Type": "application/json",
    }
    async with httpx.AsyncClient(timeout=TTS_API_TIMEOUT_SEC) as client:
        response = await client.post(f"{SARVAM_API_URL}/text-to-speech", json=payload, headers=headers)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            detail = response.text
            raise HTTPException(status_code=exc.response.status_code, detail=detail) from exc
    data = response.json()
    audios = data.get("audios") or []
    if not audios:
        raise HTTPException(status_code=502, detail="Sarvam returned no audio data")
    return base64.b64decode("".join(audios)), "audio/wav"


@app.post("/synthesize")
async def synthesize(request: SynthRequest):
    if not request.text.strip():
        raise HTTPException(status_code=400, detail="Text is required")
    emotion = request.emotion or map_emotion(request.text, request.context)
    if TTS_PROVIDER == "elevenlabs":
        audio_bytes, content_type, voice_used = await _elevenlabs_synthesize(request)
        return StreamingResponse(
            io.BytesIO(audio_bytes),
            media_type=content_type,
            headers={
                "X-Emotion": emotion,
                "X-Language": _normalize_language(request.language),
                "X-TTS-Provider": "elevenlabs",
                "X-TTS-Voice": voice_used,
            },
        )
    if TTS_PROVIDER == "sarvam":
        audio_bytes, content_type = await _sarvam_synthesize(request)
        return StreamingResponse(
            io.BytesIO(audio_bytes),
            media_type=content_type,
            headers={
                "X-Emotion": emotion,
                "X-Language": _normalize_language(request.language),
                "X-TTS-Provider": "sarvam",
                "X-TTS-Voice": _select_sarvam_speaker(request),
            },
        )
    if TTS_PROVIDER == "http":
        audio_bytes, content_type = await _remote_synthesize(request, emotion)
        return StreamingResponse(
            io.BytesIO(audio_bytes),
            media_type=content_type,
            headers={"X-Emotion": emotion, "X-Language": request.language, "X-TTS-Provider": "http"},
        )
    audio, sample_rate = generate_wave(request.text[:500], request.language, request.gender, emotion)
    buffer = io.BytesIO()
    sf.write(buffer, audio, sample_rate, format="WAV", subtype="PCM_16")
    buffer.seek(0)
    return StreamingResponse(
        buffer,
        media_type="audio/wav",
        headers={"X-Emotion": emotion, "X-Language": request.language},
    )


@app.post("/register-voice")
async def register_voice(client_id: str = Form(...), voice_name: str = Form("default"), gender: str = Form("female"), audio: UploadFile = File(...)):
    payload = await audio.read()
    if not payload:
        raise HTTPException(status_code=400, detail="Audio is required")
    profile = voice_library.register_voice(f"{client_id}_{voice_name}", gender, payload)
    return profile.model_dump()


@app.get("/voices")
async def list_voices(language: str | None = None):
    local_voices = [voice.model_dump() for voice in voice_library.list_available_voices(language)]
    if TTS_PROVIDER == "elevenlabs":
        eleven_voices = [
            {"voice_id": ELEVENLABS_VOICE_MAP["female"], "language": "hi-IN", "gender": "female", "variant": 1, "path": f"elevenlabs://{ELEVENLABS_VOICE_MAP['female']}"},
            {"voice_id": ELEVENLABS_VOICE_MAP["male"],   "language": "hi-IN", "gender": "male",   "variant": 1, "path": f"elevenlabs://{ELEVENLABS_VOICE_MAP['male']}"},
        ]
        return {"voices": [*local_voices, *eleven_voices]}
    if TTS_PROVIDER == "sarvam":
        return {"voices": [*local_voices, *_filter_sarvam_voices(language)]}
    return {"voices": local_voices}


@app.get("/health")
async def health():
    if TTS_PROVIDER == "elevenlabs":
        return {
            "status": "ok",
            "provider": "elevenlabs",
            "engine": ELEVENLABS_MODEL,
            "upstream": ELEVENLABS_API_URL,
            "output_format": ELEVENLABS_OUTPUT_FORMAT,
            "voice_female": ELEVENLABS_VOICE_MAP["female"],
            "voice_male": ELEVENLABS_VOICE_MAP["male"],
            "sample_ready": bool(ELEVENLABS_API_KEY),
        }
    if TTS_PROVIDER == "sarvam":
        return {
            "status": "ok",
            "provider": "sarvam",
            "engine": SARVAM_TTS_MODEL,
            "upstream": SARVAM_API_URL,
            "voices_registered": len(_filter_sarvam_voices()),
            "sample_ready": bool(SARVAM_API_KEY),
        }
    if TTS_PROVIDER == "http":
        async with httpx.AsyncClient(timeout=min(TTS_API_TIMEOUT_SEC, 60)) as client:
            response = await client.get(f"{TTS_API_URL}/health")
            response.raise_for_status()
        payload = response.json()
        return {
            "status": "ok",
            "provider": "http",
            "engine": payload.get("engine", "modal"),
            "upstream": TTS_API_URL,
            "voices_registered": len(voice_library.list_available_voices()),
            "sample_ready": True,
        }
    sample, _ = generate_wave("health check", "en", "female", "neutral")
    return {
        "status": "ok",
        "provider": "local",
        "device": "cuda" if torch.cuda.is_available() else "cpu",
        "voices_registered": len(voice_library.list_available_voices()),
        "sample_ready": bool(sample.size),
    }
