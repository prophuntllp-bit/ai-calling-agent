from __future__ import annotations

import io
import os
import signal
from contextlib import suppress

import httpx
import numpy as np
import soundfile as sf
from fastapi import FastAPI, File, Form, HTTPException, UploadFile, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from shared.logging import configure_logging
from shared.tracing import RequestTracingMiddleware


logger = configure_logging("stt-service")
app = FastAPI(title="STT Service", version="2.0.0")
app.add_middleware(RequestTracingMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)

LANGUAGE_MAP = {
    "hi": "hi",
    "mr": "mr",
    "en": "en",
    "ta": "ta",
    "te": "te",
    "kn": "kn",
    "ml": "ml",
    "bn": "bn",
    "gu": "gu",
    "pa": "pa",
    "auto": None,
}

shutdown_requested = False
STT_PROVIDER = os.getenv("STT_PROVIDER", "local")
STT_API_URL = os.getenv("STT_API_URL", "").rstrip("/")
STT_API_TOKEN = os.getenv("STT_API_TOKEN", "")
STT_API_TIMEOUT_SEC = float(os.getenv("STT_API_TIMEOUT_SEC", "300"))
WHISPER_MODEL_NAME = os.getenv("WHISPER_MODEL", "large-v3")

model = None
torch = None


def _get_local_model():
    global model, torch
    if model is not None:
        return model
    import torch as torch_module
    from faster_whisper import WhisperModel

    torch = torch_module
    model = WhisperModel(
        WHISPER_MODEL_NAME,
        device="cuda" if torch.cuda.is_available() else "cpu",
        compute_type="float16" if torch.cuda.is_available() else "int8",
    )
    return model


def _load_audio(audio_bytes: bytes) -> np.ndarray:
    buffer = io.BytesIO(audio_bytes)
    audio_array, sample_rate = sf.read(buffer, dtype="float32")
    if sample_rate != 16000:
        import librosa

        audio_array = librosa.resample(audio_array, orig_sr=sample_rate, target_sr=16000)
    if len(audio_array.shape) > 1:
        audio_array = audio_array.mean(axis=1)
    return audio_array


def _transcribe(audio_array: np.ndarray, language: str | None):
    local_model = _get_local_model()
    segments, info = local_model.transcribe(
        audio_array,
        language=language,
        beam_size=5,
        word_timestamps=False,
        vad_filter=True,
        vad_parameters={"min_silence_duration_ms": 300, "threshold": 0.45},
    )
    text = " ".join(segment.text for segment in segments).strip()
    return text, info


async def _remote_transcribe(audio_bytes: bytes, language: str | None = None) -> dict:
    if not STT_API_URL:
        raise HTTPException(status_code=500, detail="STT_API_URL is not configured")
    headers = {"Authorization": f"Bearer {STT_API_TOKEN}"} if STT_API_TOKEN else {}
    files = {"audio": ("audio.wav", audio_bytes, "audio/wav")}
    data = {"language": language or "auto"}
    async with httpx.AsyncClient(timeout=STT_API_TIMEOUT_SEC, follow_redirects=True) as client:
        response = await client.post(f"{STT_API_URL}/transcribe", files=files, data=data, headers=headers)
        response.raise_for_status()
    return response.json()


async def _remote_detect_language(audio_bytes: bytes) -> dict:
    if not STT_API_URL:
        raise HTTPException(status_code=500, detail="STT_API_URL is not configured")
    headers = {"Authorization": f"Bearer {STT_API_TOKEN}"} if STT_API_TOKEN else {}
    files = {"audio": ("audio.wav", audio_bytes, "audio/wav")}
    async with httpx.AsyncClient(timeout=STT_API_TIMEOUT_SEC, follow_redirects=True) as client:
        response = await client.post(f"{STT_API_URL}/detect-language", files=files, headers=headers)
        response.raise_for_status()
    return response.json()


@app.post("/transcribe")
async def transcribe(audio: UploadFile = File(...), language: str = Form("auto")):
    if shutdown_requested:
        raise HTTPException(status_code=503, detail="Service shutting down")
    audio_bytes = await audio.read()
    if not audio_bytes:
        raise HTTPException(status_code=400, detail="Empty audio payload")
    if STT_PROVIDER == "http":
        return JSONResponse(await _remote_transcribe(audio_bytes, LANGUAGE_MAP.get(language, None)))
    audio_array = _load_audio(audio_bytes)
    text, info = _transcribe(audio_array, LANGUAGE_MAP.get(language, None))
    return JSONResponse(
        {
            "text": text,
            "language": info.language,
            "language_confidence": info.language_probability,
            "audio_duration_sec": round(len(audio_array) / 16000, 2),
        }
    )


@app.post("/detect-language")
async def detect_language(audio: UploadFile = File(...)):
    audio_bytes = await audio.read()
    if STT_PROVIDER == "http":
        return await _remote_detect_language(audio_bytes)
    audio_array = _load_audio(audio_bytes)
    _, info = _transcribe(audio_array[: 16000 * 3], None)
    return {"language": info.language, "confidence": info.language_probability}


@app.websocket("/stream")
async def stream_transcribe(websocket: WebSocket):
    await websocket.accept()
    buffer = np.array([], dtype=np.float32)
    try:
        while True:
            raw = await websocket.receive_bytes()
            chunk = np.frombuffer(raw, dtype=np.int16).astype(np.float32) / 32768.0
            buffer = np.concatenate([buffer, chunk])
            if len(buffer) >= 16000:
                text, info = _transcribe(buffer, None)
                await websocket.send_json({"text": text, "language": info.language, "final": False})
                buffer = buffer[-8000:]
    except Exception:
        with suppress(Exception):
            await websocket.close()


@app.get("/health")
async def health():
    try:
        if STT_PROVIDER == "http":
            async with httpx.AsyncClient(timeout=min(STT_API_TIMEOUT_SEC, 60)) as client:
                response = await client.get(f"{STT_API_URL}/health")
                response.raise_for_status()
            payload = response.json()
            return {
                "status": "ok",
                "provider": "http",
                "model": payload.get("model", WHISPER_MODEL_NAME),
                "upstream": STT_API_URL,
                "inference_ready": True,
            }
        # Use a tiny non-silent probe so faster-whisper doesn't error on empty language candidates.
        probe = np.sin(np.linspace(0, 2 * np.pi * 220, 16000, dtype=np.float32)) * 0.001
        _, info = _transcribe(probe, "en")
        ready = bool(info is not None)
    except Exception as exc:
        logger.error("STT health probe failed", extra={"extra_fields": {"error": str(exc)}})
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return {
        "status": "ok",
        "provider": "local",
        "model": WHISPER_MODEL_NAME,
        "device": "cuda" if torch and torch.cuda.is_available() else "cpu",
        "languages": list(LANGUAGE_MAP.keys()),
        "inference_ready": ready,
    }


def _shutdown_handler(*_args):
    global shutdown_requested
    shutdown_requested = True


signal.signal(signal.SIGTERM, _shutdown_handler)
signal.signal(signal.SIGINT, _shutdown_handler)
