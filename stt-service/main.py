"""
STT Microservice — Whisper-based Speech-to-Text
Supports Hindi, Marathi, English (auto-detect)
Exposes REST API for the orchestrator
"""

import io
import time
import logging
from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
import numpy as np
import soundfile as sf
import torch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="STT Service", version="1.0")

# ─── Load Whisper Model ───────────────────────────────────────────────────────
# Use faster-whisper for 4x speed vs original whisper
try:
    from faster_whisper import WhisperModel
    # medium model = best balance for Hindi/Marathi accuracy vs speed
    # Use "large-v3" for max accuracy at cost of more GPU memory
    model = WhisperModel(
        "medium",
        device="cuda" if torch.cuda.is_available() else "cpu",
        compute_type="float16" if torch.cuda.is_available() else "int8",
    )
    logger.info(f"Whisper medium loaded on {'GPU' if torch.cuda.is_available() else 'CPU'}")
except ImportError:
    # Fallback to standard whisper
    import whisper
    model = whisper.load_model("medium")
    logger.info("Standard Whisper loaded")

# Language mapping for Indian languages
LANGUAGE_MAP = {
    "hi": "hi",   # Hindi
    "mr": "mr",   # Marathi
    "en": "en",   # English
    "auto": None,  # Auto-detect
}

# ─── Transcription Endpoint ───────────────────────────────────────────────────
@app.post("/transcribe")
async def transcribe(
    audio: UploadFile = File(...),
    language: str = Form("hi"),
):
    t_start = time.time()

    try:
        # Read audio bytes
        audio_bytes = await audio.read()

        # Convert to numpy array
        audio_buffer = io.BytesIO(audio_bytes)
        audio_array, sample_rate = sf.read(audio_buffer, dtype="float32")

        # Resample to 16kHz if needed (Whisper requirement)
        if sample_rate != 16000:
            import librosa
            audio_array = librosa.resample(audio_array, orig_sr=sample_rate, target_sr=16000)

        # Mono conversion
        if len(audio_array.shape) > 1:
            audio_array = audio_array.mean(axis=1)

        # Transcribe
        lang = LANGUAGE_MAP.get(language, "hi")

        if hasattr(model, "transcribe") and not hasattr(model, "pipeline"):
            # faster-whisper
            segments, info = model.transcribe(
                audio_array,
                language=lang,
                beam_size=5,
                word_timestamps=False,
                vad_filter=True,          # built-in VAD
                vad_parameters={
                    "min_silence_duration_ms": 300,
                    "threshold": 0.5,
                },
            )
            text = " ".join(seg.text for seg in segments).strip()
            detected_lang = info.language
        else:
            # Standard whisper
            result = model.transcribe(
                audio_array,
                language=lang,
                fp16=torch.cuda.is_available(),
            )
            text = result["text"].strip()
            detected_lang = result.get("language", language)

        latency_ms = round((time.time() - t_start) * 1000)
        logger.info(f"STT: '{text[:50]}...' lang={detected_lang} latency={latency_ms}ms")

        return JSONResponse({
            "text": text,
            "language": detected_lang,
            "latency_ms": latency_ms,
            "audio_duration_sec": round(len(audio_array) / 16000, 2),
        })

    except Exception as e:
        logger.error(f"Transcription error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ─── Streaming Transcription (WebSocket) ─────────────────────────────────────
from fastapi import WebSocket
import asyncio

@app.websocket("/stream")
async def stream_transcribe(websocket: WebSocket):
    """
    Real-time streaming transcription for live call audio.
    Client sends raw PCM chunks, receives text as it's detected.
    """
    await websocket.accept()
    audio_buffer = np.array([], dtype=np.float32)
    min_chunk_sec = 0.5  # process every 500ms of accumulated audio

    try:
        while True:
            # Receive raw PCM bytes (16kHz, 16-bit, mono)
            raw = await websocket.receive_bytes()
            chunk = np.frombuffer(raw, dtype=np.int16).astype(np.float32) / 32768.0
            audio_buffer = np.concatenate([audio_buffer, chunk])

            # Process when we have enough audio
            if len(audio_buffer) >= 16000 * min_chunk_sec:
                if hasattr(model, "transcribe") and not hasattr(model, "pipeline"):
                    segments, _ = model.transcribe(
                        audio_buffer,
                        language="hi",
                        beam_size=3,
                        vad_filter=True,
                    )
                    text = " ".join(s.text for s in segments).strip()
                else:
                    result = model.transcribe(audio_buffer, language="hi")
                    text = result["text"].strip()

                if text:
                    await websocket.send_json({"text": text, "final": False})

                # Keep a small overlap for context
                audio_buffer = audio_buffer[-16000 * min_chunk_sec // 2:]

    except Exception as e:
        logger.error(f"Streaming STT error: {e}")
    finally:
        await websocket.close()


# ─── Health Check ─────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "status": "ok",
        "model": "whisper-medium",
        "device": "cuda" if torch.cuda.is_available() else "cpu",
        "languages": ["hi", "mr", "en"],
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002, workers=1)
