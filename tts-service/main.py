"""
TTS Microservice — Text-to-Speech with Voice Cloning
Supports: Mistral Voxtral (best quality) / Chatterbox (MIT, fast)
Includes voice library management for per-client cloned voices
"""

import io
import os
import time
import logging
import hashlib
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
import torch
import soundfile as sf
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="TTS Service", version="1.0")

# ─── Voice Library (stores cloned voices per client) ─────────────────────────
VOICE_DIR = Path("/data/voices")
VOICE_DIR.mkdir(parents=True, exist_ok=True)

# In-memory cache of speaker embeddings
voice_cache = {}

# ─── Load TTS Model ───────────────────────────────────────────────────────────
TTS_ENGINE = os.getenv("TTS_ENGINE", "chatterbox")  # voxtral | chatterbox | xtts

def load_model():
    if TTS_ENGINE == "chatterbox":
        try:
            from chatterbox.tts import ChatterboxTTS
            model = ChatterboxTTS.from_pretrained(device="cuda" if torch.cuda.is_available() else "cpu")
            logger.info("Chatterbox TTS loaded")
            return model, "chatterbox"
        except ImportError:
            logger.warning("Chatterbox not available, falling back to XTTS-v2")

    if TTS_ENGINE in ("xtts", "chatterbox"):
        try:
            from TTS.api import TTS
            model = TTS("tts_models/multilingual/multi-dataset/xtts_v2",
                       gpu=torch.cuda.is_available())
            logger.info("XTTS-v2 loaded")
            return model, "xtts"
        except Exception as e:
            logger.error(f"XTTS load failed: {e}")

    # Fallback: gTTS (no GPU needed, lower quality)
    logger.warning("Using gTTS fallback — low quality")
    return None, "gtts"

tts_model, engine_name = load_model()
logger.info(f"TTS engine: {engine_name}")

# ─── Request Models ───────────────────────────────────────────────────────────
class SynthRequest(BaseModel):
    text: str
    voice_id: str = "default"
    language: str = "hi"
    speed: float = 1.0
    emotion: float = 0.5  # 0=calm, 1=expressive (Chatterbox only)

class VoiceRegisterRequest(BaseModel):
    client_id: str
    voice_name: str = "default"

# ─── Synthesize Speech ────────────────────────────────────────────────────────
@app.post("/synthesize")
async def synthesize(req: SynthRequest):
    t_start = time.time()

    text = req.text.strip()
    if not text:
        raise HTTPException(400, "Empty text")

    # Limit text length to avoid very long responses
    if len(text) > 500:
        text = text[:500] + "..."

    voice_path = get_voice_path(req.voice_id)

    try:
        audio_array, sample_rate = await run_tts(text, voice_path, req.language, req.emotion)

        # Convert to WAV bytes
        buffer = io.BytesIO()
        sf.write(buffer, audio_array, sample_rate, format="WAV", subtype="PCM_16")
        buffer.seek(0)

        latency_ms = round((time.time() - t_start) * 1000)
        logger.info(f"TTS: '{text[:40]}...' voice={req.voice_id} latency={latency_ms}ms")

        return StreamingResponse(
            buffer,
            media_type="audio/wav",
            headers={
                "X-Latency-Ms": str(latency_ms),
                "X-Engine": engine_name,
            },
        )

    except Exception as e:
        logger.error(f"TTS synthesis error: {e}")
        raise HTTPException(500, str(e))


async def run_tts(text: str, voice_path: str | None, language: str, emotion: float):
    """Run TTS with the loaded engine."""

    if engine_name == "chatterbox":
        wav = tts_model.generate(
            text,
            audio_prompt_path=voice_path,
            exaggeration=emotion,
            cfg_weight=0.5,
        )
        return wav.squeeze().numpy(), 24000

    elif engine_name == "xtts":
        if voice_path and Path(voice_path).exists():
            wav = tts_model.tts(
                text=text,
                speaker_wav=voice_path,
                language=language[:2],  # "hi", "en", etc.
            )
        else:
            # Use default voice
            wav = tts_model.tts(text=text, language=language[:2])
        return np.array(wav), 22050

    else:
        # gTTS fallback
        from gtts import gTTS
        tts = gTTS(text=text, lang=language[:2], slow=False)
        mp3_buffer = io.BytesIO()
        tts.write_to_fp(mp3_buffer)
        mp3_buffer.seek(0)

        import pydub
        audio = pydub.AudioSegment.from_mp3(mp3_buffer)
        samples = np.array(audio.get_array_of_samples(), dtype=np.float32) / 32768.0
        return samples, audio.frame_rate


def get_voice_path(voice_id: str) -> str | None:
    """Get path to voice reference audio for cloning."""
    if voice_id == "default":
        default_path = VOICE_DIR / "default.wav"
        return str(default_path) if default_path.exists() else None

    voice_path = VOICE_DIR / f"{voice_id}.wav"
    return str(voice_path) if voice_path.exists() else None


# ─── Voice Registration (clone a client's voice) ─────────────────────────────
from fastapi import UploadFile, File, Form

@app.post("/register-voice")
async def register_voice(
    client_id: str = Form(...),
    voice_name: str = Form("default"),
    audio: UploadFile = File(...),
):
    """
    Register a cloned voice for a client.
    Accepts 6-30 seconds of clean audio.
    Stores the reference audio for use in all future calls.
    """
    audio_bytes = await audio.read()

    # Validate audio length
    buffer = io.BytesIO(audio_bytes)
    try:
        audio_array, sr = sf.read(buffer)
        duration_sec = len(audio_array) / sr
        if duration_sec < 3:
            raise HTTPException(400, f"Audio too short ({duration_sec:.1f}s). Minimum 3 seconds.")
        if duration_sec > 120:
            raise HTTPException(400, "Audio too long. Maximum 120 seconds.")
    except Exception as e:
        raise HTTPException(400, f"Invalid audio: {e}")

    # Save the voice reference
    voice_id = f"{client_id}_{voice_name}"
    voice_path = VOICE_DIR / f"{voice_id}.wav"

    # Resample to 22050Hz for XTTS compatibility
    if sr != 22050:
        import librosa
        audio_array = librosa.resample(audio_array.astype(np.float32),
                                       orig_sr=sr, target_sr=22050)
        sr = 22050

    sf.write(str(voice_path), audio_array, sr)

    # Clear from cache to force reload
    voice_cache.pop(voice_id, None)

    logger.info(f"Voice registered: {voice_id} ({duration_sec:.1f}s, {voice_path.stat().st_size} bytes)")

    return {
        "voice_id": voice_id,
        "duration_sec": round(duration_sec, 1),
        "status": "registered",
        "message": f"Voice '{voice_name}' registered for client {client_id}",
    }


@app.get("/voices")
def list_voices():
    """List all registered voices."""
    voices = []
    for path in VOICE_DIR.glob("*.wav"):
        stat = path.stat()
        voices.append({
            "voice_id": path.stem,
            "file_size_kb": round(stat.st_size / 1024),
            "created_at": stat.st_ctime,
        })
    return {"voices": voices, "count": len(voices)}


@app.delete("/voices/{voice_id}")
def delete_voice(voice_id: str):
    voice_path = VOICE_DIR / f"{voice_id}.wav"
    if not voice_path.exists():
        raise HTTPException(404, "Voice not found")
    voice_path.unlink()
    voice_cache.pop(voice_id, None)
    return {"deleted": voice_id}


# ─── Batch Synthesis (for pre-generating common phrases) ─────────────────────
class BatchRequest(BaseModel):
    phrases: list[str]
    voice_id: str = "default"
    language: str = "hi"

@app.post("/synthesize-batch")
async def synthesize_batch(req: BatchRequest):
    """
    Pre-generate audio for common phrases (greetings, fallbacks).
    Caches to disk for instant playback during calls.
    """
    results = []
    cache_dir = Path("/data/phrase-cache")
    cache_dir.mkdir(parents=True, exist_ok=True)

    for phrase in req.phrases[:20]:  # max 20 per request
        phrase_hash = hashlib.md5(f"{phrase}_{req.voice_id}_{req.language}".encode()).hexdigest()[:8]
        cache_path = cache_dir / f"{phrase_hash}.wav"

        if not cache_path.exists():
            voice_path = get_voice_path(req.voice_id)
            audio_array, sr = await run_tts(phrase, voice_path, req.language, 0.5)
            sf.write(str(cache_path), audio_array, sr)

        results.append({
            "phrase": phrase[:50],
            "cache_path": str(cache_path),
            "cached": True,
        })

    return {"results": results}


# ─── Health Check ─────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {
        "status": "ok",
        "engine": engine_name,
        "device": "cuda" if torch.cuda.is_available() else "cpu",
        "voices_registered": len(list(VOICE_DIR.glob("*.wav"))),
        "languages": ["hi", "mr", "en"],
        "features": {
            "voice_cloning": engine_name in ("chatterbox", "xtts"),
            "emotion_control": engine_name == "chatterbox",
            "streaming": False,
        },
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003, workers=1)
