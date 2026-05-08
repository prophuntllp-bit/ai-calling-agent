"""
VAD Microservice — Voice Activity Detection
Uses Silero-VAD (1MB model, CPU, <5ms latency)
Tells orchestrator when caller is speaking vs silent
"""

import time
import logging
import numpy as np
import torch
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="VAD Service", version="1.0")

# ─── Load Silero VAD Model ────────────────────────────────────────────────────
model, utils = torch.hub.load(
    repo_or_dir="snakers4/silero-vad",
    model="silero_vad",
    force_reload=False,
    onnx=False,
)
(get_speech_timestamps, save_audio, read_audio, VADIterator, collect_chunks) = utils

# VAD iterator for streaming (maintains state across chunks)
vad_iterators = {}  # session_id → VADIterator

logger.info("Silero VAD loaded")


def _speech_confidence(audio_chunk: np.ndarray, sample_rate: int = 16000) -> float:
    num_samples = 512 if sample_rate == 16000 else 256
    if audio_chunk.size == 0:
        return 0.0
    if audio_chunk.size < num_samples:
        padded = np.zeros(num_samples, dtype=np.float32)
        padded[: audio_chunk.size] = audio_chunk
        audio_chunk = padded
    if audio_chunk.size == num_samples:
        tensor = torch.from_numpy(audio_chunk)
        with torch.no_grad():
            return float(model(tensor, sample_rate).item())

    max_confidence = 0.0
    for start in range(0, audio_chunk.size, num_samples):
        window = audio_chunk[start : start + num_samples]
        if window.size < num_samples:
            padded = np.zeros(num_samples, dtype=np.float32)
            padded[: window.size] = window
            window = padded
        tensor = torch.from_numpy(window)
        with torch.no_grad():
            max_confidence = max(max_confidence, float(model(tensor, sample_rate).item()))
        if max_confidence > 0.5:
            break
    return max_confidence

# ─── Single Chunk Detection ───────────────────────────────────────────────────
@app.post("/detect")
async def detect(request: Request):
    """
    Fast speech detection on a single audio chunk.
    Expects raw PCM bytes (16kHz, 16-bit, mono).
    Returns is_speech: true/false in <5ms.
    """
    t_start = time.time()
    raw = await request.body()

    if len(raw) < 512:
        return JSONResponse({"is_speech": False, "confidence": 0.0})

    try:
        # Convert raw PCM to float32 tensor
        audio_chunk = np.frombuffer(raw, dtype=np.int16).astype(np.float32) / 32768.0
        confidence = _speech_confidence(audio_chunk, 16000)

        is_speech = confidence > 0.5
        latency_ms = round((time.time() - t_start) * 1000, 1)

        return JSONResponse({
            "is_speech": is_speech,
            "confidence": round(confidence, 3),
            "latency_ms": latency_ms,
        })

    except Exception as e:
        logger.error(f"VAD error: {e}")
        return JSONResponse({"is_speech": False, "confidence": 0.0})


# ─── Segment Detection (full audio file) ─────────────────────────────────────
@app.post("/segments")
async def detect_segments(request: Request):
    """
    Detect all speech segments in a complete audio buffer.
    Returns list of {start, end} timestamps in seconds.
    Used for post-call analysis.
    """
    raw = await request.body()
    audio = np.frombuffer(raw, dtype=np.int16).astype(np.float32) / 32768.0
    tensor = torch.from_numpy(audio)

    timestamps = get_speech_timestamps(
        tensor,
        model,
        sampling_rate=16000,
        min_speech_duration_ms=250,
        min_silence_duration_ms=100,
        return_seconds=True,
    )

    total_speech = sum(t["end"] - t["start"] for t in timestamps)
    total_audio = len(audio) / 16000

    return {
        "segments": timestamps,
        "total_speech_sec": round(total_speech, 2),
        "total_audio_sec": round(total_audio, 2),
        "speech_ratio": round(total_speech / max(total_audio, 0.001), 3),
    }


# ─── Streaming VAD (WebSocket) ────────────────────────────────────────────────
from fastapi import WebSocket

@app.websocket("/stream/{session_id}")
async def stream_vad(websocket: WebSocket, session_id: str):
    """
    Streaming VAD for real-time barge-in detection.
    Maintains state across chunks within a session.
    """
    await websocket.accept()
    vad_iter = VADIterator(model, sampling_rate=16000, threshold=0.5)
    vad_iterators[session_id] = vad_iter

    try:
        while True:
            raw = await websocket.receive_bytes()
            audio_chunk = np.frombuffer(raw, dtype=np.int16).astype(np.float32) / 32768.0
            tensor = torch.from_numpy(audio_chunk)

            speech_dict = vad_iter(tensor, return_seconds=True)
            if speech_dict:
                await websocket.send_json(speech_dict)

    except Exception as e:
        logger.error(f"Streaming VAD error: {e}")
    finally:
        vad_iterators.pop(session_id, None)
        vad_iter.reset_states()
        await websocket.close()


@app.get("/health")
def health():
    return {
        "status": "ok",
        "model": "silero-vad",
        "device": "cpu",
        "latency_target_ms": 5,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, workers=2)  # 2 workers for CPU
