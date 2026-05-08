from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np


class IndicTTSFallback:
    def __init__(self, cache_dir: str = "/data/voices/indic-cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _voice_seed(self, language: str, gender: str) -> int:
        digest = hashlib.md5(f"{language}:{gender}".encode()).hexdigest()
        return int(digest[:8], 16)

    def synthesize(self, text: str, language: str, gender: str, emotion: str) -> tuple[np.ndarray, int]:
        seed = self._voice_seed(language, gender)
        sample_rate = 22050
        duration = max(1.0, min(6.0, len(text) / 18.0))
        x = np.linspace(0, duration, int(sample_rate * duration), endpoint=False)
        base = 170 + (seed % 80)
        emotion_shift = {"neutral": 0, "warm": 20, "excited": 45, "empathetic": -15, "professional": 10}.get(emotion, 0)
        tone = np.sin(2 * np.pi * (base + emotion_shift) * x) * 0.15
        envelope = np.linspace(0.2, 1.0, tone.shape[0])
        return (tone * envelope).astype(np.float32), sample_rate
