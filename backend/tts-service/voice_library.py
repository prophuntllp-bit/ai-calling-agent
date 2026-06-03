from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel


class VoiceProfile(BaseModel):
    voice_id: str
    language: str
    gender: str
    variant: int
    path: str
    client_id: str | None = None


class VoiceLibrary:
    def __init__(self, root: str = "/data/voices"):
        self.root = Path(root)
        self.defaults_root = self.root / "defaults"
        self.defaults_root.mkdir(parents=True, exist_ok=True)

    def get_voice(self, language: str, gender: str, variant: int = 1) -> str:
        voice_id = f"{language}_{gender}_{variant:02d}"
        target = self.defaults_root / f"{voice_id}.wav"
        return str(target)

    def get_client_voice(self, client_id: str, gender: str) -> str | None:
        candidate = self.root / f"{client_id}_{gender}.wav"
        return str(candidate) if candidate.exists() else None

    def register_voice(self, client_id: str, gender: str, audio: bytes) -> VoiceProfile:
        target = self.root / f"{client_id}_{gender}.wav"
        target.write_bytes(audio)
        return VoiceProfile(voice_id=target.stem, language="multi", gender=gender, variant=1, path=str(target), client_id=client_id)

    def list_available_voices(self, language: str | None = None) -> list[VoiceProfile]:
        profiles = []
        for lang in ["hi", "mr", "en", "ta", "te", "kn", "ml", "bn", "gu", "pa"]:
            for gender in ["male", "female"]:
                for variant in [1, 2]:
                    voice_id = f"{lang}_{gender}_{variant:02d}"
                    if not language or language == lang:
                        profiles.append(VoiceProfile(voice_id=voice_id, language=lang, gender=gender, variant=variant, path=str(self.defaults_root / f"{voice_id}.wav")))
        for custom in self.root.glob("*.wav"):
            if custom.parent == self.defaults_root:
                continue
            stem = custom.stem
            parts = stem.split("_")
            gender = parts[-1] if parts else "female"
            profiles.append(VoiceProfile(voice_id=stem, language="multi", gender=gender, variant=1, path=str(custom), client_id="_".join(parts[:-1]) or stem))
        return profiles
