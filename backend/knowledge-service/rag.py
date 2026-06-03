from __future__ import annotations

import math
import os
from dataclasses import dataclass
import logging

import httpx

from models import KnowledgeChunk, Project


logger = logging.getLogger("knowledge-service.rag")


def _normalize(values: list[float]) -> list[float]:
    norm = math.sqrt(sum(value * value for value in values)) or 1.0
    return [value / norm for value in values]


def _cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right:
        return 0.0
    return sum(a * b for a, b in zip(_normalize(left), _normalize(right)))


@dataclass
class RagEngine:
    provider: str = os.getenv("EMBEDDING_PROVIDER", "ollama")
    ollama_url: str = os.getenv("OLLAMA_URL", "http://llm:11434")
    api_url: str = os.getenv("EMBEDDING_API_URL", "")
    api_token: str = os.getenv("EMBEDDING_API_TOKEN", "")
    model: str = os.getenv("EMBEDDING_MODEL", "nomic-embed-text")

    async def embed(self, text: str) -> list[float]:
        try:
            if self.provider == "http" and self.api_url:
                headers = {"Authorization": f"Bearer {self.api_token}"} if self.api_token else {}
                async with httpx.AsyncClient(timeout=60) as client:
                    response = await client.post(
                        self.api_url,
                        json={"model": self.model, "text": text},
                        headers=headers,
                    )
                    response.raise_for_status()
                body = response.json()
                if "embedding" in body:
                    return body.get("embedding", [])
                embeddings = body.get("embeddings", [])
                if embeddings:
                    return embeddings[0]
                return []

            async with httpx.AsyncClient(timeout=30) as client:
                # Newer Ollama versions prefer /api/embed while older ones used /api/embeddings.
                for path, payload_key in (
                    ("/api/embed", "input"),
                    ("/api/embeddings", "prompt"),
                ):
                    response = await client.post(
                        f"{self.ollama_url}{path}",
                        json={"model": self.model, payload_key: text},
                    )
                    if response.status_code == 404:
                        continue
                    response.raise_for_status()
                    body = response.json()
                    if "embedding" in body:
                        return body.get("embedding", [])
                    embeddings = body.get("embeddings", [])
                    if embeddings:
                        return embeddings[0]
                    return []
        except httpx.HTTPError as exc:
            logger.warning("Embedding request failed; falling back to empty embedding", extra={"error": str(exc)})
            return []
        return []

    def chunk_project(self, project: Project) -> list[tuple[str, str]]:
        chunks = [
            ("overview", f"{project.name} by {project.developer} in {project.location}, {project.city}. RERA {project.rera_number or 'not available'}."),
            ("pricing", " | ".join(f"{cfg.type}: {cfg.carpet_area_sqft} sqft at {cfg.price_lakh} lakh ({cfg.price_per_sqft}/sqft)" for cfg in project.configurations)),
            ("amenities", ", ".join(project.amenities)),
            ("location", " | ".join(f"{item.place} is {item.distance_km} km away in about {item.travel_time_min} minutes" for item in project.location_advantages)),
            ("usp", " | ".join(project.usp)),
            ("site_visit", f"Visit at {project.site_visit_info.address}; days: {', '.join(project.site_visit_info.available_days)}; timings: {project.site_visit_info.timings}."),
        ]
        for objection in project.common_objections:
            chunks.append(("objection", f"{objection.objection}. Hindi: {objection.response_hi or ''} English: {objection.response_en or ''} Marathi: {objection.response_mr or ''}".strip()))
        return chunks

    async def index_project(self, project: Project) -> list[KnowledgeChunk]:
        chunks: list[KnowledgeChunk] = []
        for idx, (section, text) in enumerate(self.chunk_project(project), start=1):
            embedding = await self.embed(text)
            chunks.append(KnowledgeChunk(id=f"{project.id}-{section}-{idx}", project_id=project.id, section=section, text=text, embedding=embedding))
        return chunks

    async def rank(self, query: str, chunks: list[KnowledgeChunk], top_k: int = 3) -> list[KnowledgeChunk]:
        query_embedding = await self.embed(query)
        scored = sorted(chunks, key=lambda chunk: _cosine_similarity(query_embedding, chunk.embedding), reverse=True)
        return scored[:top_k]
