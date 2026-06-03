from __future__ import annotations

import json
import os
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from shared.logging import configure_logging
from shared.tracing import RequestTracingMiddleware

from document_processor import DocumentProcessor
from models import KnowledgeAddRequest, KnowledgeChunk, Project, QueryResponse
from rag import RagEngine


logger = configure_logging("knowledge-service")
app = FastAPI(title="Knowledge Service", version="1.0.0")
app.add_middleware(RequestTracingMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_DIR = Path(os.getenv("KNOWLEDGE_DATA_DIR", "/data/knowledge"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
PROJECTS: dict[str, Project] = {}
CHUNKS: dict[str, list[KnowledgeChunk]] = {}
processor = DocumentProcessor()
rag = RagEngine()


def _project_path(project_id: str) -> Path:
    return DATA_DIR / f"{project_id}.json"


async def _persist_project(project: Project):
    _project_path(project.id).write_text(project.model_dump_json(indent=2), encoding="utf-8")
    CHUNKS[project.id] = await rag.index_project(project)


def _load_existing():
    for path in DATA_DIR.glob("*.json"):
        project = Project(**json.loads(path.read_text(encoding="utf-8")))
        PROJECTS[project.id] = project


@app.on_event("startup")
async def startup():
    _load_existing()
    for project in PROJECTS.values():
        CHUNKS[project.id] = await rag.index_project(project)


@app.post("/projects")
async def upsert_project(project: Project):
    PROJECTS[project.id] = project
    await _persist_project(project)
    return {"status": "saved", "project_id": project.id}


@app.get("/projects")
async def list_projects():
    return {"projects": [project.model_dump(mode="json") for project in PROJECTS.values()]}


@app.get("/projects/{project_id}")
async def get_project(project_id: str):
    project = PROJECTS.get(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project.model_dump(mode="json")


@app.post("/projects/{project_id}/documents")
async def upload_document(project_id: str, file: UploadFile = File(...)):
    project = PROJECTS.get(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    content = await file.read()
    extracted_text = processor.extract_text(file.filename, content)
    serialized = processor.serialize_document(file.filename, extracted_text)
    doc_dir = DATA_DIR / project_id
    doc_dir.mkdir(parents=True, exist_ok=True)
    doc_path = doc_dir / file.filename
    doc_path.write_text(serialized, encoding="utf-8")
    chunk = KnowledgeChunk(id=f"{project_id}-doc-{file.filename}", project_id=project_id, section="document", text=serialized, embedding=await rag.embed(serialized))
    CHUNKS.setdefault(project_id, []).append(chunk)
    return {"status": "processed", "project_id": project_id, "file": file.filename}


@app.post("/projects/{project_id}/knowledge")
async def add_knowledge(project_id: str, request: KnowledgeAddRequest):
    if project_id not in PROJECTS:
        raise HTTPException(status_code=404, detail="Project not found")
    content_text = request.content if isinstance(request.content, str) else json.dumps(request.content)
    chunk = KnowledgeChunk(
        id=f"{project_id}-{request.section}-{len(CHUNKS.get(project_id, [])) + 1}",
        project_id=project_id,
        section=request.section,
        text=content_text,
        embedding=await rag.embed(content_text),
    )
    CHUNKS.setdefault(project_id, []).append(chunk)
    return {"status": "indexed", "chunk_id": chunk.id}


@app.get("/projects/{project_id}/query", response_model=QueryResponse)
async def query_project(project_id: str, q: str):
    project = PROJECTS.get(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    matches = await rag.rank(q, CHUNKS.get(project_id, []), top_k=3)
    return QueryResponse(project_id=project_id, query=q, matches=matches)


@app.get("/health")
async def health():
    try:
        sample = await rag.embed("project pricing and location")
        return {"status": "ok", "projects_loaded": len(PROJECTS), "embedding_ok": bool(sample)}
    except Exception as exc:
        logger.error("Health check failed", extra={"extra_fields": {"error": str(exc)}})
        raise HTTPException(status_code=500, detail=str(exc)) from exc
