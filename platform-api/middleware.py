from __future__ import annotations

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from auth import decode_token


class TenantIsolationMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.method == "OPTIONS":
            return await call_next(request)
        public_paths = {"/health", "/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc", "/voice-catalog"}
        if request.url.path.startswith("/auth/") or request.url.path.startswith("/internal/") or request.url.path in public_paths:
            return await call_next(request)
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return JSONResponse(status_code=401, content={"detail": "Missing bearer token"})
        try:
            payload = decode_token(auth_header.split(" ", 1)[1])
        except Exception:
            return JSONResponse(status_code=401, content={"detail": "Invalid bearer token"})
        request.state.tenant_id = payload["tenant_id"]
        request.state.user_id = payload["sub"]
        request.state.role = payload["role"]
        response = await call_next(request)
        return response
