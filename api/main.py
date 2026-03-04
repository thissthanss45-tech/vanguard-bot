"""Vanguard Bot — Public SaaS API.

Запуск (dev):
    cd /root/vanguard_bot
    uvicorn api.main:app --reload --host 0.0.0.0 --port 8090

Документация:
    http://localhost:8090/docs   (Swagger UI)
    http://localhost:8090/redoc

Переменные окружения:
    API_DATABASE_URL   sqlite:///./vanguard_api.db  (или postgresql://...)
    API_ADMIN_KEY      мастер-ключ для создания других ключей
    STRIPE_WEBHOOK_SECRET  whsec_... (опционально)
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.database import init_db
from api.routes.analyze import router as analyze_router
from api.routes.keys import router as keys_router
from api.routes.webhook import router as webhook_router
from api.schemas import ErrorDetail, HealthResponse

logger = logging.getLogger(__name__)

API_VERSION = "v1"
API_PREFIX = f"/api/{API_VERSION}"


def create_app() -> FastAPI:
    app = FastAPI(
        title="Vanguard Bot — Market Analysis API",
        description=(
            "REST API для технического анализа финансовых тикеров (акции, крипта, форекс, сырьё).\n\n"
            "## Аутентификация\n\n"
            "Передавай ключ в заголовке:\n"
            "```\nX-API-Key: vgd_your_api_key\n```\n\n"
            "## Тиры\n\n"
            "| Тир | Запросов/день | Получить |\n"
            "|---|---|---|\n"
            "| **free** | 10 | /api/v1/keys (admin) |\n"
            "| **pro** | 200 | $29/month |\n"
            "| **enterprise** | ∞ | $149/month |\n\n"
            "## Endpoints\n\n"
            "- `GET /api/v1/analyze/forecast/{ticker}` — технический анализ *(все тиры)*\n"
            "- `GET /api/v1/analyze/{ticker}` — полный AI анализ *(pro/enterprise)*\n"
            "- `GET /api/v1/keys/me` — инфо о своём ключе\n"
            "- `GET /api/v1/keys/me/usage` — история запросов\n"
        ),
        version="1.3.0",
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # ─── CORS ─────────────────────────────────────────────────────────────────
    import os
    cors_origins = os.getenv("API_CORS_ORIGINS", "*").split(",")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[o.strip() for o in cors_origins],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ─── Request timing middleware ──────────────────────────────────────────────
    @app.middleware("http")
    async def add_process_time_header(request: Request, call_next):
        t0 = time.perf_counter()
        response = await call_next(request)
        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        response.headers["X-Process-Time-Ms"] = str(elapsed_ms)
        return response

    # ─── Global exception handler ──────────────────────────────────────────────
    @app.exception_handler(HTTPException)
    async def http_exception_handler(request: Request, exc: HTTPException):
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": exc.detail, "detail": None},
            headers=getattr(exc, "headers", {}),
        )

    @app.exception_handler(Exception)
    async def generic_exception_handler(request: Request, exc: Exception):
        logger.exception("unhandled error", exc_info=exc)
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error", "detail": str(exc)},
        )

    # ─── Startup: инициализируем БД ──────────────────────────────────────────
    @app.on_event("startup")
    def on_startup():
        init_db()
        logger.info("✅ Vanguard API started, DB initialized")

    # ─── Routes ──────────────────────────────────────────────────────────────
    app.include_router(analyze_router, prefix=API_PREFIX)
    app.include_router(keys_router,    prefix=API_PREFIX)
    app.include_router(webhook_router, prefix=API_PREFIX)

    # ─── Health ──────────────────────────────────────────────────────────────
    @app.get(
        "/health",
        response_model=HealthResponse,
        tags=["system"],
        summary="Health check",
    )
    @app.get(
        f"{API_PREFIX}/health",
        response_model=HealthResponse,
        tags=["system"],
        summary="Health check (versioned)",
    )
    def health():
        return HealthResponse(timestamp=datetime.now(timezone.utc))

    # ─── Pricing page (info) ─────────────────────────────────────────────────
    @app.get("/pricing", tags=["system"], include_in_schema=False)
    def pricing():
        return {
            "tiers": {
                "free":       {"price_usd": 0,   "requests_per_day": 10,   "features": ["forecast"]},
                "pro":        {"price_usd": 29,  "requests_per_day": 200,  "features": ["forecast", "ai_analysis", "news"]},
                "enterprise": {"price_usd": 149, "requests_per_day": "∞",  "features": ["forecast", "ai_analysis", "news", "priority_support"]},
            },
            "upgrade": "Contact admin or use Stripe checkout link.",
        }

    return app


app = create_app()
