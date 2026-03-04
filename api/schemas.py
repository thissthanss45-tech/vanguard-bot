"""Pydantic схемы для запросов / ответов API."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# ─── API Key ─────────────────────────────────────────────────────────────────

class ApiKeyCreate(BaseModel):
    label: str = Field(..., min_length=1, max_length=128)
    owner_email: str = Field(..., min_length=3, max_length=256)
    tier: str = Field(default="free", pattern="^(free|pro|enterprise)$")
    is_admin: bool = False


class ApiKeyResponse(BaseModel):
    id: int
    key: str
    label: str
    owner_email: str
    tier: str
    is_active: bool
    is_admin: bool
    daily_limit: int
    created_at: datetime
    expires_at: datetime | None = None

    model_config = {"from_attributes": True}


class ApiKeyInfo(BaseModel):
    """Публичная инфо по ключу (без самого ключа — только маскированный)."""
    id: int
    key_masked: str       # "vgd_abc***xyz"
    label: str
    tier: str
    is_active: bool
    daily_limit: int
    requests_today: int
    remaining_today: int
    created_at: datetime
    expires_at: datetime | None = None


# ─── Analysis ────────────────────────────────────────────────────────────────

class ForecastResponse(BaseModel):
    ticker: str
    instrument_name: str
    current_price: float
    change_pct_1d: float
    sma_20: float | None = None
    sma_50: float | None = None
    ema_200: float | None = None
    rsi_14: float | None = None
    atr_14: float | None = None
    adx_14: float | None = None
    annualized_volatility_pct: float | None = None
    forecast_bias: str        # "bullish" / "bearish" / "neutral"
    forecast_bull_pct: float
    forecast_bear_pct: float
    forecast_action: str
    forecast_confidence: str
    forecast_regime: str
    trade_allowed: bool
    gate_reason: str
    data_lag_human: str
    latency_ms: int


class AnalyzeResponse(ForecastResponse):
    """ForecastResponse + AI анализ (только pro/enterprise)."""
    ai_analysis: str
    ai_provider: str
    news_summary: str | None = None


class HealthResponse(BaseModel):
    status: str = "ok"
    version: str = "1.3.0"
    timestamp: datetime


# ─── Usage ────────────────────────────────────────────────────────────────────

class UsageEntry(BaseModel):
    endpoint: str
    ticker: str | None
    status_code: int
    latency_ms: int
    created_at: datetime

    model_config = {"from_attributes": True}


class UsageStatsResponse(BaseModel):
    requests_today: int
    requests_total: int
    daily_limit: int
    remaining_today: int
    recent: list[UsageEntry]


# ─── Error ────────────────────────────────────────────────────────────────────

class ErrorDetail(BaseModel):
    error: str
    detail: str | None = None
