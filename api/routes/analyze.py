"""GET /api/v1/forecast/{ticker}   — rule-based анализ (все тиры)
GET /api/v1/analyze/{ticker}    — + AI объяснение (только pro/enterprise)
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

# vanguard_bot в Python path при запуске из корня проекта
_BOT_ROOT = Path(__file__).resolve().parents[2]
if str(_BOT_ROOT) not in sys.path:
    sys.path.insert(0, str(_BOT_ROOT))

from api.auth import get_current_key, require_tier
from api.database import get_db
from api.models import ApiKey
from api.rate_limiter import check_rate_limit, log_usage
from api.schemas import AnalyzeResponse, ForecastResponse

router = APIRouter(prefix="/analyze", tags=["analysis"])


def _get_forecast_data(ticker: str) -> dict:
    """Загружаем рыночные данные + rule-based прогноз."""
    try:
        from data_provider import build_rule_based_forecast, get_market_data
    except ImportError as e:
        raise HTTPException(status_code=503, detail=f"data_provider unavailable: {e}")

    market_data = get_market_data(ticker.upper())
    if not market_data or market_data.get("error"):
        raise HTTPException(
            status_code=404,
            detail=f"Ticker '{ticker}' not found or data unavailable.",
        )
    rule = build_rule_based_forecast(market_data)
    return {"market": market_data, "rule": rule}


def _build_forecast_response(ticker: str, data: dict, latency_ms: int) -> ForecastResponse:
    m = data["market"]
    r = data["rule"]
    return ForecastResponse(
        ticker=ticker.upper(),
        instrument_name=m.get("instrument_name", ticker),
        current_price=float(m.get("current_price", 0)),
        change_pct_1d=float(m.get("change_pct_1d", 0)),
        sma_20=m.get("sma_20"),
        sma_50=m.get("sma_50"),
        ema_200=m.get("ema_200"),
        rsi_14=m.get("rsi_14"),
        atr_14=m.get("atr_14"),
        adx_14=m.get("adx_14"),
        annualized_volatility_pct=m.get("annualized_volatility_pct"),
        forecast_bias=r.get("bias", "neutral"),
        forecast_bull_pct=float(r.get("bull", 50)),
        forecast_bear_pct=float(r.get("bear", 50)),
        forecast_action=r.get("action", "hold"),
        forecast_confidence=r.get("confidence", "low"),
        forecast_regime=r.get("regime", "unknown"),
        trade_allowed=bool(r.get("trade_allowed", False)),
        gate_reason=str(r.get("gate_reason", "")),
        data_lag_human=str(m.get("data_lag_human", "н/д")),
        latency_ms=latency_ms,
    )


@router.get(
    "/forecast/{ticker}",
    response_model=ForecastResponse,
    summary="Rule-based forecast (все тиры)",
    description=(
        "Технический анализ и rule-based прогноз тикера.\n\n"
        "Доступно для всех тиров (free, pro, enterprise).\n\n"
        "Free: 10 запросов/день. Pro: 200/день."
    ),
)
def get_forecast(
    ticker: str,
    key: ApiKey = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    t0 = time.perf_counter()
    try:
        data = _get_forecast_data(ticker)
        latency_ms = int((time.perf_counter() - t0) * 1000)
        result = _build_forecast_response(ticker, data, latency_ms)
        log_usage(db, key, f"GET /forecast/{ticker}", ticker.upper(), 200, latency_ms)
        return result
    except HTTPException as exc:
        latency_ms = int((time.perf_counter() - t0) * 1000)
        log_usage(db, key, f"GET /forecast/{ticker}", ticker.upper(), exc.status_code, latency_ms, str(exc.detail))
        raise


@router.get(
    "/{ticker}",
    response_model=AnalyzeResponse,
    summary="Full AI analysis (pro/enterprise)",
    description=(
        "Технический анализ + AI объяснение (DeepSeek/Groq/Claude).\n\n"
        "**Требует tier: pro или enterprise.**\n\n"
        "Pro: 200 запросов/день. Enterprise: без лимита."
    ),
)
def get_analyze(
    ticker: str,
    provider: str = Query(default="deepseek", pattern="^(deepseek|groq|claude)$"),
    risk_profile: str = Query(default="balanced", pattern="^(conservative|balanced|aggressive)$"),
    key: ApiKey = Depends(require_tier("pro")),
    _rate: ApiKey = Depends(check_rate_limit),
    db: Session = Depends(get_db),
):
    t0 = time.perf_counter()
    try:
        data = _get_forecast_data(ticker)

        # AI анализ
        try:
            from ai_engine import get_ai_prediction
            ai_text = get_ai_prediction(data["market"], provider=provider, risk_profile=risk_profile)
            ai_provider = provider
        except Exception as ai_err:
            ai_text = f"AI analysis unavailable: {ai_err}"
            ai_provider = "unavailable"

        # Новости (опционально)
        news_summary = None
        try:
            from news_provider import get_ticker_news_payload
            news_payload = get_ticker_news_payload(ticker.upper())
            news_summary = news_payload.get("text", "")
        except Exception:
            pass

        latency_ms = int((time.perf_counter() - t0) * 1000)
        base = _build_forecast_response(ticker, data, latency_ms)
        result = AnalyzeResponse(
            **base.model_dump(),
            ai_analysis=ai_text,
            ai_provider=ai_provider,
            news_summary=news_summary,
        )
        log_usage(db, key, f"GET /analyze/{ticker}", ticker.upper(), 200, latency_ms)
        return result
    except HTTPException as exc:
        latency_ms = int((time.perf_counter() - t0) * 1000)
        log_usage(db, key, f"GET /analyze/{ticker}", ticker.upper(), exc.status_code, latency_ms, str(exc.detail))
        raise
