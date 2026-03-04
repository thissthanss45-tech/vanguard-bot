import os
import time

import pytest
from dotenv import load_dotenv
from openai import OpenAI

import ai_engine


load_dotenv(".env")


@pytest.mark.integration
@pytest.mark.skipif(not os.getenv("DEEPSEEK_API_KEY"), reason="DEEPSEEK_API_KEY is not set")
def test_deepseek_minimal_live_response():
    client = OpenAI(
        api_key=os.getenv("DEEPSEEK_API_KEY"),
        base_url="https://api.deepseek.com",
        timeout=float(os.getenv("AI_PROVIDER_TIMEOUT_SEC", "22")),
        max_retries=0,
    )

    started_at = time.monotonic()
    response = client.chat.completions.create(
        model=os.getenv("DEEPSEEK_MODEL", "deepseek-chat"),
        messages=[
            {"role": "system", "content": "Отвечай кратко."},
            {"role": "user", "content": "Ответь одним словом: OK"},
        ],
        temperature=0,
        max_tokens=8,
    )
    elapsed = time.monotonic() - started_at

    text = (response.choices[0].message.content or "").strip()

    assert text
    assert "OK" in text.upper()
    assert elapsed < 30


@pytest.mark.integration
@pytest.mark.skipif(not os.getenv("DEEPSEEK_API_KEY"), reason="DEEPSEEK_API_KEY is not set")
def test_ai_engine_deepseek_prediction_live():
    market_data = {
        "symbol": "AMZN",
        "data_lag_human": "11 ч",
        "last_candle_utc": "2026-03-02 05:00 UTC",
        "current_price": 206.79,
        "change_pct_1d": -1.53,
        "sma_20": 211.79,
        "sma_50": 226.28,
        "ema_200": 221.89,
        "rsi_14": 47.55,
        "atr_14": 5.64,
        "adx_14": 49.74,
        "annualized_volatility_pct": 31.6,
        "low_20d": 196.0,
        "high_20d": 246.35,
        "corr_with_spy_60d": 0.456,
        "corr_with_btc_60d": None,
        "rule_forecast": {
            "bias": "Медвежий",
            "bullish_probability": 17,
            "bearish_probability": 83,
            "action": "Сокращать риск / ждать",
            "confidence": "Высокая",
            "regime": "Тренд",
            "trade_allowed": True,
            "gate_reason": "",
        },
        "forecast_3d": [
            {"day": 1, "bullish_probability": 17, "bearish_probability": 83, "bias": "Медвежий"},
            {"day": 2, "bullish_probability": 21, "bearish_probability": 79, "bias": "Медвежий"},
            {"day": 3, "bullish_probability": 25, "bearish_probability": 75, "bias": "Медвежий"},
        ],
        "backtest": {"trades": 12, "win_rate": 51.2, "total_return_pct": 6.9},
    }

    result = ai_engine.get_ai_prediction(market_data, provider="deepseek", risk_profile="balanced")

    assert result
    assert "DeepSeek отсутствует" not in result
    assert "Request timed out" not in result
