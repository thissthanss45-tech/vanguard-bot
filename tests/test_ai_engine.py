import ai_engine


def _market_stub():
    return {
        "symbol": "BTC-USD",
        "data_lag_human": "10 мин",
        "last_candle_utc": "2026-03-01 12:00 UTC",
        "current_price": 65000.0,
        "change_pct_1d": 2.3,
        "sma_20": 64000.0,
        "sma_50": 62000.0,
        "ema_200": 58000.0,
        "rsi_14": 58.0,
        "atr_14": 1200.0,
        "adx_14": 24.0,
        "annualized_volatility_pct": 55.0,
        "low_20d": 60000.0,
        "high_20d": 67000.0,
        "corr_with_spy_60d": 0.36,
        "corr_with_btc_60d": 1.0,
        "forecast_3d": [
            {"day": 1, "bullish_probability": 62, "bearish_probability": 38, "bias": "Бычий"},
            {"day": 2, "bullish_probability": 60, "bearish_probability": 40, "bias": "Бычий"},
            {"day": 3, "bullish_probability": 57, "bearish_probability": 43, "bias": "Нейтральный"},
        ],
        "backtest": {"trades": 12, "win_rate": 58.3, "total_return_pct": 14.2},
        "rule_forecast": {
            "bias": "Бычий",
            "bullish_probability": 62,
            "bearish_probability": 38,
            "action": "Входить частями",
            "confidence": "Средняя",
            "regime": "Тренд",
            "trade_allowed": True,
            "gate_reason": "",
        },
    }


def test_get_ai_prediction_fallback_when_all_providers_fail(monkeypatch):
    monkeypatch.setattr(ai_engine, "_provider_chain", lambda _: ["deepseek", "groq"])
    monkeypatch.setattr(ai_engine, "DEEPSEEK_STRICT", False)
    monkeypatch.setattr(ai_engine, "DEEPSEEK_REQUIRE_SUCCESS", False)

    def fail_provider(provider, prompt):
        raise RuntimeError(f"{provider} unavailable")

    monkeypatch.setattr(ai_engine, "_safe_call_provider", fail_provider)

    result = ai_engine.get_ai_prediction(_market_stub(), provider="deepseek", risk_profile="balanced")

    assert "Ошибка AI-анализа" in result
    assert "локальный прогноз" in result


def test_get_ai_prediction_deepseek_strict_no_fallback(monkeypatch):
    monkeypatch.setattr(ai_engine, "DEEPSEEK_STRICT", True)
    monkeypatch.setattr(ai_engine, "DEEPSEEK_REQUIRE_SUCCESS", True)
    monkeypatch.setattr(ai_engine, "_provider_chain", lambda _: ["deepseek"])

    called = []

    def fail_provider(provider, prompt):
        called.append(provider)
        raise RuntimeError("Request timed out")

    monkeypatch.setattr(ai_engine, "_safe_call_provider", fail_provider)

    result = ai_engine.get_ai_prediction(_market_stub(), provider="deepseek", risk_profile="balanced")

    assert called == ["deepseek"]
    assert "fallback отключён" in result
    assert "локальный прогноз" not in result


def test_analyze_news_success_with_mocked_provider(monkeypatch):
    monkeypatch.setattr(ai_engine, "_provider_chain", lambda _: ["groq"])
    monkeypatch.setattr(ai_engine, "_safe_call_provider", lambda provider, prompt: "OK NEWS SUMMARY")

    result = ai_engine.analyze_news("Positive earnings and strong guidance", provider="groq")

    assert result == "OK NEWS SUMMARY"
