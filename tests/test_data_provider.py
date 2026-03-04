import pandas as pd

import data_provider


class DummyTicker:
    def __init__(self):
        self.info = {
            "longName": "Test Asset",
            "quoteType": "EQUITY",
            "exchange": "TESTEX",
            "currency": "USD",
        }


def _build_hist(rows: int = 260) -> pd.DataFrame:
    idx = pd.date_range("2025-01-01", periods=rows, freq="D", tz="UTC")
    close = pd.Series([100 + i * 0.2 for i in range(rows)], index=idx)
    df = pd.DataFrame(
        {
            "Open": close * 0.995,
            "High": close * 1.01,
            "Low": close * 0.99,
            "Close": close,
            "Volume": [1_000_000 + (i % 10) * 1000 for i in range(rows)],
        },
        index=idx,
    )
    return df


def test_build_rule_based_forecast_shape():
    sample = {
        "current_price": 120.0,
        "ema_200": 100.0,
        "sma_20": 118.0,
        "sma_50": 110.0,
        "rsi_14": 55.0,
        "change_pct_1d": 1.8,
        "volume_ratio": 1.25,
        "annualized_volatility_pct": 42.0,
        "adx_14": 26.0,
        "corr_with_spy_60d": 0.42,
    }

    result = data_provider.build_rule_based_forecast(sample)

    assert "bullish_probability" in result
    assert "bearish_probability" in result
    assert result["bullish_probability"] + result["bearish_probability"] == 100
    assert isinstance(result["score_breakdown"], dict)


def test_get_market_data_with_mocked_download(monkeypatch):
    hist = _build_hist()

    def fake_download(symbol: str, period: str, interval: str):
        if symbol in {"SPY", "BTC-USD"}:
            return hist.copy()
        return hist.copy()

    monkeypatch.setattr(data_provider, "_download_history", fake_download)
    monkeypatch.setattr(data_provider.yf, "Ticker", lambda _: DummyTicker())

    result = data_provider.get_market_data("AAPL")

    assert result is not None
    assert result["symbol"] == "AAPL"
    assert "rule_forecast" in result
    assert "forecast_3d" in result
    assert len(result["forecast_3d"]) == 3
    assert "backtest" in result
    assert "corr_with_spy_60d" in result
    assert "corr_with_btc_60d" in result
