import logging
from datetime import datetime, timezone

import pandas as pd
import yfinance as yf
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from backtesting import simple_backtest
from config import SETTINGS
from utils import format_lag

logger = logging.getLogger(__name__)


TICKER_FALLBACK_INFO = {
    "AAPL": {"name": "Apple Inc.", "type": "Акция"},
    "NVDA": {"name": "NVIDIA Corporation", "type": "Акция"},
    "TSLA": {"name": "Tesla, Inc.", "type": "Акция"},
    "MSFT": {"name": "Microsoft Corporation", "type": "Акция"},
    "AMZN": {"name": "Amazon.com, Inc.", "type": "Акция"},
    "GOOGL": {"name": "Alphabet Inc.", "type": "Акция"},
    "META": {"name": "Meta Platforms, Inc.", "type": "Акция"},
    "BTC-USD": {"name": "Bitcoin", "type": "Криптовалюта"},
    "ETH-USD": {"name": "Ethereum", "type": "Криптовалюта"},
    "SOL-USD": {"name": "Solana", "type": "Криптовалюта"},
    "XAUUSD=X": {"name": "Gold Spot", "type": "Форекс/металл"},
    "GC=F": {"name": "Gold Futures", "type": "Фьючерс"},
    "SI=F": {"name": "Silver Futures", "type": "Фьючерс"},
    "BZ=F": {"name": "Brent Crude Futures", "type": "Фьючерс"},
    "CL=F": {"name": "WTI Crude Oil Futures", "type": "Фьючерс"},
    "EURUSD=X": {"name": "EUR/USD", "type": "Форекс"},
    "GBPUSD=X": {"name": "GBP/USD", "type": "Форекс"},
}


def _humanize_quote_type(quote_type: str) -> str:
    mapping = {
        "EQUITY": "Акция",
        "ETF": "ETF",
        "MUTUALFUND": "Фонд",
        "CRYPTOCURRENCY": "Криптовалюта",
        "CURRENCY": "Форекс",
        "FUTURE": "Фьючерс",
        "INDEX": "Индекс",
    }
    return mapping.get((quote_type or "").upper(), "Инструмент")


def _extract_ticker_meta(ticker_symbol: str, ticker_obj) -> dict:
    fallback = TICKER_FALLBACK_INFO.get(ticker_symbol, {})

    name = fallback.get("name")
    instrument_type = fallback.get("type")
    exchange = None
    currency = None

    info = {}
    try:
        info = ticker_obj.info or {}
    except Exception:
        info = {}

    if not name:
        name = info.get("longName") or info.get("shortName") or ticker_symbol

    if not instrument_type:
        instrument_type = _humanize_quote_type(info.get("quoteType"))

    exchange = info.get("exchange") or info.get("fullExchangeName")
    currency = info.get("currency")

    description = f"{name} ({instrument_type})"
    extra_parts = [p for p in [exchange, currency] if p]
    if extra_parts:
        description = f"{description}, {' / '.join(extra_parts)}"

    return {
        "instrument_name": name,
        "instrument_type": instrument_type,
        "exchange": exchange,
        "currency": currency,
        "instrument_description": description,
    }


def _calc_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)

    avg_gain = gains.rolling(window=period).mean()
    avg_loss = losses.rolling(window=period).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    return 100 - (100 / (1 + rs))


def _calc_atr(df: pd.DataFrame, period: int = 14) -> float:
    high = df["High"]
    low = df["Low"]
    close = df["Close"]
    prev_close = close.shift(1)

    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.rolling(window=period).mean().iloc[-1]
    return float(atr) if pd.notna(atr) else 0.0


def _calc_adx(df: pd.DataFrame, period: int = 14) -> float:
    high = df["High"]
    low = df["Low"]
    close = df["Close"]

    up_move = high.diff()
    down_move = -low.diff()

    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    tr_components = pd.concat(
        [
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    )
    tr = tr_components.max(axis=1)

    atr = tr.rolling(window=period).mean()
    plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr.replace(0, pd.NA))
    minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr.replace(0, pd.NA))

    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, pd.NA)) * 100
    adx = dx.rolling(window=period).mean().iloc[-1]
    return float(adx) if pd.notna(adx) else 10.0


def _safe_last(series: pd.Series, default: float = 0.0) -> float:
    value = series.iloc[-1]
    return float(value) if pd.notna(value) else default


def build_rule_based_forecast(market_data):
    score = 0.0
    score_breakdown = {}

    price_above_ema200 = market_data["current_price"] > market_data["ema_200"]
    ema_contrib = 10 if price_above_ema200 else -10
    score += ema_contrib
    score_breakdown["ema200_trend"] = ema_contrib

    if market_data["current_price"] > market_data["sma_20"]:
        score += 8
        score_breakdown["price_vs_sma20"] = 8
    else:
        score -= 8
        score_breakdown["price_vs_sma20"] = -8

    if market_data["sma_20"] > market_data["sma_50"]:
        score += 12
        score_breakdown["sma20_vs_sma50"] = 12
    else:
        score -= 12
        score_breakdown["sma20_vs_sma50"] = -12

    rsi = market_data["rsi_14"]
    if 48 <= rsi <= 62:
        score += 6
        score_breakdown["rsi"] = 6
    elif rsi >= 72:
        score -= 8
        score_breakdown["rsi"] = -8
    elif rsi <= 30:
        score += 5
        score_breakdown["rsi"] = 5
    else:
        score_breakdown["rsi"] = 0

    change = market_data["change_pct_1d"]
    momentum_contrib = min(8, abs(change) * 1.2)
    score += momentum_contrib if change > 0 else -momentum_contrib
    score_breakdown["momentum_1d"] = round(momentum_contrib if change > 0 else -momentum_contrib, 2)

    volume_ratio = market_data["volume_ratio"]
    if volume_ratio >= 1.2:
        score += 4
        score_breakdown["volume"] = 4
    elif volume_ratio <= 0.75:
        score -= 3
        score_breakdown["volume"] = -3
    else:
        score_breakdown["volume"] = 0

    annual_volatility = market_data["annualized_volatility_pct"]
    if annual_volatility > 65:
        score -= 4
        score_breakdown["volatility_penalty"] = -4
    else:
        score_breakdown["volatility_penalty"] = 0

    adx = market_data["adx_14"]
    regime = "Тренд" if adx >= 23 else "Флэт/неуверенный"
    if adx >= 25:
        score += 4
        score_breakdown["adx_strength"] = 4
    elif adx <= 16:
        score -= 4
        score_breakdown["adx_strength"] = -4
    else:
        score_breakdown["adx_strength"] = 0

    corr_to_spx = market_data.get("corr_with_spy_60d")
    if corr_to_spx is not None:
        corr_factor = min(4, abs(corr_to_spx) * 4)
        score += corr_factor if market_data["change_pct_1d"] >= 0 else -corr_factor
        score_breakdown["corr_regime"] = round(corr_factor if market_data["change_pct_1d"] >= 0 else -corr_factor, 2)

    bullish_probability = max(5, min(95, round(50 + score)))
    bearish_probability = 100 - bullish_probability

    edge = abs(bullish_probability - 50)
    trade_allowed = True
    gate_reason = ""

    if adx < 16:
        trade_allowed = False
        gate_reason = "Низкий ADX: рынок без выраженного тренда"
    elif edge < 8:
        trade_allowed = False
        gate_reason = "Недостаточное статистическое преимущество"

    if bullish_probability >= 65:
        action = "Входить частями"
        bias = "Бычий"
    elif bullish_probability <= 40:
        action = "Сокращать риск / ждать"
        bias = "Медвежий"
    else:
        action = "Ждать подтверждения"
        bias = "Нейтральный"

    if not trade_allowed:
        action = "Ждать (фильтр качества)"
        bias = "Нейтральный"

    confidence = "Высокая" if abs(bullish_probability - 50) >= 20 else "Средняя"
    if not trade_allowed:
        confidence = "Низкая"

    return {
        "bias": bias,
        "bullish_probability": bullish_probability,
        "bearish_probability": bearish_probability,
        "action": action,
        "confidence": confidence,
        "score": round(score, 2),
        "regime": regime,
        "trade_allowed": trade_allowed,
        "gate_reason": gate_reason,
        "score_breakdown": score_breakdown,
    }


def build_three_day_forecast(market_data, rule_forecast):
    base_bull = float(rule_forecast["bullish_probability"])
    edge = base_bull - 50
    adx = float(market_data["adx_14"])
    vol = float(market_data["annualized_volatility_pct"])

    if adx >= 25:
        stability = 0.9
    elif adx >= 18:
        stability = 0.75
    else:
        stability = 0.6

    vol_damp = min(8.0, max(0.0, vol / 20.0))
    results = []

    for day in range(1, 4):
        projected_edge = edge * (stability ** (day - 1))
        if projected_edge > 0:
            projected_edge = max(0.0, projected_edge - (day - 1) * 0.5 * vol_damp)
        elif projected_edge < 0:
            projected_edge = min(0.0, projected_edge + (day - 1) * 0.5 * vol_damp)

        bull = int(max(5, min(95, round(50 + projected_edge))))
        bear = 100 - bull

        if bull >= 62:
            bias = "Бычий"
        elif bull <= 38:
            bias = "Медвежий"
        else:
            bias = "Нейтральный"

        results.append({
            "day": day,
            "bullish_probability": bull,
            "bearish_probability": bear,
            "bias": bias,
        })

    return results


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def _download_history(ticker_symbol: str, period: str, interval: str) -> pd.DataFrame:
    ticker = yf.Ticker(ticker_symbol)
    return ticker.history(period=period, interval=interval)


def _calc_corr_with_benchmark(symbol_close: pd.Series, benchmark_symbol: str, period: str = "6mo") -> float | None:
    try:
        benchmark = _download_history(benchmark_symbol, period=period, interval="1d")
        if benchmark.empty:
            return None
        sym_r = symbol_close.pct_change().dropna().tail(60)
        bench_r = benchmark["Close"].pct_change().dropna().tail(60)
        aligned = pd.concat([sym_r, bench_r], axis=1).dropna()
        if len(aligned) < 20:
            return None
        return float(aligned.iloc[:, 0].corr(aligned.iloc[:, 1]))
    except Exception as exc:
        logger.info("corr calc failed for %s: %s", benchmark_symbol, exc)
        return None


def get_market_data(ticker_symbol):
    try:
        ticker = yf.Ticker(ticker_symbol)
        meta = _extract_ticker_meta(ticker_symbol, ticker)

        hist = _download_history(ticker_symbol, period="2y", interval="1d")
        if hist.empty or len(hist) < 120:
            return None

        close = hist["Close"].astype(float)
        hist = hist.copy()
        hist["SMA20"] = close.rolling(window=20).mean()
        hist["SMA50"] = close.rolling(window=50).mean()

        last_candle_ts = pd.Timestamp(hist.index[-1])
        if last_candle_ts.tzinfo is None:
            last_candle_ts = last_candle_ts.tz_localize("UTC")
        else:
            last_candle_ts = last_candle_ts.tz_convert("UTC")
        now_utc = datetime.now(timezone.utc)
        lag_seconds = (now_utc - last_candle_ts.to_pydatetime()).total_seconds()

        current_price = float(close.iloc[-1])
        prev_price = float(close.iloc[-2])
        change_pct_1d = ((current_price - prev_price) / prev_price) * 100

        rsi_series = _calc_rsi(close, period=14)
        rsi_14 = _safe_last(rsi_series, 50.0)
        atr_14 = _calc_atr(hist, period=14)
        adx_14 = _calc_adx(hist, period=14)

        returns = close.pct_change().dropna()
        annualized_volatility_pct = float(returns.std() * (252**0.5) * 100)

        avg_vol_20 = hist["Volume"].tail(20).mean()
        curr_vol = hist["Volume"].iloc[-1]
        volume_ratio = float(curr_vol / avg_vol_20) if avg_vol_20 else 1.0

        corr_spy = _calc_corr_with_benchmark(close, "SPY")
        corr_btc = _calc_corr_with_benchmark(close, "BTC-USD")
        bt_stats = simple_backtest(close.tail(252), rsi_series.tail(252))

        data = {
            "symbol": ticker_symbol,
            "instrument_name": meta["instrument_name"],
            "instrument_type": meta["instrument_type"],
            "exchange": meta["exchange"],
            "currency": meta["currency"],
            "instrument_description": meta["instrument_description"],
            "last_candle_utc": last_candle_ts.strftime("%Y-%m-%d %H:%M UTC"),
            "data_lag_seconds": int(max(0, lag_seconds)),
            "data_lag_human": format_lag(lag_seconds),
            "current_price": round(current_price, 2),
            "change_pct_1d": round(change_pct_1d, 2),
            "annualized_volatility_pct": round(annualized_volatility_pct, 2),
            "sma_20": round(_safe_last(hist["SMA20"], current_price), 2),
            "sma_50": round(_safe_last(hist["SMA50"], current_price), 2),
            "ema_200": round(float(close.ewm(span=200, adjust=False).mean().iloc[-1]), 2),
            "rsi_14": round(rsi_14, 2),
            "atr_14": round(atr_14, 2),
            "adx_14": round(adx_14, 2),
            "high_20d": round(float(hist["High"].tail(20).max()), 2),
            "low_20d": round(float(hist["Low"].tail(20).min()), 2),
            "volume_ratio": round(volume_ratio, 2),
            "corr_with_spy_60d": round(corr_spy, 3) if corr_spy is not None else None,
            "corr_with_btc_60d": round(corr_btc, 3) if corr_btc is not None else None,
            "backtest": bt_stats,
            "chart_history": hist[["Close", "SMA20", "SMA50"]].tail(180),
        }

        data["rule_forecast"] = build_rule_based_forecast(data)
        data["forecast_3d"] = build_three_day_forecast(data, data["rule_forecast"])
        return data
    except Exception as exc:
        logger.exception("Ошибка при получении данных для %s: %s", ticker_symbol, exc)
        return None


if __name__ == "__main__":
    test_data = get_market_data("BTC-USD")
    if test_data:
        print(f"Данные получены: {test_data['symbol']}")
