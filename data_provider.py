import logging
import urllib.request
from datetime import datetime, timedelta, timezone

import pandas as pd
import yfinance as yf
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

import ml_forecast as _ml
from backtesting import multi_strategy_backtest, simple_backtest
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


def _calc_adx_series(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Return full ADX series (vectorized, O(n)) for use in backtesting."""
    high = df["High"].astype(float)
    low = df["Low"].astype(float)
    close = df["Close"].astype(float)

    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    tr = pd.concat(
        [high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()],
        axis=1,
    ).max(axis=1)

    atr = tr.rolling(window=period).mean().replace(0, pd.NA)
    plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr)
    minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr)
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, pd.NA)) * 100
    return dx.rolling(window=period).mean().fillna(10.0)


def _safe_last(series: pd.Series, default: float = 0.0) -> float:
    value = series.iloc[-1]
    return float(value) if pd.notna(value) else default


# ──────────────────────── ДОПОЛНИТЕЛЬНЫЕ ИНДИКАТОРЫ ────────────────────────

def _calc_bollinger(close: pd.Series, period: int = 20, std_dev: float = 2.0) -> dict:
    """Bollinger Bands: upper, middle, lower, %B, bandwidth."""
    sma = close.rolling(period).mean()
    std = close.rolling(period).std()
    upper = sma + std_dev * std
    lower = sma - std_dev * std
    last_close = float(close.iloc[-1])
    last_upper = float(_safe_last(upper, last_close))
    last_lower = float(_safe_last(lower, last_close))
    last_mid   = float(_safe_last(sma, last_close))
    bandwidth  = (last_upper - last_lower) / last_mid * 100 if last_mid else 0
    pct_b      = (last_close - last_lower) / (last_upper - last_lower) if (last_upper - last_lower) > 0 else 0.5
    return {
        "bb_upper": round(last_upper, 4),
        "bb_mid":   round(last_mid, 4),
        "bb_lower": round(last_lower, 4),
        "bb_pct_b": round(pct_b, 3),       # 0=lower band, 1=upper band
        "bb_bandwidth": round(bandwidth, 2),
    }


def _calc_stochastic(hist: pd.DataFrame, k_period: int = 14, d_period: int = 3) -> dict:
    """Stochastic oscillator %K and %D."""
    high = hist["High"].astype(float)
    low  = hist["Low"].astype(float)
    close = hist["Close"].astype(float)
    low_k  = low.rolling(k_period).min()
    high_k = high.rolling(k_period).max()
    denom  = (high_k - low_k).replace(0, pd.NA)
    pct_k  = ((close - low_k) / denom * 100).fillna(50)
    pct_d  = pct_k.rolling(d_period).mean().fillna(50)
    stoch_k = round(float(_safe_last(pct_k, 50.0)), 2)
    stoch_d = round(float(_safe_last(pct_d, 50.0)), 2)
    # Signal: oversold <20, overbought >80
    if stoch_k < 20 and stoch_d < 20:
        stoch_signal = "перепродан"
        stoch_contrib = 6
    elif stoch_k > 80 and stoch_d > 80:
        stoch_signal = "перекуплен"
        stoch_contrib = -6
    elif stoch_k < 30 and stoch_d < 30:
        stoch_signal = "слабо перепродан"
        stoch_contrib = 3
    elif stoch_k > 70 and stoch_d > 70:
        stoch_signal = "слабо перекуплен"
        stoch_contrib = -3
    elif stoch_k > stoch_d:
        stoch_signal = "бычий кросс"
        stoch_contrib = 2
    elif stoch_k < stoch_d:
        stoch_signal = "медвежий кросс"
        stoch_contrib = -2
    else:
        stoch_signal = "нейтральный"
        stoch_contrib = 0
    return {
        "stoch_k": stoch_k,
        "stoch_d": stoch_d,
        "stoch_signal": stoch_signal,
        "stoch_contrib": stoch_contrib,
    }


def _calc_obv(hist: pd.DataFrame) -> dict:
    """On-Balance Volume trend (5d slope vs 20d avg)."""
    close = hist["Close"].astype(float)
    vol   = hist["Volume"].astype(float)
    direction = close.diff().apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    obv = (direction * vol).cumsum()
    obv_sma5  = obv.rolling(5).mean()
    obv_sma20 = obv.rolling(20).mean()
    last_obv5  = float(_safe_last(obv_sma5, 0))
    last_obv20 = float(_safe_last(obv_sma20, 0))
    # Trend: rising OBV confirms price move
    if last_obv5 > last_obv20 * 1.01:
        obv_trend = "растущий (бычий)"
        obv_contrib = 4
    elif last_obv5 < last_obv20 * 0.99:
        obv_trend = "падающий (медвежий)"
        obv_contrib = -4
    else:
        obv_trend = "нейтральный"
        obv_contrib = 0
    return {
        "obv_trend": obv_trend,
        "obv_contrib": obv_contrib,
    }


def _calc_macd_full(close: pd.Series) -> dict:
    """Full MACD: line, signal, histogram, and crossover signal."""
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd_line   = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    histogram   = macd_line - signal_line
    last_macd  = round(float(_safe_last(macd_line, 0)), 4)
    last_sig   = round(float(_safe_last(signal_line, 0)), 4)
    last_hist  = round(float(_safe_last(histogram, 0)), 4)
    prev_hist  = round(float(histogram.iloc[-2]) if len(histogram) > 1 else 0, 4)
    bull_cross = last_macd > last_sig
    # Histogram direction: positive and growing = bullish momentum building
    hist_growing = last_hist > prev_hist
    if bull_cross and hist_growing:
        macd_signal = "бычий (гистограмма растёт)"
        macd_contrib = 5
    elif bull_cross and not hist_growing:
        macd_signal = "бычий (гистограмма сжимается)"
        macd_contrib = 2
    elif not bull_cross and not hist_growing:
        macd_signal = "медвежий (гистограмма падает)"
        macd_contrib = -5
    else:
        macd_signal = "медвежий (гистограмма растёт)"
        macd_contrib = -2
    return {
        "macd_line":    last_macd,
        "macd_signal":  last_sig,
        "macd_hist":    last_hist,
        "macd_signal_bull": bull_cross,
        "macd_full_signal": macd_signal,
        "macd_full_contrib": macd_contrib,
    }


def _calc_parabolic_sar(hist: pd.DataFrame, af_start: float = 0.02, af_max: float = 0.2) -> dict:
    """
    Parabolic SAR — trend-following indicator.
    Returns: sar_value, sar_trend ('бычий'|'медвежий'), sar_contrib (+/-4)
    """
    high  = hist["High"].astype(float).values
    low   = hist["Low"].astype(float).values
    close = hist["Close"].astype(float).values
    n = len(close)
    if n < 5:
        return {"sar_value": None, "sar_trend": "н/д", "sar_contrib": 0}

    # Init
    bull = close[1] >= close[0]
    sar  = low[0] if bull else high[0]
    ep   = high[0] if bull else low[0]
    af   = af_start

    for i in range(1, n):
        sar = sar + af * (ep - sar)
        if bull:
            sar = min(sar, low[i-1], low[max(0, i-2)])
            if low[i] < sar:
                bull = False
                sar  = ep
                ep   = low[i]
                af   = af_start
            else:
                if high[i] > ep:
                    ep = high[i]
                    af = min(af + af_start, af_max)
        else:
            sar = max(sar, high[i-1], high[max(0, i-2)])
            if high[i] > sar:
                bull = True
                sar  = ep
                ep   = high[i]
                af   = af_start
            else:
                if low[i] < ep:
                    ep = low[i]
                    af = min(af + af_start, af_max)

    sar_trend  = "бычий" if bull else "медвежий"
    sar_contrib = 4 if bull else -4
    return {
        "sar_value":  round(float(sar), 4),
        "sar_trend":  sar_trend,
        "sar_contrib": sar_contrib,
    }


def _calc_support_resistance(hist: pd.DataFrame, n_levels: int = 3) -> dict:
    """
    Finds key S/R levels using pivot-point highs/lows on the last 60 bars.
    Returns: support list (desc), resistance list (asc), nearest_support, nearest_resistance,
             and pivot_classic (high+low+close)/3 from previous bar.
    """
    df = hist.tail(60)
    close  = df["Close"].astype(float)
    high_s = df["High"].astype(float)
    low_s  = df["Low"].astype(float)
    current = float(close.iloc[-1])

    # Classic pivot (yesterday's bar)
    yest_high  = float(high_s.iloc[-2])
    yest_low   = float(low_s.iloc[-2])
    yest_close = float(close.iloc[-2])
    pivot = (yest_high + yest_low + yest_close) / 3
    r1 = 2 * pivot - yest_low
    s1 = 2 * pivot - yest_high
    r2 = pivot + (yest_high - yest_low)
    s2 = pivot - (yest_high - yest_low)

    # Swing highs/lows: local max/min with window=3
    highs = []
    lows  = []
    h_vals = high_s.values
    l_vals = low_s.values
    for i in range(2, len(h_vals) - 2):
        if h_vals[i] == max(h_vals[i-2:i+3]):
            highs.append(h_vals[i])
        if l_vals[i] == min(l_vals[i-2:i+3]):
            lows.append(l_vals[i])

    resistances = sorted(set([r1, r2] + [h for h in highs if h > current]))[:n_levels]
    supports    = sorted(set([s1, s2] + [l for l in lows  if l < current]), reverse=True)[:n_levels]

    nearest_res  = resistances[0] if resistances else None
    nearest_sup  = supports[0]    if supports    else None

    # Distance to nearest S/R as % of price
    dist_res = round((nearest_res - current) / current * 100, 2) if nearest_res else None
    dist_sup = round((current - nearest_sup) / current * 100, 2) if nearest_sup else None

    return {
        "support_levels":    [round(x, 4) for x in supports],
        "resistance_levels": [round(x, 4) for x in resistances],
        "nearest_support":   round(nearest_sup, 4) if nearest_sup else None,
        "nearest_resistance": round(nearest_res, 4) if nearest_res else None,
        "dist_to_support_pct":    dist_sup,
        "dist_to_resistance_pct": dist_res,
        "pivot_classic": round(pivot, 4),
    }


def _get_fear_greed(instrument_type: str = "") -> dict:
    """
    Fetch Fear & Greed Index.
    Crypto tickers → alternative.me first (Crypto F&G), CNN fallback.
    Stock/other    → CNN first, alternative.me fallback.
    Returns:
        score      : 0-100
        rating     : Extreme Fear / Fear / Neutral / Greed / Extreme Greed (EN)
        rating_ru  : русский перевод
        source     : 'cnn' | 'alternative.me' | 'unavailable'
        score_contrib : вклад в rule_score (-8 .. +8)
    """
    result = {
        "score": None,
        "rating": None,
        "rating_ru": "н/д",
        "source": "unavailable",
        "score_contrib": 0,
    }
    _RATING_RU = {
        "extreme fear": "Екстремальный страх",
        "fear": "Страх",
        "neutral": "Нейтрально",
        "greed": "Жадность",
        "extreme greed": "Екстремальная жадность",
    }

    import json as _json
    _is_crypto = "крипто" in (instrument_type or "").lower()

    def _try_cnn():
        try:
            req = urllib.request.Request(
                "https://production.dataviz.cnn.io/index/fearandgreed/graphdata",
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "application/json",
                    "Referer": "https://edition.cnn.com/",
                },
            )
            with urllib.request.urlopen(req, timeout=8) as r:
                d = _json.loads(r.read())
            fg = d.get("fear_and_greed", {})
            score = float(fg.get("score", 0))
            rating = str(fg.get("rating", "")).lower()
            if score == 0:
                return False   # CNN sometimes returns score=0 on soft-fail
            result["score"] = round(score, 1)
            result["rating"] = rating
            result["rating_ru"] = _RATING_RU.get(rating, rating)
            result["source"] = "cnn"
            return True
        except Exception as exc:
            logger.debug("fear_greed CNN failed: %s", exc)
            return False

    def _try_altme():
        try:
            req2 = urllib.request.Request(
                "https://api.alternative.me/fng/?limit=1",
                headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) Chrome/120"},
            )
            with urllib.request.urlopen(req2, timeout=8) as r2:
                d2 = _json.loads(r2.read())
            entry = d2.get("data", [{}])[0]
            score = float(entry.get("value", 50))
            rating = str(entry.get("value_classification", "")).lower()
            result["score"] = round(score, 1)
            result["rating"] = rating
            result["rating_ru"] = _RATING_RU.get(rating, rating)
            result["source"] = "alternative.me"
            return True
        except Exception as exc:
            logger.debug("fear_greed alternative.me failed: %s", exc)
            return False

    # Crypto → alt.me first (более релевантный индекс); fallback → CNN
    # Stocks/other → CNN first; fallback → alt.me
    if _is_crypto:
        _try_altme() or _try_cnn()
    else:
        _try_cnn() or _try_altme()

    # Compute score_contrib: extreme fear = contrarian buy (+6..+8),
    # extreme greed = contrarian sell (-6..-8), neutral = 0
    if result["score"] is not None:
        s = result["score"]
        if s <= 20:
            result["score_contrib"] = 8   # extreme fear → contrarian bullish
        elif s <= 35:
            result["score_contrib"] = 4
        elif s <= 45:
            result["score_contrib"] = 2
        elif s <= 55:
            result["score_contrib"] = 0
        elif s <= 65:
            result["score_contrib"] = -2
        elif s <= 80:
            result["score_contrib"] = -4
        else:
            result["score_contrib"] = -8  # extreme greed → contrarian bearish

    return result


def _calc_volume_anomaly(hist: pd.DataFrame) -> dict:
    """
    Detects volume anomalies:
    - Spike: current volume > 2x 20d average
    - Divergence: price direction diverges from volume trend (weak move)
    - Squeeze: volume < 0.5x average (drying up, potential explosion)

    Returns:
        vol_spike        : bool
        vol_spike_ratio  : float (curr / avg20)
        vol_divergence   : bool (price up + vol down = weak)
        vol_squeeze      : bool
        vol_signal       : str (текстовое описание аномалии)
        vol_score_contrib: int вклад в rule_score
    """
    result = {
        "vol_spike": False,
        "vol_spike_ratio": 1.0,
        "vol_divergence": False,
        "vol_squeeze": False,
        "vol_signal": "норма",
        "vol_score_contrib": 0,
    }
    try:
        vol = hist["Volume"].astype(float)
        close = hist["Close"].astype(float)

        avg20 = vol.tail(21).iloc[:-1].mean()  # 20d avg excluding today
        curr_vol = float(vol.iloc[-1])
        if avg20 <= 0:
            return result

        ratio = curr_vol / avg20
        result["vol_spike_ratio"] = round(ratio, 2)

        price_change = float(close.iloc[-1]) - float(close.iloc[-2])
        vol_change_5d = vol.iloc[-1] / vol.tail(6).iloc[:-1].mean() - 1  # 5d vol trend

        signals = []
        contrib = 0

        # SPIKE: volume > 2x average
        if ratio >= 2.0:
            result["vol_spike"] = True
            # Spike + price move = confirmation (+/-)
            if price_change > 0:
                signals.append(f"Спайк объёма x{ratio:.1f} + рост цены (бычье подтверждено)")
                contrib += 6
            else:
                signals.append(f"Спайк объёма x{ratio:.1f} + падение цены (продажи на объёме)")
                contrib -= 6
        elif ratio >= 1.5:
            if price_change > 0:
                signals.append(f"Повышен. объём x{ratio:.1f} + рост")
                contrib += 3
            else:
                signals.append(f"Повышен. объём x{ratio:.1f} + падение")
                contrib -= 3

        # DIVERGENCE: price up but vol trend strongly down (weak move)
        if price_change > 0 and vol_change_5d < -0.35:
            result["vol_divergence"] = True
            signals.append("Дивергенция: цена растёт на падающем объёме (слабый рост)")
            contrib -= 4
        elif price_change < 0 and vol_change_5d < -0.35:
            result["vol_divergence"] = True
            signals.append("Дивергенция: падение на слабом объёме (нет продавцов)")
            contrib += 3  # слабое падение = меньше медвежье

        # SQUEEZE: volume < 0.5x average
        if ratio < 0.5:
            result["vol_squeeze"] = True
            signals.append("Объём иссяк: возможен пробой (нейтрально)")
            # No directional contrib — just informational

        result["vol_signal"] = "; ".join(signals) if signals else "норма"
        result["vol_score_contrib"] = max(-8, min(8, contrib))
    except Exception as exc:
        logger.debug("volume_anomaly failed: %s", exc)
    return result


def _get_earnings_info(ticker: yf.Ticker, symbol: str) -> dict:
    """
    Returns earnings proximity info for stocks.
    For non-equity tickers (crypto, futures, forex) returns neutral defaults.
    """
    result = {
        "earnings_date": None,
        "days_to_earnings": None,
        "earnings_warning": False,
        "earnings_warning_text": "",
    }
    # Only relevant for stocks — skip commodities, crypto, forex
    if any(x in symbol for x in ["-USD", "-EUR", "=F", "=X"]):
        return result
    try:
        cal = ticker.get_earnings_dates(limit=4)
        if cal is None or cal.empty:
            return result
        now_utc = datetime.now(timezone.utc)
        for idx in cal.index:
            ts = pd.Timestamp(idx)
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            else:
                ts = ts.tz_convert("UTC")
            dt = ts.to_pydatetime()
            days_delta = (dt - now_utc).days
            # Look for upcoming earnings (within 14 days)
            if -1 <= days_delta <= 14:
                result["earnings_date"] = dt.strftime("%Y-%m-%d")
                result["days_to_earnings"] = days_delta
                if days_delta <= 3:
                    result["earnings_warning"] = True
                    result["earnings_warning_text"] = (
                        f"⚠️ EARNINGS ЧЕРЕЗ {days_delta} ДН. — прогноз ненадёжен"
                        if days_delta > 0
                        else "⚠️ EARNINGS СЕГОДНЯ — прогноз ненадёжен"
                    )
                elif days_delta <= 7:
                    result["earnings_warning_text"] = (
                        f"ℹ️ Отчётность через {days_delta} дн. — повышенная неопределённость"
                    )
                break
    except Exception as exc:
        logger.debug("earnings_info failed for %s: %s", symbol, exc)
    return result


def _calc_multitf_signals(symbol: str) -> dict:
    """
    Download 1h (60d) and 1W (3y) data; compute RSI + SMA-trend for each timeframe.
    Returns alignment score and per-TF bias.
    """
    result = {
        "tf_1h_bias": "н/д",
        "tf_1h_rsi": None,
        "tf_1w_bias": "н/д",
        "tf_1w_rsi": None,
        "tf_alignment": "н/д",       # Concordant / Divergent / Mixed
        "tf_alignment_score": 0,       # +2 all bull, -2 all bear, 0 neutral/mixed
        "tf_available": False,
    }
    try:
        # 1-hour bars for 60 days
        h1 = _download_history(symbol, period="60d", interval="1h")
        # Weekly bars for 3 years
        w1 = _download_history(symbol, period="3y", interval="1wk")

        signals = {}
        for label, df in [("1h", h1), ("1w", w1)]:
            if df is None or len(df) < 30:
                signals[label] = None
                continue
            c = df["Close"].astype(float)
            rsi = _calc_rsi(c, 14)
            rsi_val = _safe_last(rsi, 50.0)
            sma20 = c.rolling(20).mean()
            sma50 = c.rolling(50).mean() if len(c) >= 50 else sma20
            trend_bull = (
                float(c.iloc[-1]) > float(_safe_last(sma20, c.iloc[-1]))
                and float(_safe_last(sma20, 0)) > float(_safe_last(sma50, 0))
            )
            # RSI confirmation
            if trend_bull and rsi_val >= 45:
                bias = "Бычий"
            elif not trend_bull and rsi_val <= 55:
                bias = "Медвежий"
            else:
                bias = "Нейтральный"
            signals[label] = {"bias": bias, "rsi": round(rsi_val, 1)}

        if signals.get("1h") is None and signals.get("1w") is None:
            return result

        result["tf_available"] = True

        if signals.get("1h"):
            result["tf_1h_bias"] = signals["1h"]["bias"]
            result["tf_1h_rsi"] = signals["1h"]["rsi"]
        if signals.get("1w"):
            result["tf_1w_bias"] = signals["1w"]["bias"]
            result["tf_1w_rsi"] = signals["1w"]["rsi"]

        # Alignment across 1h + 1d (already in data) + 1w
        biases = [
            b for b in [result["tf_1h_bias"], result["tf_1w_bias"]]
            if b not in ("н/д", None)
        ]
        bull_count = biases.count("Бычий")
        bear_count = biases.count("Медвежий")

        if bull_count == len(biases) and len(biases) >= 1:
            result["tf_alignment"] = "Всё бычье"
            result["tf_alignment_score"] = bull_count
        elif bear_count == len(biases) and len(biases) >= 1:
            result["tf_alignment"] = "Всё медвежье"
            result["tf_alignment_score"] = -bear_count
        elif bull_count > bear_count:
            result["tf_alignment"] = "Преим. бычье"
            result["tf_alignment_score"] = 1
        elif bear_count > bull_count:
            result["tf_alignment"] = "Преим. медвежье"
            result["tf_alignment_score"] = -1
        else:
            result["tf_alignment"] = "Разнонаправленно"
            result["tf_alignment_score"] = 0

    except Exception as exc:
        logger.debug("multitf_signals failed for %s: %s", symbol, exc)
    return result


def build_rule_based_forecast(market_data):
    score = 0.0
    score_breakdown = {}

    # ── Настройка весов по типу инструмента ──
    itype = (market_data.get("instrument_type") or "").lower()
    is_crypto   = "крипто" in itype
    is_futures  = "фьючерс" in itype
    is_forex    = "форекс" in itype or "валют" in itype
    adx_gate    = 14 if (is_crypto or is_futures) else 16
    sma_cross_w = 8  if is_crypto else 12       # кросс SMA менее значим на крипте
    volume_w    = 6  if is_futures else 4        # объём важнее на фьючерсах
    ema200_w    = 8  if is_forex else 10         # ema200 чуть слабее на форексе

    price_above_ema200 = market_data["current_price"] > market_data["ema_200"]
    ema_contrib = ema200_w if price_above_ema200 else -ema200_w
    score += ema_contrib
    score_breakdown["ema200_trend"] = ema_contrib

    if market_data["current_price"] > market_data["sma_20"]:
        score += 8
        score_breakdown["price_vs_sma20"] = 8
    else:
        score -= 8
        score_breakdown["price_vs_sma20"] = -8

    if market_data["sma_20"] > market_data["sma_50"]:
        score += sma_cross_w
        score_breakdown["sma20_vs_sma50"] = sma_cross_w
    else:
        score -= sma_cross_w
        score_breakdown["sma20_vs_sma50"] = -sma_cross_w

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
        score += volume_w
        score_breakdown["volume"] = volume_w
    elif volume_ratio <= 0.75:
        score -= round(volume_w * 0.75)
        score_breakdown["volume"] = -round(volume_w * 0.75)
    else:
        score_breakdown["volume"] = 0

    annual_volatility = market_data["annualized_volatility_pct"]
    if annual_volatility > 65:
        score -= 4
        score_breakdown["volatility_penalty"] = -4
    else:
        score_breakdown["volatility_penalty"] = 0

    # MACD signal
    macd_contrib = market_data.get("macd_signal_bull")
    if macd_contrib is True:
        score += 8
        score_breakdown["macd"] = 8
    elif macd_contrib is False:
        score -= 8
        score_breakdown["macd"] = -8
    else:
        score_breakdown["macd"] = 0

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

    if adx < adx_gate:
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
        # Bias сохраняет реальное направление — только действие блокируется фильтром

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
    rule_bull = float(rule_forecast["bullish_probability"])

    # Ensemble: blend rule-based with calibrated ML probability
    ml_result = market_data.get("ml_forecast", {})
    base_bull = _ml.ensemble_probability(rule_bull, ml_result)

    # If ML signals neutral dominance, widen neutral zone to avoid false calls
    neutral_dominant = ml_result.get("ml_neutral_dominant", False)
    ml_conf = ml_result.get("ml_confidence", "Низкая")

    # Prediction threshold: stricter when ML uncertain
    if neutral_dominant or ml_conf == "Низкая":
        bull_threshold = 70  # very confident only
        bear_threshold = 30
    elif ml_conf == "Средняя":
        bull_threshold = 65
        bear_threshold = 35
    else:  # Высокая
        bull_threshold = 62
        bear_threshold = 38

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

        if bull >= bull_threshold:
            bias = "Бычий"
        elif bull <= bear_threshold:
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

        # Multi-strategy backtest (vectorized ADX series — O(n))
        sma20_series = close.rolling(window=20).mean()
        sma50_series = close.rolling(window=50).mean()
        adx_series = _calc_adx_series(hist)
        multi_bt = multi_strategy_backtest(
            close.tail(252),
            rsi_series.tail(252),
            sma20_series.tail(252),
            sma50_series.tail(252),
            adx_series.tail(252),
        )

        # MACD for rule-based score
        macd_full = _calc_macd_full(close)
        macd_signal_bull = macd_full["macd_signal_bull"]

        # Bollinger Bands
        bb = _calc_bollinger(close)

        # Stochastic
        stoch = _calc_stochastic(hist)

        # OBV trend
        obv = _calc_obv(hist)

        # Parabolic SAR
        sar = _calc_parabolic_sar(hist)

        # Support / Resistance levels
        sr = _calc_support_resistance(hist)

        # ML forecast (trained on full 2-year history)
        ml_stats = _ml.train_and_predict(hist)

        # Earnings calendar (stocks only)
        earnings_info = _get_earnings_info(ticker, ticker_symbol)

        # Multi-timeframe signals (1h + 1W)
        multitf = _calc_multitf_signals(ticker_symbol)

        # Fear & Greed Index
        fear_greed = _get_fear_greed(instrument_type=meta.get("instrument_type", ""))

        # Volume anomaly analysis
        vol_anomaly = _calc_volume_anomaly(hist)

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
            "macd_signal_bull": macd_signal_bull,
            "macd_line": macd_full["macd_line"],
            "macd_hist": macd_full["macd_hist"],
            "macd_full_signal": macd_full["macd_full_signal"],
            "bollinger": bb,
            "stochastic": stoch,
            "obv": obv,
            "sar": sar,
            "support_resistance": sr,
            "backtest": bt_stats,
            "multi_backtest": multi_bt,
            "earnings_info": earnings_info,
            "multitf": multitf,
            "fear_greed": fear_greed,
            "vol_anomaly": vol_anomaly,
            "chart_history": hist[["Close", "SMA20", "SMA50"]].tail(180),
        }

        data["ml_forecast"] = ml_stats
        data["rule_forecast"] = build_rule_based_forecast(data)

        # Patch rule_forecast with multi-TF alignment score
        tf_score = multitf.get("tf_alignment_score", 0)
        if tf_score != 0:
            rule = data["rule_forecast"]
            contrib = tf_score * 5  # ±5 or ±10 points
            new_bull = max(5, min(95, rule["bullish_probability"] + contrib))
            rule["bullish_probability"] = new_bull
            rule["bearish_probability"] = 100 - new_bull
            rule["score_breakdown"]["multitf_alignment"] = contrib

        # Patch rule_forecast with Fear & Greed contrarian signal
        fg_contrib = fear_greed.get("score_contrib", 0)
        if fg_contrib != 0:
            rule = data["rule_forecast"]
            new_bull = max(5, min(95, rule["bullish_probability"] + fg_contrib))
            rule["bullish_probability"] = new_bull
            rule["bearish_probability"] = 100 - new_bull
            rule["score_breakdown"]["fear_greed_contrarian"] = fg_contrib

        # Patch rule_forecast with volume anomaly signal
        va_contrib = vol_anomaly.get("vol_score_contrib", 0)
        if va_contrib != 0:
            rule = data["rule_forecast"]
            new_bull = max(5, min(95, rule["bullish_probability"] + va_contrib))
            rule["bullish_probability"] = new_bull
            rule["bearish_probability"] = 100 - new_bull
            rule["score_breakdown"]["vol_anomaly"] = va_contrib

        # Patch: MACD full histogram signal
        macd_c = macd_full.get("macd_full_contrib", 0)
        if macd_c != 0:
            rule = data["rule_forecast"]
            new_bull = max(5, min(95, rule["bullish_probability"] + macd_c))
            rule["bullish_probability"] = new_bull
            rule["bearish_probability"] = 100 - new_bull
            rule["score_breakdown"]["macd_full"] = macd_c

        # Patch: Stochastic
        stoch_c = stoch.get("stoch_contrib", 0)
        if stoch_c != 0:
            rule = data["rule_forecast"]
            new_bull = max(5, min(95, rule["bullish_probability"] + stoch_c))
            rule["bullish_probability"] = new_bull
            rule["bearish_probability"] = 100 - new_bull
            rule["score_breakdown"]["stochastic"] = stoch_c

        # Patch: OBV trend
        obv_c = obv.get("obv_contrib", 0)
        if obv_c != 0:
            rule = data["rule_forecast"]
            new_bull = max(5, min(95, rule["bullish_probability"] + obv_c))
            rule["bullish_probability"] = new_bull
            rule["bearish_probability"] = 100 - new_bull
            rule["score_breakdown"]["obv"] = obv_c

        # Patch: Parabolic SAR
        sar_c = sar.get("sar_contrib", 0)
        if sar_c != 0:
            rule = data["rule_forecast"]
            new_bull = max(5, min(95, rule["bullish_probability"] + sar_c))
            rule["bullish_probability"] = new_bull
            rule["bearish_probability"] = 100 - new_bull
            rule["score_breakdown"]["sar"] = sar_c

        # ── Пересчёт trade_allowed / bias / action ПОСЛЕ всех патчей ──
        rule = data["rule_forecast"]
        final_bull = rule["bullish_probability"]
        final_edge = abs(final_bull - 50)
        final_adx = data["adx_14"]
        _itype = (data.get("instrument_type") or "").lower()
        _adx_gate = 14 if ("крипто" in _itype or "фьючерс" in _itype) else 16
        if final_adx < _adx_gate:
            rule["trade_allowed"] = False
            rule["gate_reason"] = "Низкий ADX: рынок без выраженного тренда"
        elif final_edge < 8:
            rule["trade_allowed"] = False
            rule["gate_reason"] = "Недостаточное статистическое преимущество"
        else:
            rule["trade_allowed"] = True
            rule["gate_reason"] = ""

        if final_bull >= 65:
            rule["action"] = "Входить частями"
            rule["bias"] = "Бычий"
        elif final_bull <= 40:
            rule["action"] = "Сокращать риск / ждать"
            rule["bias"] = "Медвежий"
        else:
            rule["action"] = "Ждать подтверждения"
            rule["bias"] = "Нейтральный"
        if not rule["trade_allowed"]:
            rule["action"] = "Ждать (фильтр качества)"
        rule["confidence"] = "Высокая" if final_edge >= 20 else ("Средняя" if rule["trade_allowed"] else "Низкая")

        data["forecast_3d"] = build_three_day_forecast(data, data["rule_forecast"])
        return data
    except Exception as exc:
        logger.exception("Ошибка при получении данных для %s: %s", ticker_symbol, exc)
        return None


if __name__ == "__main__":
    test_data = get_market_data("BTC-USD")
    if test_data:
        print(f"Данные получены: {test_data['symbol']}")
