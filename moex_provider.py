"""moex_provider.py — MOEX ISS API (free, no key required).

Provides OHLCV history and live prices for Russian equities (TQBR board).
No API key needed. Rate limit: ~5 req/sec (soft).
"""
from __future__ import annotations

import json
import logging
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Russian equities on TQBR board (Moscow Exchange main market)
MOEX_TICKERS: frozenset[str] = frozenset({
    "SBER", "SBERP", "GAZP", "LKOH", "YNDX", "ROSN", "NVTK", "GMKN",
    "TCSG", "MGNT", "VTBR", "ALRS", "TATN", "TATAP", "CHMF", "NLMK",
    "MAGN", "IRAO", "FEES", "RTKM", "RTKMP", "AFLT", "PIKK", "OZON",
    "HHRU", "SMLT", "CBOM", "BSPB", "FLOT", "WUSH", "SFIN", "LSRG",
    "MOEX", "MTSS", "PLZL", "PHOR", "SNGS", "SNGSP", "TRNFP",
    "FIVE", "LENT", "FIXP", "DVEC", "UWGN", "NMTP", "AQUA",
})

_ISS_BASE = "https://iss.moex.com/iss"
_HEADERS  = {"User-Agent": "VanguardTgBot/1.0"}


def is_moex_ticker(ticker: str) -> bool:
    return ticker.strip().upper() in MOEX_TICKERS


def _iss_get(url: str, timeout: int = 12) -> Optional[dict]:
    try:
        req = urllib.request.Request(url, headers=_HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())
    except Exception as exc:
        logger.warning("MOEX ISS request failed: %s — %s", url, exc)
        return None


def get_moex_price(ticker: str) -> Optional[float]:
    """Live last-price from MOEX TQBR board."""
    t = ticker.strip().upper()
    url = (
        f"{_ISS_BASE}/engines/stock/markets/shares/boards/TQBR"
        f"/securities/{t}.json?iss.only=marketdata&iss.meta=off"
    )
    data = _iss_get(url)
    if not data:
        return None
    md = data.get("marketdata", {})
    cols = md.get("columns", [])
    rows = md.get("data", [])
    if not rows:
        return None
    row = dict(zip(cols, rows[0]))
    for field in ("LAST", "MARKETPRICE", "WAPRICE", "OPEN"):
        val = row.get(field)
        if val is not None:
            try:
                return float(val)
            except Exception:
                pass
    return None


def get_moex_history(ticker: str, days: int = 700) -> Optional[pd.DataFrame]:
    """Daily OHLCV candles from MOEX ISS.

    Returns DataFrame with columns Open/High/Low/Close/Volume and UTC DatetimeIndex —
    same format as yfinance history(), so all indicator calculations work unchanged.
    """
    t = ticker.strip().upper()
    till_dt = datetime.now(timezone.utc).date()
    from_dt = (datetime.now(timezone.utc) - timedelta(days=days)).date()

    all_rows: list[list] = []
    columns: list[str] = []
    start = 0
    while True:
        url = (
            f"{_ISS_BASE}/engines/stock/markets/shares/boards/TQBR"
            f"/securities/{t}/candles.json"
            f"?interval=24&from={from_dt}&till={till_dt}&start={start}&iss.meta=off"
        )
        data = _iss_get(url)
        if not data:
            break
        candles = data.get("candles", {})
        if not columns:
            columns = candles.get("columns", [])
        batch = candles.get("data", [])
        if not batch:
            break
        all_rows.extend(batch)
        if len(batch) < 500:
            break
        start += 500

    if not all_rows or not columns:
        return None

    df = pd.DataFrame(all_rows, columns=columns)
    rename = {
        "open": "Open", "close": "Close",
        "high": "High",  "low": "Low",
        "volume": "Volume", "begin": "Datetime",
    }
    df = df.rename(columns=rename)
    if "Datetime" not in df.columns:
        return None
    df["Datetime"] = pd.to_datetime(df["Datetime"], utc=True)
    df = df.set_index("Datetime").sort_index()
    needed = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]
    df = df[needed].astype(float).dropna()
    return df if len(df) >= 30 else None


def get_moex_instrument_info(ticker: str) -> dict:
    """Basic instrument metadata from MOEX ISS."""
    t = ticker.strip().upper()
    url = f"{_ISS_BASE}/securities/{t}.json?iss.only=description&iss.meta=off"
    data = _iss_get(url)
    name = t
    currency = "RUB"
    if data:
        desc = data.get("description", {})
        cols = desc.get("columns", [])
        rows = desc.get("data", [])
        if "name" in cols and "value" in cols:
            ni = cols.index("name")
            vi = cols.index("value")
            info_map = {row[ni]: row[vi] for row in rows if len(row) > max(ni, vi)}
            name = info_map.get("SHORTNAME") or info_map.get("NAME") or t
            currency = info_map.get("CURRENCYID") or "RUB"
    return {
        "instrument_name": name,
        "instrument_type": "Акция",
        "exchange": "MOEX",
        "currency": currency,
        "instrument_description": f"{name} (Акция), MOEX / {currency}",
    }
