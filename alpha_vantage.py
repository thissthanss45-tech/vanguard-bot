"""alpha_vantage.py — Optional fundamental data from Alpha Vantage.

Set env var ALPHA_VANTAGE_API_KEY to enable.
Free tier: 25 requests/day, 5 requests/min.
Only useful for US equities (not crypto, futures, forex).
"""
from __future__ import annotations

import json
import logging
import os
import urllib.request
from typing import Optional

logger = logging.getLogger(__name__)

_AV_BASE = "https://www.alphavantage.co/query"


def _get_key() -> Optional[str]:
    key = os.getenv("ALPHA_VANTAGE_API_KEY", "").strip()
    return key if key and key.lower() not in ("", "demo", "your_key_here") else None


def get_fundamentals(ticker: str) -> Optional[dict]:
    """Fetch company overview from Alpha Vantage OVERVIEW endpoint.

    Returns dict with fundamental metrics, or None if no key / request fails.
    """
    key = _get_key()
    if not key:
        return None

    url = f"{_AV_BASE}?function=OVERVIEW&symbol={ticker.upper()}&apikey={key}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "VanguardTgBot/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
    except Exception as exc:
        logger.warning("Alpha Vantage OVERVIEW failed for %s: %s", ticker, exc)
        return None

    if not data or "Symbol" not in data:
        return None

    def _flt(key_name: str) -> Optional[float]:
        v = data.get(key_name, "")
        if not v or v in ("None", "-", "N/A", "0"):
            return None
        try:
            return float(v)
        except Exception:
            return None

    def _fmt_large(v_str: str) -> Optional[str]:
        try:
            v = float(v_str)
            if v <= 0:
                return None
            if v >= 1e12:
                return f"{v/1e12:.2f}T"
            if v >= 1e9:
                return f"{v/1e9:.2f}B"
            if v >= 1e6:
                return f"{v/1e6:.2f}M"
            return str(round(v, 2))
        except Exception:
            return None

    result = {
        "name":           data.get("Name") or None,
        "sector":         data.get("Sector") or None,
        "industry":       data.get("Industry") or None,
        "market_cap":     _fmt_large(data.get("MarketCapitalization", "")),
        "pe_ratio":       _flt("PERatio"),
        "eps":            _flt("EPS"),
        "dividend_yield": _flt("DividendYield"),
        "profit_margin":  _flt("ProfitMargin"),
        "revenue_ttm":    _fmt_large(data.get("RevenueTTM", "")),
        "week_52_high":   _flt("52WeekHigh"),
        "week_52_low":    _flt("52WeekLow"),
        "analyst_target": _flt("AnalystTargetPrice"),
        "beta":           _flt("Beta"),
    }
    cleaned = {k: v for k, v in result.items() if v is not None}
    return cleaned if cleaned else None


def format_fundamentals_block(fundamentals: dict) -> str:
    """Format fundamentals dict into Markdown string for Telegram."""
    lines = ["📋 *Фундаментальные данные (Alpha Vantage)*"]
    if fundamentals.get("sector"):
        lines.append(f"  Сектор: {fundamentals['sector']}")
    if fundamentals.get("market_cap"):
        lines.append(f"  Капитализация: ${fundamentals['market_cap']}")
    if fundamentals.get("pe_ratio") is not None:
        lines.append(f"  P/E: {fundamentals['pe_ratio']}")
    if fundamentals.get("eps") is not None:
        lines.append(f"  EPS: ${fundamentals['eps']}")
    if fundamentals.get("revenue_ttm"):
        lines.append(f"  Выручка (TTM): ${fundamentals['revenue_ttm']}")
    if fundamentals.get("profit_margin") is not None:
        pm = round(fundamentals["profit_margin"] * 100, 1)
        lines.append(f"  Маржа прибыли: {pm}%")
    if fundamentals.get("dividend_yield") is not None:
        dy = round(fundamentals["dividend_yield"] * 100, 2)
        lines.append(f"  Дивидендная доходность: {dy}%")
    if fundamentals.get("beta") is not None:
        lines.append(f"  Бета: {fundamentals['beta']}")
    if fundamentals.get("analyst_target") is not None:
        lines.append(f"  Целевая цена (аналитики): ${fundamentals['analyst_target']}")
    if fundamentals.get("week_52_high") and fundamentals.get("week_52_low"):
        lines.append(
            f"  52-нед. диапазон: {fundamentals['week_52_low']} – {fundamentals['week_52_high']}"
        )
    return "\n".join(lines)
