import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yfinance as yf


FORECAST_LOG_PATH = Path("data/forecast_snapshots.jsonl")


@dataclass
class ForecastSnapshot:
    symbol: str
    analyzed_at_utc: str
    base_price: float
    atr_14: float
    d1_bull: int
    d1_bear: int
    d1_bias: str
    d2_bull: int
    d2_bear: int
    d2_bias: str
    d3_bull: int
    d3_bear: int
    d3_bias: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "analyzed_at_utc": self.analyzed_at_utc,
            "base_price": self.base_price,
            "atr_14": self.atr_14,
            "d1_bull": self.d1_bull,
            "d1_bear": self.d1_bear,
            "d1_bias": self.d1_bias,
            "d2_bull": self.d2_bull,
            "d2_bear": self.d2_bear,
            "d2_bias": self.d2_bias,
            "d3_bull": self.d3_bull,
            "d3_bear": self.d3_bear,
            "d3_bias": self.d3_bias,
        }


def _safe_int(value: Any, default: int = 50) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _parse_dt(dt_text: str) -> datetime:
    parsed = datetime.fromisoformat(dt_text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def append_snapshot_from_market_data(market_data: dict):
    forecast = market_data.get("forecast_3d", [])
    if len(forecast) < 3:
        return

    analyzed_at = datetime.now(timezone.utc).isoformat()
    item1, item2, item3 = forecast[0], forecast[1], forecast[2]

    snapshot = ForecastSnapshot(
        symbol=str(market_data.get("symbol", "")).strip().upper(),
        analyzed_at_utc=analyzed_at,
        base_price=float(market_data.get("current_price", 0.0) or 0.0),
        atr_14=float(market_data.get("atr_14", 0.0) or 0.0),
        d1_bull=_safe_int(item1.get("bullish_probability"), 50),
        d1_bear=_safe_int(item1.get("bearish_probability"), 50),
        d1_bias=str(item1.get("bias", "Нейтральный")),
        d2_bull=_safe_int(item2.get("bullish_probability"), 50),
        d2_bear=_safe_int(item2.get("bearish_probability"), 50),
        d2_bias=str(item2.get("bias", "Нейтральный")),
        d3_bull=_safe_int(item3.get("bullish_probability"), 50),
        d3_bear=_safe_int(item3.get("bearish_probability"), 50),
        d3_bias=str(item3.get("bias", "Нейтральный")),
    )

    FORECAST_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with FORECAST_LOG_PATH.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(snapshot.to_dict(), ensure_ascii=False) + "\n")


def _load_snapshots() -> list[dict[str, Any]]:
    if not FORECAST_LOG_PATH.exists():
        return []
    rows = []
    with FORECAST_LOG_PATH.open("r", encoding="utf-8") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    return rows


def _latest_per_symbol(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        ts = str(row.get("analyzed_at_utc", ""))
        prev = latest.get(symbol)
        if prev is None or ts > str(prev.get("analyzed_at_utc", "")):
            latest[symbol] = row
    return list(latest.values())


def _is_matured(analyzed_at_utc: str, hours: int = 72) -> bool:
    try:
        dt = _parse_dt(analyzed_at_utc)
    except Exception:
        return False
    return datetime.now(timezone.utc) - dt >= timedelta(hours=hours)


def _eta_to_maturity(analyzed_at_utc: str, hours: int = 72) -> tuple[int | None, str | None]:
    try:
        dt = _parse_dt(analyzed_at_utc)
    except Exception:
        return None, None

    target = dt + timedelta(hours=hours)
    remaining = int((target - datetime.now(timezone.utc)).total_seconds())
    if remaining <= 0:
        return 0, "уже доступен"

    rem_hours = remaining // 3600
    rem_minutes = (remaining % 3600) // 60
    return remaining, f"{rem_hours} ч {rem_minutes} мин"


def _classify_outcome(return_pct: float, atr_14: float, base_price: float) -> str:
    if base_price <= 0:
        return "Нейтральный"

    atr_part = (atr_14 / base_price) * 100 if atr_14 > 0 else 0.5
    threshold = max(0.25, min(1.5, 0.25 * atr_part))

    if return_pct > threshold:
        return "Бычий"
    if return_pct < -threshold:
        return "Медвежий"
    return "Нейтральный"


def _actual_return_3d(symbol: str, analyzed_at_utc: str, base_price: float) -> tuple[float | None, str | None]:
    try:
        start = _parse_dt(analyzed_at_utc)
    except Exception:
        return None, None

    target = start + timedelta(days=3)

    try:
        hist = yf.Ticker(symbol).history(period="10d", interval="1d")
    except Exception:
        return None, None

    if hist is None or hist.empty:
        return None, None

    index_utc = pd.to_datetime(hist.index, utc=True)
    hist = hist.copy()
    hist.index = index_utc
    eligible = hist[hist.index >= target]
    if eligible.empty:
        return None, None

    close_price = float(eligible["Close"].iloc[0])
    if base_price <= 0:
        return None, None

    ret = ((close_price - base_price) / base_price) * 100
    return round(ret, 2), close_price.__str__()


def build_matured_report(max_items: int = 20) -> tuple[str, list[dict[str, Any]]]:
    all_rows = _latest_per_symbol(_load_snapshots())
    snapshots = [row for row in all_rows if _is_matured(str(row.get("analyzed_at_utc", "")), 72)]
    snapshots = sorted(snapshots, key=lambda row: row.get("analyzed_at_utc", ""), reverse=True)

    if not snapshots:
        pending = []
        for row in all_rows:
            eta_seconds, eta_text = _eta_to_maturity(str(row.get("analyzed_at_utc", "")), 72)
            if eta_seconds is None:
                continue
            if eta_seconds > 0:
                pending.append((eta_seconds, str(row.get("symbol", "?")), eta_text))

        if not pending:
            return "Пока нет тикеров с созревшим горизонтом D3 (3+ дня).", []

        pending.sort(key=lambda item: item[0])
        _, symbol, eta_text = pending[0]

        recent_rows = sorted(
            [row for row in all_rows if not _is_matured(str(row.get("analyzed_at_utc", "")), 72)],
            key=lambda row: str(row.get("analyzed_at_utc", "")),
            reverse=True,
        )

        lines = [
            "Пока нет тикеров с созревшим горизонтом D3 (3+ дня).",
            f"Ближайший: {symbol} примерно через {eta_text}.",
        ]

        if recent_rows:
            lines.append("Последние ожидающие тикеры:")
            for row in recent_rows[:5]:
                s = str(row.get("symbol", "?"))
                _, eta_item = _eta_to_maturity(str(row.get("analyzed_at_utc", "")), 72)
                lines.append(f"• {s}: примерно через {eta_item or 'н/д'}")

        return "\n".join(lines), []

    rows: list[dict[str, Any]] = []
    lines = ["📈 Анализ по тикерам (созревшие D3):"]

    for item in snapshots[:max_items]:
        symbol = str(item.get("symbol", ""))
        base_price = float(item.get("base_price", 0.0) or 0.0)
        atr_14 = float(item.get("atr_14", 0.0) or 0.0)
        d3_bias = str(item.get("d3_bias", "Нейтральный"))
        d3_bull = _safe_int(item.get("d3_bull"), 50)
        d3_bear = _safe_int(item.get("d3_bear"), 50)

        ret_3d, close_text = _actual_return_3d(symbol, str(item.get("analyzed_at_utc", "")), base_price)
        if ret_3d is None:
            actual_bias = "н/д"
            ret_text = "н/д"
            verdict = "pending"
        else:
            actual_bias = _classify_outcome(ret_3d, atr_14, base_price)
            ret_text = f"{ret_3d:+.2f}%"
            verdict = "correct" if actual_bias == d3_bias else "incorrect"

        lines.append(
            f"• {symbol}: D3 прогноз={d3_bias} ({d3_bull}/{d3_bear}) | факт={actual_bias} | доходность={ret_text} | verdict={verdict}"
        )

        rows.append(
            {
                "symbol": symbol,
                "analyzed_at_utc": item.get("analyzed_at_utc"),
                "base_price": round(base_price, 4),
                "d3_bias_forecast": d3_bias,
                "d3_bull_forecast": d3_bull,
                "d3_bear_forecast": d3_bear,
                "actual_return_3d_pct": ret_3d,
                "actual_bias_3d": actual_bias,
                "verdict": verdict,
                "close_used": close_text,
            }
        )

    return "\n".join(lines), rows


def export_matured_report_to_excel(out_path: str = "data/forecast_report_3d.xlsx") -> str | None:
    _, rows = build_matured_report(max_items=500)
    if not rows:
        return None

    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(rows)
    df.to_excel(path, index=False)
    return str(path)
