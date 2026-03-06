"""
portfolio_tracker.py — простой трекер сделок пользователя.
Хранит открытые/закрытые позиции в data/portfolio_{user_id}.json
Команды в боте:
  /trade add AAPL buy 150.00 10      — добавить лонг 10 шт. по 150.00
  /trade add BTC-USD sell 45000 0.5  — добавить шорт 0.5 BTC по 45000
  /trade close N                     — закрыть сделку №N (удаляет из открытых)
  /trade list                        — показать все открытые сделки
  /portfolio                         — P&L по текущим ценам
"""

from __future__ import annotations
import json
import os
from datetime import datetime, timezone
from typing import Optional

_DATA_DIR = "data"


def _portfolio_path(user_id: int) -> str:
    return os.path.join(_DATA_DIR, f"portfolio_{user_id}.json")


def _load(user_id: int) -> list[dict]:
    path = _portfolio_path(user_id)
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def _save(user_id: int, trades: list[dict]) -> None:
    os.makedirs(_DATA_DIR, exist_ok=True)
    path = _portfolio_path(user_id)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(trades, f, ensure_ascii=False, indent=2)


def trade_add(user_id: int, ticker: str, direction: str, price: float, qty: float) -> str:
    """direction: 'buy' (long) или 'sell' (short)"""
    direction = direction.lower()
    if direction not in ("buy", "sell"):
        return "❌ Направление должно быть buy или sell."
    if price <= 0 or qty <= 0:
        return "❌ Цена и количество должны быть > 0."
    trades = _load(user_id)
    entry = {
        "id": len(trades) + 1,
        "ticker": ticker.upper(),
        "direction": direction,
        "entry_price": round(price, 6),
        "qty": round(qty, 6),
        "opened_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }
    trades.append(entry)
    _save(user_id, trades)
    side = "ЛОНГ" if direction == "buy" else "ШОРТ"
    total = round(price * qty, 2)
    return f"✅ Добавлено #{entry['id']}: {side} {qty} {ticker.upper()} @ {price} = ${total}"


def trade_close(user_id: int, trade_id: int) -> str:
    trades = _load(user_id)
    original = next((t for t in trades if t["id"] == trade_id), None)
    if original is None:
        return f"❌ Сделка #{trade_id} не найдена."
    trades = [t for t in trades if t["id"] != trade_id]
    _save(user_id, trades)
    return f"✅ Сделка #{trade_id} ({original['direction'].upper()} {original['qty']} {original['ticker']}) закрыта и удалена."


def trade_close_with_pnl(user_id: int, trade_id: int, close_price: float) -> tuple[str, dict | None]:
    """
    Закрыть сделку с расчётом P&L.
    Returns: (текст сообщения, данные сделки или None)
    """
    trades = _load(user_id)
    original = next((t for t in trades if t["id"] == trade_id), None)
    if original is None:
        return f"❌ Сделка #{trade_id} не найдена.", None

    entry  = original["entry_price"]
    qty    = original["qty"]
    direction = original["direction"]
    ticker = original["ticker"]
    opened_at = original.get("opened_at", "?")

    if direction == "buy":
        pnl = (close_price - entry) * qty
        pct = (close_price - entry) / entry * 100
    else:
        pnl = (entry - close_price) * qty
        pct = (entry - close_price) / entry * 100

    pnl_r = round(pnl, 2)
    pct_r = round(pct, 2)
    side  = "🟢 ЛОНГ" if direction == "buy" else "🔴 ШОРТ"
    emoji = "🟢" if pnl_r >= 0 else "🔴"
    sign  = "+" if pnl_r >= 0 else ""
    result_word = "ПРИБЫЛЬ" if pnl_r >= 0 else "УБЫТОК"

    # save trade to closed history
    closed_path = os.path.join(_DATA_DIR, f"portfolio_closed_{user_id}.json")
    closed: list[dict] = []
    if os.path.exists(closed_path):
        try:
            with open(closed_path, "r", encoding="utf-8") as f:
                closed = json.load(f)
        except Exception:
            pass
    closed.append({
        **original,
        "close_price": round(close_price, 6),
        "pnl_usd": pnl_r,
        "pnl_pct": pct_r,
        "closed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    })
    with open(closed_path, "w", encoding="utf-8") as f:
        json.dump(closed, f, ensure_ascii=False, indent=2)

    # remove from open trades
    trades = [t for t in trades if t["id"] != trade_id]
    _save(user_id, trades)

    msg = (
        f"{emoji} *Сделка закрыта*\n\n"
        f"Тикер: `{ticker}` | {side}\n"
        f"Вход: {entry} → Выход: {close_price}\n"
        f"Объём: {qty}\n"
        f"Дата открытия: {opened_at}\n\n"
        f"{emoji} *{result_word}: {sign}{pnl_r} USD ({sign}{pct_r}%)*"
    )
    return msg, original


def trade_list(user_id: int) -> str:
    trades = _load(user_id)
    if not trades:
        return "📋 Нет открытых сделок. Добавь через /trade add TICKER buy/sell PRICE QTY"
    lines = ["📋 *Открытые сделки:*"]
    for t in trades:
        side = "🟢 ЛОНГ" if t["direction"] == "buy" else "🔴 ШОРТ"
        lines.append(
            f"#{t['id']} {side} {t['qty']} {t['ticker']} @ {t['entry_price']}  "
            f"({t.get('opened_at', '?')})"
        )
    return "\n".join(lines)


def portfolio_summary(user_id: int, current_prices: dict[str, float]) -> str:
    """
    current_prices: {ticker: price, ...} — текущие цены для расчёта P&L.
    Неизвестные тикеры показываются как "цена неизвестна".
    """
    trades = _load(user_id)
    if not trades:
        return "📊 Портфель пуст. Добавь сделки через /trade add"

    lines = ["📊 *ПОРТФЕЛЬ — P&L*", ""]
    total_pnl_usd = 0.0

    for t in trades:
        ticker  = t["ticker"]
        entry   = t["entry_price"]
        qty     = t["qty"]
        direction = t["direction"]
        cur_price = current_prices.get(ticker)

        if cur_price is None:
            lines.append(f"#{t['id']} {ticker}: цена не получена ⚠️")
            continue

        if direction == "buy":
            pnl = (cur_price - entry) * qty
            pct = (cur_price - entry) / entry * 100
        else:
            pnl = (entry - cur_price) * qty
            pct = (entry - cur_price) / entry * 100

        pnl_r = round(pnl, 2)
        pct_r = round(pct, 2)
        total_pnl_usd += pnl_r
        sign = "+" if pnl_r >= 0 else ""
        emoji = "🟢" if pnl_r >= 0 else "🔴"
        side = "ЛОНГ" if direction == "buy" else "ШОРТ"
        lines.append(
            f"{emoji} #{t['id']} {side} {qty} {ticker}"
            f"\n   Вход: {entry} → Сейчас: {cur_price}"
            f"\n   P&L: {sign}{pnl_r} USD ({sign}{pct_r}%)"
        )

    total_sign = "+" if total_pnl_usd >= 0 else ""
    total_emoji = "🟢" if total_pnl_usd >= 0 else "🔴"
    lines.append("")
    lines.append(f"{total_emoji} *Итого P&L: {total_sign}{round(total_pnl_usd, 2)} USD*")
    return "\n".join(lines)
