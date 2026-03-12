"""handlers/alerts.py — Alerts menu FSM handlers."""
from __future__ import annotations
import json
import logging
from pathlib import Path
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from utils import validate_ticker, normalize_ticker
from bot_globals import (
    MENU, ALERT_MENU, ALERT_ADD, ALERT_DEL,
    alerts_menu_markup,
    _deny_if_unauthorized, _is_back, _is_prev, _is_next, _main_markup,
    _get_alerts, ReplyKeyboardMarkup,
)

_FORECAST_LOG = Path("data/forecast_snapshots.jsonl")
_ALERT_PAGE_SIZE = 12   # 4 ряда по 3 тикера

_COND_MAP = {
    "📉 rsi ниже": "rsi_below",
    "📈 rsi выше": "rsi_above",
    "💹 цена выше": "price_above",
    "💰 цена ниже": "price_below",
}

_COND_MARKUP = ReplyKeyboardMarkup(
    [
        ['📉 RSI ниже', '📈 RSI выше'],
        ['💹 Цена выше', '💰 Цена ниже'],
        ['↩️ Назад'],
    ],
    resize_keyboard=True,
)

_BACK_MARKUP = ReplyKeyboardMarkup([['↩️ Назад']], resize_keyboard=True)


def _load_forecast_tickers() -> list:
    """Уникальные тикеры из снапшотов прогнозов (от новых к старым)."""
    if not _FORECAST_LOG.exists():
        return []
    seen = {}
    try:
        lines = _FORECAST_LOG.read_text(encoding="utf-8").splitlines()
    except Exception:
        return []
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
            sym = str(row.get("symbol", "")).strip().upper()
            if sym:
                seen[sym] = None
        except Exception:
            continue
    return list(seen.keys())


def _build_alert_ticker_markup(tickers: list, page: int) -> ReplyKeyboardMarkup:
    """Постраничная клавиатура тикеров прогноза (3 в ряду)."""
    total = max(1, (len(tickers) + _ALERT_PAGE_SIZE - 1) // _ALERT_PAGE_SIZE)
    page = max(0, min(page, total - 1))
    start = page * _ALERT_PAGE_SIZE
    items = tickers[start:start + _ALERT_PAGE_SIZE]

    rows = []
    for i in range(0, len(items), 3):
        rows.append(items[i:i + 3])

    nav = []
    if page > 0:
        nav.append('⬅️ Предыдущие')
    if page < total - 1:
        nav.append('➡️ Следующие')
    if nav:
        rows.append(nav)

    rows.append(['✏️ Ввести вручную', '↩️ Назад'])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _fmt_alerts(alerts: dict) -> str:
    if not alerts:
        return "🔔 У тебя пока нет алертов.\n\nДобавь через кнопку *➕ Добавить алерт*."
    lines = ["🔔 *Активные алерты:*"]
    for ticker, cfg in alerts.items():
        conds = []
        if cfg.get("rsi_below"):   conds.append(f"RSI < {cfg['rsi_below']}")
        if cfg.get("rsi_above"):   conds.append(f"RSI > {cfg['rsi_above']}")
        if cfg.get("price_above"): conds.append(f"цена > {cfg['price_above']}")
        if cfg.get("price_below"): conds.append(f"цена < {cfg['price_below']}")
        lines.append(f"  • *{ticker}*: {', '.join(conds) or 'н/д'}")
    lines.append("\n_Проверяются каждые 15 минут._")
    return "\n".join(lines)


def _sync_alerts(context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = str(context.user_data.get("_chat_id", ""))
    if chat_id:
        alerts = context.user_data.get("alerts", {})
        context.bot_data.setdefault("user_alerts_map", {})[chat_id] = dict(alerts)


def _clear_add_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    for k in ("alert_add_step", "alert_add_ticker", "alert_add_cond",
              "alert_ticker_page", "alert_forecast_tickers"):
        context.user_data.pop(k, None)


# ─── Точка входа ──────────────────────────────────────────────────────────────

async def open_alerts_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    context.user_data["_chat_id"] = str(update.effective_chat.id)
    _clear_add_state(context)
    alerts = _get_alerts(context)
    await update.message.reply_text(
        _fmt_alerts(alerts),
        parse_mode="Markdown",
        reply_markup=alerts_menu_markup,
    )
    return ALERT_MENU


# ─── Роутер меню алертов ──────────────────────────────────────────────────────

async def alerts_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    text = (update.message.text or "").strip()
    low = text.lower()

    if _is_back(text):
        _clear_add_state(context)
        await update.message.reply_text("↩️ Главное меню.", reply_markup=_main_markup(context))
        return MENU

    if "алерт" in low and "добавить" not in low and "удалить" not in low \
            and "очистить" not in low and "мои" not in low:
        _clear_add_state(context)
        alerts = _get_alerts(context)
        await update.message.reply_text(
            _fmt_alerts(alerts), parse_mode="Markdown", reply_markup=alerts_menu_markup
        )
        return ALERT_MENU

    if "мои алерты" in low:
        alerts = _get_alerts(context)
        await update.message.reply_text(
            _fmt_alerts(alerts), parse_mode="Markdown", reply_markup=alerts_menu_markup
        )
        return ALERT_MENU

    if "добавить" in low:
        return await _start_add_alert(update, context)

    if "удалить" in low:
        alerts = _get_alerts(context)
        if not alerts:
            await update.message.reply_text("Нет активных алертов.", reply_markup=alerts_menu_markup)
            return ALERT_MENU
        rows = [[t] for t in alerts.keys()]
        rows.append(['↩️ Назад'])
        await update.message.reply_text(
            "Выбери тикер для удаления алерта:",
            reply_markup=ReplyKeyboardMarkup(rows, resize_keyboard=True),
        )
        return ALERT_DEL

    if "очистить" in low:
        alerts = _get_alerts(context)
        count = len(alerts)
        alerts.clear()
        _sync_alerts(context)
        await update.message.reply_text(f"🧹 Удалено алертов: {count}.", reply_markup=alerts_menu_markup)
        return ALERT_MENU

    await update.message.reply_text("Выбери действие из меню.", reply_markup=alerts_menu_markup)
    return ALERT_MENU


# ─── Запуск добавления ────────────────────────────────────────────────────────

async def _start_add_alert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tickers = _load_forecast_tickers()
    if tickers:
        context.user_data["alert_forecast_tickers"] = tickers
        context.user_data["alert_ticker_page"] = 0
        context.user_data["alert_add_step"] = "pick_ticker"
        total_pages = max(1, (len(tickers) + _ALERT_PAGE_SIZE - 1) // _ALERT_PAGE_SIZE)
        markup = _build_alert_ticker_markup(tickers, 0)
        await update.message.reply_text(
            f"Выбери тикер из прогнозов (стр. 1/{total_pages}) или введи вручную:",
            reply_markup=markup,
        )
    else:
        context.user_data["alert_add_step"] = "ticker"
        await update.message.reply_text(
            "Введи тикер для алерта (например: TSLA, BTC-USD, SBER):",
            reply_markup=_BACK_MARKUP,
        )
    return ALERT_ADD


# ─── FSM: добавление алерта ───────────────────────────────────────────────────

async def alerts_add_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    text = (update.message.text or "").strip()

    if _is_back(text):
        _clear_add_state(context)
        alerts = _get_alerts(context)
        await update.message.reply_text(
            _fmt_alerts(alerts), parse_mode="Markdown", reply_markup=alerts_menu_markup
        )
        return ALERT_MENU

    step = context.user_data.get("alert_add_step", "ticker")

    # ── pick_ticker: выбор из списка прогнозов ────────────────────────────
    if step == "pick_ticker":
        tickers = context.user_data.get("alert_forecast_tickers", [])
        page = context.user_data.get("alert_ticker_page", 0)
        total_pages = max(1, (len(tickers) + _ALERT_PAGE_SIZE - 1) // _ALERT_PAGE_SIZE)

        if _is_prev(text):
            page = max(0, page - 1)
            context.user_data["alert_ticker_page"] = page
            await update.message.reply_text(
                f"Выбери тикер (стр. {page + 1}/{total_pages}):",
                reply_markup=_build_alert_ticker_markup(tickers, page),
            )
            return ALERT_ADD

        if _is_next(text):
            page = min(total_pages - 1, page + 1)
            context.user_data["alert_ticker_page"] = page
            await update.message.reply_text(
                f"Выбери тикер (стр. {page + 1}/{total_pages}):",
                reply_markup=_build_alert_ticker_markup(tickers, page),
            )
            return ALERT_ADD

        if "ввести вручную" in text.lower():
            context.user_data["alert_add_step"] = "ticker"
            await update.message.reply_text(
                "Введи тикер для алерта (например: TSLA, BTC-USD, SBER):",
                reply_markup=_BACK_MARKUP,
            )
            return ALERT_ADD

        ticker = normalize_ticker(text)
        if not validate_ticker(ticker):
            await update.message.reply_text(
                "Некорректный тикер. Выбери из списка или нажми «✏️ Ввести вручную»:",
                reply_markup=_build_alert_ticker_markup(tickers, page),
            )
            return ALERT_ADD

        context.user_data["alert_add_ticker"] = ticker
        context.user_data["alert_add_step"] = "condition"
        await update.message.reply_text(
            f"Тикер: *{ticker}*\nВыбери условие алерта:",
            parse_mode="Markdown",
            reply_markup=_COND_MARKUP,
        )
        return ALERT_ADD

    # ── ticker: ручной ввод ───────────────────────────────────────────────
    if step == "ticker":
        ticker = normalize_ticker(text)
        if not validate_ticker(ticker):
            await update.message.reply_text("Некорректный тикер. Попробуй ещё раз:", reply_markup=_BACK_MARKUP)
            return ALERT_ADD
        context.user_data["alert_add_ticker"] = ticker
        context.user_data["alert_add_step"] = "condition"
        await update.message.reply_text(
            f"Тикер: *{ticker}*\nВыбери условие алерта:",
            parse_mode="Markdown",
            reply_markup=_COND_MARKUP,
        )
        return ALERT_ADD

    # ── condition: выбор условия ──────────────────────────────────────────
    if step == "condition":
        cond_key = _COND_MAP.get(text.lower())
        if not cond_key:
            await update.message.reply_text("Выбери условие из меню:", reply_markup=_COND_MARKUP)
            return ALERT_ADD
        context.user_data["alert_add_cond"] = cond_key
        context.user_data["alert_add_step"] = "value"
        cond_hint = {
            "rsi_below": "RSI ниже (например: 30)",
            "rsi_above": "RSI выше (например: 70)",
            "price_above": "Цена выше (например: 250.0)",
            "price_below": "Цена ниже (например: 100.0)",
        }[cond_key]
        await update.message.reply_text(f"Введи значение — {cond_hint}:", reply_markup=_BACK_MARKUP)
        return ALERT_ADD

    # ── value: ввод числа ─────────────────────────────────────────────────
    if step == "value":
        try:
            value = float(text.replace(",", "."))
        except ValueError:
            await update.message.reply_text(
                "Введи числовое значение (например: 30 или 250.5):",
                reply_markup=_BACK_MARKUP,
            )
            return ALERT_ADD

        ticker = context.user_data.pop("alert_add_ticker", None)
        cond   = context.user_data.pop("alert_add_cond", None)
        _clear_add_state(context)

        if not ticker or not cond:
            await update.message.reply_text("Ошибка состояния, попробуй заново.", reply_markup=alerts_menu_markup)
            return ALERT_MENU

        alerts = _get_alerts(context)
        alerts.setdefault(ticker, {})[cond] = value
        _sync_alerts(context)

        cond_labels = {
            "rsi_below": f"RSI < {value}",
            "rsi_above": f"RSI > {value}",
            "price_above": f"Цена > {value}",
            "price_below": f"Цена < {value}",
        }
        await update.message.reply_text(
            f"✅ Алерт добавлен: *{ticker}* — {cond_labels[cond]}",
            parse_mode="Markdown",
            reply_markup=alerts_menu_markup,
        )
        return ALERT_MENU

    await update.message.reply_text("Введи значение:", reply_markup=_BACK_MARKUP)
    return ALERT_ADD


# ─── FSM: удаление алерта ─────────────────────────────────────────────────────

async def alerts_del_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    text = (update.message.text or "").strip()

    if _is_back(text):
        alerts = _get_alerts(context)
        await update.message.reply_text(
            _fmt_alerts(alerts), parse_mode="Markdown", reply_markup=alerts_menu_markup
        )
        return ALERT_MENU

    ticker = normalize_ticker(text)
    alerts = _get_alerts(context)
    removed = alerts.pop(ticker, None)
    if removed:
        _sync_alerts(context)
        await update.message.reply_text(
            f"🗑️ Алерт по *{ticker}* удалён.",
            parse_mode="Markdown",
            reply_markup=alerts_menu_markup,
        )
    else:
        await update.message.reply_text(f"Алерт по {ticker} не найден.", reply_markup=alerts_menu_markup)
    return ALERT_MENU
