"""handlers/commands.py — Standalone command handlers + menu_router."""
from __future__ import annotations
import asyncio
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from data_provider import get_market_data
from forecast_tracker import build_per_ticker_accuracy, build_ticker_backtest
from portfolio_tracker import trade_add, trade_close, trade_list, portfolio_summary
from utils import normalize_ticker, validate_ticker
from bot_globals import (
    MENU,
    _deny_if_unauthorized, _inc_stat, _get_stats_text, _get_alerts,
    _main_markup, _normalize_ticker, _normalize_nav_text, _is_back, _is_tickers_menu,
    _wl_add, _run_with_timeout, _cache_get, _cache_set,
    MARKET_CACHE_TTL_SEC,
    cancel, start, help_command,
)
from handlers.analysis import ask_analysis, open_popular_tickers, open_models
from handlers.news import ask_news
from handlers.signals import run_last_signal
from handlers.settings import open_settings
from handlers.forecast import open_forecast_menu
from handlers.watchlist import open_watchlist
from handlers.portfolio import open_portfolio

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return
    _inc_stat(context, "cmd_stats")
    await update.message.reply_text(_get_stats_text(context), reply_markup=_main_markup(context))



async def lang_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return
    _inc_stat(context, "cmd_lang")
    parts = (update.message.text or "").split()
    if len(parts) < 2 or parts[1].lower() not in {"ru", "en"}:
        await update.message.reply_text("Использование: /lang ru или /lang en")
        return
    context.user_data["lang"] = parts[1].lower()
    await update.message.reply_text(f"✅ Язык установлен: {context.user_data['lang']}", reply_markup=_main_markup(context))



async def alert_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return
    _inc_stat(context, "cmd_alert")
    parts = (update.message.text or "").split()
    alerts = _get_alerts(context)
    # Store chat_id so background sync can find it
    context.user_data["_chat_id"] = str(update.effective_chat.id)

    _HELP = (
        "Использование:\n"
        "/alert list\n"
        "/alert add TICKER rsi_below 30\n"
        "/alert add TICKER rsi_above 70\n"
        "/alert add TICKER price_above 150.5\n"
        "/alert add TICKER price_below 100\n"
        "/alert del TICKER"
    )

    if len(parts) < 2 or parts[1].lower() == "list":
        if not alerts:
            await update.message.reply_text("Алертов пока нет.\n\n" + _HELP)
            return
        lines = ["🔔 Текущие алерты:"]
        for ticker, cfg in alerts.items():
            conds = []
            if cfg.get("rsi_below"):   conds.append(f"RSI<{cfg['rsi_below']}")
            if cfg.get("rsi_above"):   conds.append(f"RSI>{cfg['rsi_above']}")
            if cfg.get("price_above"): conds.append(f"цена>{cfg['price_above']}")
            if cfg.get("price_below"): conds.append(f"цена<{cfg['price_below']}")
            lines.append(f"  • {ticker}: {', '.join(conds) or 'н/д'}")
        await update.message.reply_text("\n".join(lines))
        return

    action = parts[1].lower()
    if action == "add" and len(parts) >= 5:
        ticker = normalize_ticker(parts[2])
        if not validate_ticker(ticker):
            await update.message.reply_text("Некорректный тикер.")
            return
        cond_type = parts[3].lower()
        if cond_type not in ("rsi_below", "rsi_above", "price_above", "price_below"):
            await update.message.reply_text(f"Неизвестный тип условия: {cond_type}\n\n" + _HELP)
            return
        try:
            value = float(parts[4])
        except ValueError:
            await update.message.reply_text("Значение должно быть числом.")
            return
        cfg = alerts.setdefault(ticker, {})
        cfg[cond_type] = value
        # Sync to bot_data for background job
        context.bot_data.setdefault("user_alerts_map", {})[str(update.effective_chat.id)] = dict(alerts)
        await update.message.reply_text(f"✅ Алерт добавлен: {ticker} {cond_type}={value}")
        return

    if action == "del" and len(parts) >= 3:
        ticker = normalize_ticker(parts[2])
        removed = alerts.pop(ticker, None)
        if removed:
            # Sync to bot_data
            context.bot_data.setdefault("user_alerts_map", {})[str(update.effective_chat.id)] = dict(alerts)
            await update.message.reply_text(f"🗑 Удалён алерт: {ticker}")
        else:
            await update.message.reply_text("Алерт не найден.")
        return

    await update.message.reply_text(_HELP)


# ──────────────────────── ФОНОВЫЕ ЗАДАЧИ ────────────────────────


async def compare_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сравнить два тикера: /compare AAPL MSFT"""
    if await _deny_if_unauthorized(update, context):
        return
    args = context.args or []
    if len(args) < 2:
        await update.message.reply_text(
            "Использование: `/compare ТИКЕР1 ТИКЕР2`\nПример: `/compare AAPL MSFT`",
            parse_mode="Markdown",
        )
        return

    t1 = _normalize_ticker(args[0])
    t2 = _normalize_ticker(args[1])
    msg = await update.message.reply_text(f"⏳ Загружаю данные для {t1} и {t2}...")

    try:
        data1, data2 = await asyncio.gather(
            _run_with_timeout(get_market_data, t1, timeout=45),
            _run_with_timeout(get_market_data, t2, timeout=45),
        )
    except Exception as exc:
        await msg.edit_text(f"❌ Ошибка загрузки: {exc}")
        return

    def _row(label, v1, v2):
        return f"  {label:<14} {str(v1):>10}   {str(v2):>10}"

    def _fmt_price(d):
        if not d:
            return "н/д"
        p = d.get("current_price")
        return f"{p:.2f}" if p else "н/д"

    def _fmt_rsi(d):
        if not d:
            return "н/д"
        v = d.get("rsi_14")
        return f"{v:.1f}" if isinstance(v, float) else "н/д"

    def _fmt_bias(d):
        if not d:
            return "н/д"
        return (d.get("rule_forecast") or {}).get("bias", "н/д")

    def _fmt_bull(d):
        if not d:
            return "н/д"
        v = (d.get("rule_forecast") or {}).get("bullish_probability")
        return f"{v}%" if v is not None else "н/д"

    def _fmt_adx(d):
        if not d:
            return "н/д"
        v = d.get("adx_14")
        return f"{v:.1f}" if isinstance(v, float) else "н/д"

    def _fmt_sar(d):
        if not d:
            return "н/д"
        trend = (d.get("sar") or {}).get("sar_trend", "н/д")
        return trend

    def _fmt_stoch(d):
        if not d:
            return "н/д"
        sig = (d.get("stochastic") or {}).get("signal", "н/д")
        return sig

    def _fmt_bb(d):
        if not d:
            return "н/д"
        v = (d.get("bollinger") or {}).get("bb_pct_b")
        return f"{v:.2f}" if isinstance(v, float) else "н/д"

    def _fmt_vol(d):
        if not d:
            return "н/д"
        v = d.get("annualized_volatility_pct")
        return f"{v:.1f}%" if isinstance(v, float) else "н/д"

    header = f"{'':16} {t1:>10}   {t2:>10}"
    sep = "─" * 42
    rows = [
        header, sep,
        _row("Цена",       _fmt_price(data1),  _fmt_price(data2)),
        _row("RSI",        _fmt_rsi(data1),    _fmt_rsi(data2)),
        _row("ADX",        _fmt_adx(data1),    _fmt_adx(data2)),
        _row("Bias",       _fmt_bias(data1),   _fmt_bias(data2)),
        _row("Бычьи %",    _fmt_bull(data1),   _fmt_bull(data2)),
        _row("SAR тренд",  _fmt_sar(data1),    _fmt_sar(data2)),
        _row("Stoch",      _fmt_stoch(data1),  _fmt_stoch(data2)),
        _row("BB %B",      _fmt_bb(data1),     _fmt_bb(data2)),
        _row("Волат.",     _fmt_vol(data1),    _fmt_vol(data2)),
    ]
    await msg.edit_text("```\n" + "\n".join(rows) + "\n```", parse_mode="Markdown")



async def trade_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/trade add TICKER buy/sell PRICE QTY  |  /trade close N  |  /trade list"""
    if await _deny_if_unauthorized(update, context):
        return
    args = context.args or []
    user_id = update.effective_user.id

    if not args or args[0] == "list":
        text = trade_list(user_id)
        await update.message.reply_text(text, parse_mode="Markdown")
        return

    sub = args[0].lower()
    if sub == "add":
        if len(args) < 5:
            await update.message.reply_text(
                "Использование: `/trade add TICKER buy|sell ЦЕНА КОЛ-ВО`\n"
                "Пример: `/trade add AAPL buy 185.50 5`",
                parse_mode="Markdown",
            )
            return
        ticker = _normalize_ticker(args[1])
        direction = args[2]
        try:
            price = float(args[3])
            qty   = float(args[4])
        except ValueError:
            await update.message.reply_text("❌ Цена и количество должны быть числами.")
            return
        result = trade_add(user_id, ticker, direction, price, qty)
        await update.message.reply_text(result)

    elif sub == "close":
        if len(args) < 2:
            await update.message.reply_text("Использование: `/trade close N` (номер сделки из /trade list)", parse_mode="Markdown")
            return
        try:
            trade_id = int(args[1])
        except ValueError:
            await update.message.reply_text("❌ Номер сделки должен быть целым числом.")
            return
        result = trade_close(user_id, trade_id)
        await update.message.reply_text(result)

    else:
        await update.message.reply_text(
            "Команды:\n"
            "`/trade list` — открытые сделки\n"
            "`/trade add TICKER buy|sell ЦЕНА QTY` — добавить\n"
            "`/trade close N` — закрыть сделку №N\n"
            "`/portfolio` — P&L по текущим ценам",
            parse_mode="Markdown",
        )



async def portfolio_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/portfolio — показать P&L по открытым позициям"""
    if await _deny_if_unauthorized(update, context):
        return
    user_id = update.effective_user.id
    from portfolio_tracker import _load as _pt_load
    trades = _pt_load(user_id)
    if not trades:
        await update.message.reply_text("📊 Портфель пуст. Добавь сделки через `/trade add`", parse_mode="Markdown")
        return

    tickers = list({t["ticker"] for t in trades})
    msg = await update.message.reply_text(f"⏳ Загружаю цены для {len(tickers)} тикеров...")

    prices: dict[str, float] = {}
    for ticker in tickers:
        cached = _cache_get("market", ticker, MARKET_CACHE_TTL_SEC)
        if cached:
            prices[ticker] = cached["current_price"]
        else:
            try:
                d = await _run_with_timeout(get_market_data, ticker, timeout=30)
                if d:
                    prices[ticker] = d["current_price"]
                    _cache_set("market", ticker, d, MARKET_CACHE_TTL_SEC)
            except Exception:
                pass

    text = portfolio_summary(user_id, prices)
    await msg.edit_text(text, parse_mode="Markdown")



async def backtest_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/backtest [TICKER] — показать точность прогнозов по тикеру"""
    if await _deny_if_unauthorized(update, context):
        return
    args = context.args or []
    if not args:
        # Без тикера — общая таблица по всем
        text = build_per_ticker_accuracy(min_forecasts=1)
        await update.message.reply_text(f"```\n{text[:3800]}\n```", parse_mode="Markdown")
        return
    ticker = _normalize_ticker(args[0])
    text = build_ticker_backtest(ticker)
    await update.message.reply_text(f"```\n{text[:3800]}\n```", parse_mode="Markdown")



async def menu_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    text = (update.message.text or "").strip()
    normalized = _normalize_nav_text(text)

    if "ai-анализ" in normalized or "ai анализ" in normalized:
        return await ask_analysis(update, context)
    if "модели" in normalized:
        return await open_models(update, context)
    if "новости" in normalized:
        return await ask_news(update, context)
    if "настройки" in normalized:
        return await open_settings(update, context)
    if _is_tickers_menu(text):
        return await open_popular_tickers(update, context)
    if "прогноз" in normalized:
        return await open_forecast_menu(update, context)
    if "вотчлист" in normalized or "watchlist" in normalized:
        # ⭐ В вотчлист — добавить последний проанализированный тикер
        if text == "⭐ В вотчлист":
            last = context.user_data.get("last_analyzed_ticker")
            if last:
                msg = _wl_add(update.effective_user.id, last)
                await update.message.reply_text(msg, reply_markup=_main_markup(context))
            else:
                await update.message.reply_text("⚠️ Сначала сделай анализ тикера.", reply_markup=_main_markup(context))
            return MENU
        return await open_watchlist(update, context)
    if "портфель" in normalized or "portfolio" in normalized:
        return await open_portfolio(update, context)
    if "помощ" in normalized:
        return await help_command(update, context)
    if "сигнал" in normalized:
        return await run_last_signal(update, context)
    if _is_back(text):
        return await cancel(update, context)

    await update.message.reply_text("Не понял команду. Выбери действие кнопкой ниже.", reply_markup=_main_markup(context))
    return MENU


