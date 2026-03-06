"""handlers/watchlist.py — Watchlist FSM handlers."""
from __future__ import annotations
import asyncio
import logging
from datetime import datetime, timezone
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from data_provider import get_market_data
from utils import validate_ticker
from bot_globals import (
    MENU, WATCHLIST_MENU, WATCHLIST_ADD, WATCHLIST_REMOVE, WATCHLIST_PICK_TICKER,
    watchlist_menu_markup, popular_categories_markup,
    _deny_if_unauthorized, _is_back, _is_categories, _is_prev, _is_next, _normalize_nav_text,
    _normalize_ticker, _run_with_timeout, _cache_get, _cache_set, _safe_edit_status,
    _wl_load, _wl_add, _wl_remove, _wl_format_list, _wl_remove_markup, _wl_picker_markup,
    _build_wl_tickers_markup, _clamp_page, _main_markup, _sync_wl_to_bot_data,
    POPULAR_TICKERS, MARKET_CACHE_TTL_SEC, ReplyKeyboardMarkup,
)

async def open_watchlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    user_id = update.effective_user.id
    tickers = _wl_load(user_id)
    text = _wl_format_list(tickers)
    await update.message.reply_text(text, reply_markup=watchlist_menu_markup)
    return WATCHLIST_MENU


async def open_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    await update.message.reply_text(
        "💼 *Портфель*\n\n"
        "📊 P&L — текущая доходность по открытым позициям\n"
        "📋 Мои сделки — список всех открытых позиций\n"
        "➕ Добавить сделку — зафиксировать вход\n"
        "❌ Закрыть сделку — удалить позицию по номеру\n"
        "📈 Бэктест — точность прогнозов по тикеру\n\n"
        "Также доступны команды:\n"
        "`/trade add AAPL buy 185.50 5`\n"
        "`/portfolio`\n"
        "`/backtest AAPL`",
        parse_mode="Markdown",
        reply_markup=portfolio_menu_markup,
    )
    return PORTFOLIO_MENU


async def portfolio_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    text = (update.message.text or "").strip()

    if _is_back(text):
        await update.message.reply_text("↩️ Главное меню.", reply_markup=_main_markup(context))
        return MENU

    if "p&l" in text.lower() or "портфель" in text.lower():
        return await _portfolio_pnl_inline(update, context)

    if "мои сделки" in text.lower():
        user_id = update.effective_user.id
        result = trade_list(user_id)
        await update.message.reply_text(result, parse_mode="Markdown", reply_markup=portfolio_menu_markup)
        return PORTFOLIO_MENU

    if "добавить сделку" in text.lower():
        user_id = update.effective_user.id
        context.user_data.pop("pt_add_step", None)
        context.user_data.pop("pt_add_ticker", None)
        context.user_data.pop("pt_add_direction", None)
        context.user_data.pop("pt_add_price", None)
        return await _pa_step_ticker(update, context)

    if "закрыть сделку" in text.lower():
        user_id = update.effective_user.id
        from portfolio_tracker import _load as _pt_load
        trades = _pt_load(user_id)
        if not trades:
            await update.message.reply_text(
                "📋 Нет открытых сделок для закрытия.",
                reply_markup=portfolio_menu_markup,
            )
            return PORTFOLIO_MENU
        # Build buttons: one per open trade
        rows = []
        for t in trades:
            side = "🟢" if t["direction"] == "buy" else "🔴"
            label = f"{side} #{t['id']} {t['ticker']} {t['qty']}@{t['entry_price']}"
            rows.append([label])
        rows.append(["↩️ Назад"])
        await update.message.reply_text(
            "Выбери сделку для закрытия:",
            reply_markup=ReplyKeyboardMarkup(rows, resize_keyboard=True),
        )
        context.user_data["pt_close_step"] = "pick"
        return PORTFOLIO_CLOSE

    if "бэктест" in text.lower():
        user_id = update.effective_user.id
        wl = _wl_load(user_id)
        rows = [["📊 Общая таблица"]]
        for i in range(0, len(wl), 2):
            rows.append(wl[i:i+2])
        rows.append(["↩️ Назад"])
        await update.message.reply_text(
            "Выбери тикер для бэктеста:",
            reply_markup=ReplyKeyboardMarkup(rows, resize_keyboard=True),
        )
        context.user_data["awaiting_backtest"] = True
        return PORTFOLIO_CLOSE

    await update.message.reply_text("Выбери действие из меню.", reply_markup=portfolio_menu_markup)
    return PORTFOLIO_MENU


async def _portfolio_pnl_inline(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    from portfolio_tracker import _load as _pt_load
    trades = _pt_load(user_id)
    if not trades:
        await update.message.reply_text(
            "📊 Портфель пуст.\nДобавь сделки через кнопку *➕ Добавить сделку* или командой `/trade add`",
            parse_mode="Markdown",
            reply_markup=portfolio_menu_markup,
        )
        return PORTFOLIO_MENU

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
    await update.message.reply_text("Вернуться:", reply_markup=portfolio_menu_markup)
    return PORTFOLIO_MENU


def _pa_ticker_markup(user_id: int) -> ReplyKeyboardMarkup:
    """Step 1: watchlist tickers as buttons + manual option."""
    wl = _wl_load(user_id)
    rows = []
    row = []
    for t in wl[:12]:  # max 12 watchlist tickers
        row.append(t)
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append(['✏️ Ввести тикер вручную'])
    rows.append(['↩️ Назад'])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _pa_direction_markup() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [['🟢 ЛОНГ (buy)', '🔴 ШОРТ (sell)'], ['↩️ Назад']],
        resize_keyboard=True,
    )


def _pa_price_markup(cur_price: float | None) -> ReplyKeyboardMarkup:
    rows = []
    if cur_price:
        rows.append([f'💰 По текущей: {cur_price}'])
    rows.append(['✏️ Ввести цену вручную'])
    rows.append(['↩️ Назад'])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _pa_qty_markup(ticker: str) -> ReplyKeyboardMarkup:
    """Quick quantity presets depending on ticker type."""
    t = ticker.upper()
    if "BTC" in t:
        presets = ["0.001", "0.01", "0.05", "0.1", "0.5", "1"]
    elif any(x in t for x in ["ETH", "SOL", "BNB"]):
        presets = ["0.01", "0.1", "0.5", "1", "5", "10"]
    elif "USD" in t or "=X" in t or "=F" in t:
        presets = ["0.01", "0.1", "1", "5", "10", "100"]
    else:
        presets = ["1", "2", "5", "10", "50", "100"]
    row1 = presets[:3]
    row2 = presets[3:]
    return ReplyKeyboardMarkup(
        [row1, row2, ['✏️ Ввести своё количество'], ['↩️ Назад']],
        resize_keyboard=True,
    )


async def _pa_step_ticker(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    wl = _wl_load(user_id)
    hint = "из вотчлиста или введи вручную" if wl else "введи тикер"
    await update.message.reply_text(
        f"➕ *Добавить сделку — Шаг 1/4*\n\nВыбери тикер {hint}:",
        parse_mode="Markdown",
        reply_markup=_pa_ticker_markup(user_id),
    )
    context.user_data["pt_add_step"] = "ticker"
    return PORTFOLIO_ADD


async def portfolio_add_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    text = (update.message.text or "").strip()

    if _is_back(text):
        # clear wizard state
        for k in ("pt_add_step", "pt_add_ticker", "pt_add_direction", "pt_add_price"):
            context.user_data.pop(k, None)
        await update.message.reply_text("↩️ Меню портфеля.", reply_markup=portfolio_menu_markup)
        return PORTFOLIO_MENU

    step = context.user_data.get("pt_add_step", "ticker")

    # ── Step 1: ticker ──
    if step == "ticker":
        if text == "✏️ Ввести тикер вручную":
            await update.message.reply_text(
                "Введи тикер (например: AAPL, BTC-USD, EURUSD=X):",
                reply_markup=ReplyKeyboardMarkup([['↩️ Назад']], resize_keyboard=True),
            )
            context.user_data["pt_add_step"] = "ticker_manual"
            return PORTFOLIO_ADD
        ticker = _normalize_ticker(text)
        if not validate_ticker(ticker):
            await update.message.reply_text("⚠️ Некорректный тикер. Попробуй ещё раз.")
            return PORTFOLIO_ADD
        context.user_data["pt_add_ticker"] = ticker
        # fetch current price for hint
        cur_price = None
        try:
            cached = _cache_get("market", ticker, MARKET_CACHE_TTL_SEC)
            if cached:
                cur_price = cached["current_price"]
        except Exception:
            pass
        context.user_data["pt_add_cur_price"] = cur_price
        await update.message.reply_text(
            f"➕ *Шаг 2/4 — Направление*\n\nТикер: `{ticker}`\nВыбери тип сделки:",
            parse_mode="Markdown",
            reply_markup=_pa_direction_markup(),
        )
        context.user_data["pt_add_step"] = "direction"
        return PORTFOLIO_ADD

    # ── Step 1b: manual ticker input ──
    if step == "ticker_manual":
        ticker = _normalize_ticker(text)
        if not validate_ticker(ticker):
            await update.message.reply_text("⚠️ Некорректный тикер. Попробуй ещё раз.")
            return PORTFOLIO_ADD
        context.user_data["pt_add_ticker"] = ticker
        cur_price = None
        try:
            cached = _cache_get("market", ticker, MARKET_CACHE_TTL_SEC)
            if cached:
                cur_price = cached["current_price"]
        except Exception:
            pass
        context.user_data["pt_add_cur_price"] = cur_price
        await update.message.reply_text(
            f"➕ *Шаг 2/4 — Направление*\n\nТикер: `{ticker}`\nВыбери тип сделки:",
            parse_mode="Markdown",
            reply_markup=_pa_direction_markup(),
        )
        context.user_data["pt_add_step"] = "direction"
        return PORTFOLIO_ADD

    # ── Step 2: direction ──
    if step == "direction":
        ticker = context.user_data.get("pt_add_ticker", "?")
        if "лонг" in text.lower() or "buy" in text.lower():
            direction = "buy"
            dir_label = "🟢 ЛОНГ"
        elif "шорт" in text.lower() or "sell" in text.lower():
            direction = "sell"
            dir_label = "🔴 ШОРТ"
        else:
            await update.message.reply_text("Выбери ЛОНГ или ШОРТ кнопкой.", reply_markup=_pa_direction_markup())
            return PORTFOLIO_ADD
        context.user_data["pt_add_direction"] = direction
        cur_price = context.user_data.get("pt_add_cur_price")
        # if no cached price, try to load it now
        if not cur_price:
            msg_wait = await update.message.reply_text(f"⏳ Получаю текущую цену {ticker}...")
            try:
                d = await _run_with_timeout(get_market_data, ticker, timeout=20)
                if d:
                    cur_price = d["current_price"]
                    _cache_set("market", ticker, d, MARKET_CACHE_TTL_SEC)
                    context.user_data["pt_add_cur_price"] = cur_price
            except Exception:
                pass
            await msg_wait.delete()
        await update.message.reply_text(
            f"➕ *Шаг 3/4 — Цена входа*\n\nТикер: `{ticker}` | {dir_label}\n"
            + (f"Текущая цена: *{cur_price}*\n" if cur_price else "")
            + "\nНажми кнопку или введи цену вручную:",
            parse_mode="Markdown",
            reply_markup=_pa_price_markup(cur_price),
        )
        context.user_data["pt_add_step"] = "price"
        return PORTFOLIO_ADD

    # ── Step 3: price ──
    if step == "price":
        ticker = context.user_data.get("pt_add_ticker", "?")
        direction = context.user_data.get("pt_add_direction", "buy")
        dir_label = "🟢 ЛОНГ" if direction == "buy" else "🔴 ШОРТ"

        if text == "✏️ Ввести цену вручную":
            await update.message.reply_text(
                "Введи цену входа (число):",
                reply_markup=ReplyKeyboardMarkup([['↩️ Назад']], resize_keyboard=True),
            )
            context.user_data["pt_add_step"] = "price_manual"
            return PORTFOLIO_ADD

        # Button "💰 По текущей: XXX" or raw number
        raw = text.replace("💰 По текущей:", "").replace("💰 По текущей", "").strip()
        try:
            price = float(raw)
        except ValueError:
            await update.message.reply_text("⚠️ Некорректное значение. Введи число или нажми кнопку.")
            return PORTFOLIO_ADD
        context.user_data["pt_add_price"] = price
        await update.message.reply_text(
            f"➕ *Шаг 4/4 — Количество*\n\nТикер: `{ticker}` | {dir_label} | Цена: *{price}*\n\nВыбери количество:",
            parse_mode="Markdown",
            reply_markup=_pa_qty_markup(ticker),
        )
        context.user_data["pt_add_step"] = "qty"
        return PORTFOLIO_ADD

    # ── Step 3b: manual price ──
    if step == "price_manual":
        ticker = context.user_data.get("pt_add_ticker", "?")
        direction = context.user_data.get("pt_add_direction", "buy")
        dir_label = "🟢 ЛОНГ" if direction == "buy" else "🔴 ШОРТ"
        try:
            price = float(text.replace(",", "."))
        except ValueError:
            await update.message.reply_text("⚠️ Введи корректное число. Например: 185.50")
            return PORTFOLIO_ADD
        context.user_data["pt_add_price"] = price
        await update.message.reply_text(
            f"➕ *Шаг 4/4 — Количество*\n\nТикер: `{ticker}` | {dir_label} | Цена: *{price}*\n\nВыбери количество:",
            parse_mode="Markdown",
            reply_markup=_pa_qty_markup(ticker),
        )
        context.user_data["pt_add_step"] = "qty"
        return PORTFOLIO_ADD

    # ── Step 4: quantity ──
    if step == "qty":
        ticker = context.user_data.get("pt_add_ticker", "?")
        direction = context.user_data.get("pt_add_direction", "buy")
        price = context.user_data.get("pt_add_price", 0)

        if text == "✏️ Ввести своё количество":
            await update.message.reply_text(
                "Введи количество (число):",
                reply_markup=ReplyKeyboardMarkup([['↩️ Назад']], resize_keyboard=True),
            )
            context.user_data["pt_add_step"] = "qty_manual"
            return PORTFOLIO_ADD
        try:
            qty = float(text.replace(",", "."))
        except ValueError:
            await update.message.reply_text("⚠️ Некорректное значение. Нажми кнопку или введи число.")
            return PORTFOLIO_ADD

        result = trade_add(update.effective_user.id, ticker, direction, price, qty)
        for k in ("pt_add_step", "pt_add_ticker", "pt_add_direction", "pt_add_price", "pt_add_cur_price"):
            context.user_data.pop(k, None)
        await update.message.reply_text(result, reply_markup=portfolio_menu_markup)
        return PORTFOLIO_MENU

    # ── Step 4b: manual qty ──
    if step == "qty_manual":
        ticker = context.user_data.get("pt_add_ticker", "?")
        direction = context.user_data.get("pt_add_direction", "buy")
        price = context.user_data.get("pt_add_price", 0)
        try:
            qty = float(text.replace(",", "."))
        except ValueError:
            await update.message.reply_text("⚠️ Введи корректное число. Например: 0.5 или 10")
            return PORTFOLIO_ADD
        result = trade_add(update.effective_user.id, ticker, direction, price, qty)
        for k in ("pt_add_step", "pt_add_ticker", "pt_add_direction", "pt_add_price", "pt_add_cur_price"):
            context.user_data.pop(k, None)
        await update.message.reply_text(result, reply_markup=portfolio_menu_markup)
        return PORTFOLIO_MENU

    # fallback
    return await _pa_step_ticker(update, context)


async def portfolio_close_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    text = (update.message.text or "").strip()

    if _is_back(text):
        context.user_data.pop("pt_close_step", None)
        context.user_data.pop("pt_close_trade", None)
        await update.message.reply_text("↩️ Меню портфеля.", reply_markup=portfolio_menu_markup)
        return PORTFOLIO_MENU

    # бэктест — выбор тикера из кнопок
    if context.user_data.get("awaiting_backtest"):
        context.user_data.pop("awaiting_backtest", None)
        if "общая таблица" in text.lower():
            bt_text = build_per_ticker_accuracy(min_forecasts=1)
        else:
            ticker = _normalize_ticker(text)
            bt_text = build_ticker_backtest(ticker) if ticker else build_per_ticker_accuracy(min_forecasts=1)
        await update.message.reply_text(f"```\n{bt_text[:3800]}\n```", parse_mode="Markdown", reply_markup=portfolio_menu_markup)
        return PORTFOLIO_MENU

    step = context.user_data.get("pt_close_step", "pick")

    # ── Шаг 1: выбор сделки кнопкой ──
    if step == "pick":
        user_id = update.effective_user.id
        from portfolio_tracker import _load as _pt_load
        trades = _pt_load(user_id)
        # parse trade id from button label "🟢 #3 AAPL 5@185.5"
        trade_id = None
        for t in trades:
            if f"#{t['id']} " in text:
                trade_id = t["id"]
                break
        if trade_id is None:
            await update.message.reply_text("⚠️ Не понял выбор. Нажми кнопку с нужной сделкой.")
            return PORTFOLIO_CLOSE

        trade = next(t for t in trades if t["id"] == trade_id)
        context.user_data["pt_close_trade"] = trade

        # Get current price for hint
        cur_price = None
        try:
            cached = _cache_get("market", trade["ticker"], MARKET_CACHE_TTL_SEC)
            if cached:
                cur_price = cached["current_price"]
            else:
                msg_wait = await update.message.reply_text(f"⏳ Получаю цену {trade['ticker']}...")
                try:
                    d = await _run_with_timeout(get_market_data, trade["ticker"], timeout=20)
                    if d:
                        cur_price = d["current_price"]
                        _cache_set("market", trade["ticker"], d, MARKET_CACHE_TTL_SEC)
                except Exception:
                    pass
                await msg_wait.delete()
        except Exception:
            pass

        side = "🟢 ЛОНГ" if trade["direction"] == "buy" else "🔴 ШОРТ"
        price_rows = []
        if cur_price:
            price_rows.append([f"💰 По текущей: {cur_price}"])
        price_rows.append(["✏️ Ввести цену закрытия"])
        price_rows.append(["↩️ Назад"])

        await update.message.reply_text(
            f"📌 Закрыть сделку:\n"
            f"`{trade['ticker']}` | {side} | {trade['qty']} шт. @ {trade['entry_price']}\n\n"
            f"По какой цене закрываешь?",
            parse_mode="Markdown",
            reply_markup=ReplyKeyboardMarkup(price_rows, resize_keyboard=True),
        )
        context.user_data["pt_close_step"] = "price"
        return PORTFOLIO_CLOSE

    # ── Шаг 2: цена закрытия ──
    if step == "price":
        trade = context.user_data.get("pt_close_trade")
        if not trade:
            await update.message.reply_text("⚠️ Ошибка. Начни заново.", reply_markup=portfolio_menu_markup)
            return PORTFOLIO_MENU

        if text == "✏️ Ввести цену закрытия":
            await update.message.reply_text(
                "Введи цену закрытия (число):",
                reply_markup=ReplyKeyboardMarkup([["↩️ Назад"]], resize_keyboard=True),
            )
            context.user_data["pt_close_step"] = "price_manual"
            return PORTFOLIO_CLOSE

        raw = text.replace("💰 По текущей:", "").strip()
        try:
            close_price = float(raw.replace(",", "."))
        except ValueError:
            await update.message.reply_text("⚠️ Некорректная цена. Нажми кнопку или введи число.")
            return PORTFOLIO_CLOSE

        return await _do_close_trade(update, context, trade["id"], close_price)

    # ── Шаг 2b: ручная цена ──
    if step == "price_manual":
        trade = context.user_data.get("pt_close_trade")
        if not trade:
            await update.message.reply_text("⚠️ Ошибка. Начни заново.", reply_markup=portfolio_menu_markup)
            return PORTFOLIO_MENU
        try:
            close_price = float(text.replace(",", "."))
        except ValueError:
            await update.message.reply_text("⚠️ Введи корректное число. Например: 190.50")
            return PORTFOLIO_CLOSE

        return await _do_close_trade(update, context, trade["id"], close_price)

    await update.message.reply_text("Выбери действие.", reply_markup=portfolio_menu_markup)
    return PORTFOLIO_MENU


async def _do_close_trade(update, context, trade_id: int, close_price: float):
    """Finalize trade close, show P&L, return to portfolio menu."""
    user_id = update.effective_user.id
    msg, _ = trade_close_with_pnl(user_id, trade_id, close_price)
    context.user_data.pop("pt_close_step", None)
    context.user_data.pop("pt_close_trade", None)
    await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=portfolio_menu_markup)
    return PORTFOLIO_MENU


async def watchlist_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    text = (update.message.text or "").strip()
    normalized = _normalize_nav_text(text)

    if _is_back(text):
        await update.message.reply_text("↩️ Главное меню.", reply_markup=_main_markup(context))
        return MENU

    if "скан" in normalized:
        return await watchlist_scan(update, context)

    if "добавить" in normalized:
        context.user_data["wl_pick_category"] = None
        context.user_data["wl_pick_page"] = 0
        await update.message.reply_text(
            "Выбери категорию тикеров для добавления в вотчлист:",
            reply_markup=popular_categories_markup,
        )
        return WATCHLIST_PICK_TICKER

    if "удалить" in normalized:
        user_id = update.effective_user.id
        tickers = _wl_load(user_id)
        if not tickers:
            await update.message.reply_text("📌 Вотчлист пуст.", reply_markup=watchlist_menu_markup)
            return WATCHLIST_MENU
        await update.message.reply_text(
            "Нажми на тикер, который хочешь удалить:",
            reply_markup=_wl_remove_markup(tickers),
        )
        return WATCHLIST_REMOVE

    if "список" in normalized or "мой" in normalized:
        user_id = update.effective_user.id
        tickers = _wl_load(user_id)
        await update.message.reply_text(_wl_format_list(tickers), reply_markup=watchlist_menu_markup)
        return WATCHLIST_MENU

    await update.message.reply_text("Выбери действие:", reply_markup=watchlist_menu_markup)
    return WATCHLIST_MENU


async def watchlist_add_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    raw = (update.message.text or "").strip()
    if _is_back(raw):
        user_id = update.effective_user.id
        tickers = _wl_load(user_id)
        await update.message.reply_text(_wl_format_list(tickers), reply_markup=watchlist_menu_markup)
        return WATCHLIST_MENU
    ticker = _normalize_ticker(raw)
    if not validate_ticker(ticker):
        await update.message.reply_text(
            "❌ Некорректный тикер. Пример: BTC-USD, AAPL, EURUSD=X\nПопробуй ещё:",
            reply_markup=ReplyKeyboardMarkup([['↩️ Назад']], resize_keyboard=True),
        )
        return WATCHLIST_ADD
    user_id = update.effective_user.id
    msg = _wl_add(user_id, ticker)
    tickers = _wl_load(user_id)
    _sync_wl_to_bot_data(context, user_id)
    await update.message.reply_text(f"{msg}\n\n{_wl_format_list(tickers)}", reply_markup=watchlist_menu_markup)
    return WATCHLIST_MENU


async def watchlist_remove_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    raw = (update.message.text or "").strip()
    if _is_back(raw):
        user_id = update.effective_user.id
        tickers = _wl_load(user_id)
        await update.message.reply_text(_wl_format_list(tickers), reply_markup=watchlist_menu_markup)
        return WATCHLIST_MENU
    # Button format: "❌ TICKER"
    if raw.startswith("❌ "):
        ticker = raw[2:].strip()
    else:
        ticker = _normalize_ticker(raw)
    user_id = update.effective_user.id
    msg = _wl_remove(user_id, ticker)
    tickers = _wl_load(user_id)
    _sync_wl_to_bot_data(context, user_id)
    if tickers:
        await update.message.reply_text(msg, reply_markup=_wl_remove_markup(tickers))
        return WATCHLIST_REMOVE
    await update.message.reply_text(f"{msg}\n\n📌 Вотчлист пуст.", reply_markup=watchlist_menu_markup)
    return WATCHLIST_MENU


async def watchlist_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    user_id = update.effective_user.id
    tickers = _wl_load(user_id)
    if not tickers:
        await update.message.reply_text("📌 Вотчлист пуст.", reply_markup=watchlist_menu_markup)
        return WATCHLIST_MENU

    status = await update.message.reply_text(f"🔍 Сканирую вотчлист ({len(tickers)} тикеров)...")
    _BIAS_EMOJI = {"Бычий": "🐂", "Медвежий": "🐻", "Нейтральный": "⚖️"}
    rows = []
    total = len(tickers)
    for idx, ticker in enumerate(tickers, start=1):
        if idx % 3 == 1 and idx > 1:
            try:
                await status.edit_text(f"🔍 Сканирую вотчлист... ({idx-1}/{total})")
            except Exception:
                pass
        try:
            data = _cache_get("market", ticker, MARKET_CACHE_TTL_SEC)
            if not data:
                data = await _run_with_timeout(get_market_data, ticker, timeout=45)
                if data:
                    _cache_set("market", ticker, data, MARKET_CACHE_TTL_SEC)
        except Exception:
            rows.append(f"  ❓ {ticker:10s} — ошибка получения данных")
            continue
        if not data:
            rows.append(f"  ❓ {ticker:10s} — нет данных")
            continue
        rule = data.get("rule_forecast", {})
        bias = rule.get("bias", "?")
        bull = rule.get("bullish_probability", "?")
        bear = rule.get("bearish_probability", "?")
        action = rule.get("action", "?")
        rsi = data.get("rsi_14", "?")
        conf = rule.get("confidence", "?")
        emoji = _BIAS_EMOJI.get(bias, "⚖️")
        rsi_str = f"{rsi:.1f}" if isinstance(rsi, float) else str(rsi)
        rows.append(
            f"  {emoji} {ticker:10s} | {bias:10s} | ↑{bull}% ↓{bear}% | RSI {rsi_str} | {action} [{conf}]"
        )

    now_utc = datetime.now(timezone.utc).strftime("%d.%m %H:%M UTC")
    header = f"📌 СКАН ВОТЧЛИСТА — {now_utc}\n{'─'*48}\n"
    body = "\n".join(rows)
    result = header + body
    await _safe_edit_status(status, result[:3900])
    await update.message.reply_text("Скан завершён.", reply_markup=watchlist_menu_markup)
    return WATCHLIST_MENU


async def watchlist_picker_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles inline keyboard presses from the ticker picker."""
    query = update.callback_query
    await query.answer()  # removes spinner
    data = query.data or ""
    user_id = update.effective_user.id

    if data == "wl_noop":
        return  # header label or already-added ticker, do nothing

    if data == "wl_close":
        await query.edit_message_text("Выбор тикеров закрыт.")
        return

    if data == "wl_manual":
        await query.edit_message_text(
            "✏️ Введи тикер вручную (например: AAPL, BTC-USD, EURUSD=X):"
        )
        return

    if data.startswith("wl_add:"):
        ticker = data[7:]
        msg = _wl_add(user_id, ticker)
        user_tickers = _wl_load(user_id)
        _sync_wl_to_bot_data(context, user_id)
        # Refresh the picker to mark added ticker with ✅
        try:
            await query.edit_message_reply_markup(reply_markup=_wl_picker_markup(user_tickers))
        except Exception:
            pass
        # Send status as a fresh message
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"{msg}\n\n{_wl_format_list(user_tickers)}",
        )
        return


# ──────────────────────── END WATCHLIST ────────────────────────


async def watchlist_pick_ticker_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the WATCHLIST_PICK_TICKER state — same category/page navigation as AI-Analysis,
    but pressing a ticker adds it to the watchlist instead of running analysis."""
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    raw_text = (update.message.text or "").strip()
    user_id = update.effective_user.id

    if _is_back(raw_text):
        # Back from category list → back to watchlist menu
        cat = context.user_data.get("wl_pick_category")
        if cat:
            context.user_data["wl_pick_category"] = None
            context.user_data["wl_pick_page"] = 0
            await update.message.reply_text(
                "Выбери категорию тикеров:", reply_markup=popular_categories_markup
            )
            return WATCHLIST_PICK_TICKER
        tickers = _wl_load(user_id)
        await update.message.reply_text(_wl_format_list(tickers), reply_markup=watchlist_menu_markup)
        return WATCHLIST_MENU

    if _is_categories(raw_text):
        context.user_data["wl_pick_category"] = None
        context.user_data["wl_pick_page"] = 0
        await update.message.reply_text("Выбери категорию:", reply_markup=popular_categories_markup)
        return WATCHLIST_PICK_TICKER

    if raw_text in POPULAR_TICKERS:
        context.user_data["wl_pick_category"] = raw_text
        context.user_data["wl_pick_page"] = 0
        await update.message.reply_text(
            f"Категория: {raw_text}. Нажми тикер — он добавится в вотчлист:",
            reply_markup=_build_wl_tickers_markup(raw_text, 0, _wl_load(user_id)),
        )
        return WATCHLIST_PICK_TICKER

    selected_category = context.user_data.get("wl_pick_category")
    selected_page = context.user_data.get("wl_pick_page", 0)

    if not selected_category:
        await update.message.reply_text("Выбери категорию:", reply_markup=popular_categories_markup)
        return WATCHLIST_PICK_TICKER

    if _is_prev(raw_text):
        selected_page = _clamp_page(selected_category, selected_page - 1)
        context.user_data["wl_pick_page"] = selected_page
        await update.message.reply_text(
            f"Страница {selected_page + 1}:",
            reply_markup=_build_wl_tickers_markup(selected_category, selected_page, _wl_load(user_id)),
        )
        return WATCHLIST_PICK_TICKER

    if _is_next(raw_text):
        selected_page = _clamp_page(selected_category, selected_page + 1)
        context.user_data["wl_pick_page"] = selected_page
        await update.message.reply_text(
            f"Страница {selected_page + 1}:",
            reply_markup=_build_wl_tickers_markup(selected_category, selected_page, _wl_load(user_id)),
        )
        return WATCHLIST_PICK_TICKER

    # Пользователь нажал тикер из списка или ввёл вручную
    ticker = _normalize_ticker(raw_text)
    # Strip ✅ prefix (already-added marker)
    if ticker.startswith("✅"):
        ticker = ticker[1:].strip()
    if not validate_ticker(ticker):
        await update.message.reply_text(
            "❌ Некорректный тикер. Нажми кнопку или введи вручную (напр. AAPL, BTC-USD):"
        )
        return WATCHLIST_PICK_TICKER

    msg = _wl_add(user_id, ticker)
    user_tickers = _wl_load(user_id)
    _sync_wl_to_bot_data(context, user_id)
    # Refresh the page to mark the added ticker with ✅
    await update.message.reply_text(
        f"{msg}\n\n{_wl_format_list(user_tickers)}",
        reply_markup=_build_wl_tickers_markup(selected_category, selected_page, user_tickers),
    )
    return WATCHLIST_PICK_TICKER


