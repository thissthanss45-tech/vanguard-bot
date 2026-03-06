"""handlers/signals.py — Trading signals FSM handlers."""
from __future__ import annotations
import asyncio
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from data_provider import get_market_data
from ai_engine import analyze_news
from utils import validate_ticker
from bot_globals import (
    MENU, SIGNAL_QUERY,
    back_markup,
    _deny_if_unauthorized, _is_back, _normalize_ticker,
    _run_with_timeout, _cache_get, _cache_set, _compact_signal_report,
    _main_markup, _inc_stat, _get_settings, _clear_user_flow_state, _safe_edit_status,
    MARKET_CACHE_TTL_SEC, NEWS_CACHE_TTL_SEC, AI_ANALYSIS_TIMEOUT_SEC,
    get_ticker_news_payload, cancel,
)

async def ask_signal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Введите тикер для итогового торгового сигнала:", reply_markup=back_markup)
    return SIGNAL_QUERY


async def run_signal_for_ticker(update: Update, context: ContextTypes.DEFAULT_TYPE, ticker: str):
    _inc_stat(context, "signal_runs")
    provider, risk_profile = _get_settings(context)
    status = await update.message.reply_text(f"🔎 Формирую сводный сигнал по {ticker}...")

    market = _cache_get("market", ticker, MARKET_CACHE_TTL_SEC)
    if not market:
        try:
            market = await _run_with_timeout(get_market_data, ticker)
        except asyncio.TimeoutError:
            await _safe_edit_status(status, "⏱ Данные рынка загружаются слишком долго. Попробуй позже.")
            return MENU
        except Exception:
            await _safe_edit_status(status, "⚠️ Ошибка получения рынка. Попробуй позже.")
            return MENU
        if market:
            _cache_set("market", ticker, market, MARKET_CACHE_TTL_SEC)

    if not market:
        await _safe_edit_status(status, "⚠️ Не удалось получить рыночные данные.")
        return MENU

    news_payload = _cache_get("news", ticker, NEWS_CACHE_TTL_SEC)
    if not news_payload:
        try:
            news_payload = await _run_with_timeout(get_ticker_news_payload, ticker)
            _cache_set("news", ticker, news_payload, NEWS_CACHE_TTL_SEC)
        except asyncio.TimeoutError:
            news_payload = {
                "text": "Новости временно недоступны (timeout).",
                "news_lag_human": "н/д",
                "latest_news_utc": None,
            }

    raw_news = news_payload.get("text", "Новости недоступны")
    news_lag_human = news_payload.get("news_lag_human", "н/д")

    try:
        news_summary = await _run_with_timeout(analyze_news, raw_news, provider, timeout=AI_ANALYSIS_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        news_summary = "⏱ AI-обработка новостей не завершилась в лимит времени."

    compact = _compact_signal_report(market, news_summary, news_lag_human=news_lag_human, risk_profile=risk_profile)  # risk_profile from _get_settings
    _clear_user_flow_state(context)

    await _safe_edit_status(status, "✅ Готово. Отправляю сигнал...")
    await update.message.reply_text(
        f"🎯 ТОРГОВЫЙ СИГНАЛ\n\n{compact}",
        reply_markup=_main_markup(context),
    )
    return MENU


async def run_last_signal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ticker = context.user_data.get("last_analyzed_ticker")
    if not ticker:
        await update.message.reply_text(
            "Сначала запусти AI-Анализ и выбери тикер, затем здесь появится быстрый торговый сигнал.",
            reply_markup=_main_markup(context),
        )
        return MENU
    return await run_signal_for_ticker(update, context, ticker)


async def run_signal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    if _is_back(update.message.text):
        return await cancel(update, context)
    text = _normalize_ticker(update.message.text)
    if not validate_ticker(text):
        await update.message.reply_text("Некорректный тикер. Пример: BTC-USD, AAPL, EURUSD=X")
        return SIGNAL_QUERY
    return await run_signal_for_ticker(update, context, text)


