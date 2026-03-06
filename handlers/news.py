"""handlers/news.py — News FSM handlers."""
from __future__ import annotations
import asyncio
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from ai_engine import analyze_news
from utils import validate_ticker
from bot_globals import (
    MENU, NEWS_QUERY, NEWS_PICK_TICKER,
    news_markup, popular_categories_markup,
    _deny_if_unauthorized, _is_back, _is_main_menu, _is_categories, _is_prev, _is_next,
    _is_tickers_menu, _clamp_page, _build_tickers_markup,
    _run_with_timeout, _cache_get, _cache_set, _main_markup, _inc_stat, _get_settings,
    _reply_long, _safe_edit_status, _normalize_ticker,
    POPULAR_TICKERS, NEWS_CACHE_TTL_SEC, AI_ANALYSIS_TIMEOUT_SEC,
    get_ticker_news_payload, cancel, start,
)

async def ask_news(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "По какому активу найти новости?\n"
        "Введи тикер вручную или нажми Тикеры.",
        reply_markup=news_markup,
    )
    return NEWS_QUERY


async def open_news_tickers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["news_category"] = None
    context.user_data["news_page"] = 0
    await update.message.reply_text(
        "Выбери категорию тикеров для новостей:",
        reply_markup=popular_categories_markup,
    )
    return NEWS_PICK_TICKER


async def run_news_for_ticker(update: Update, context: ContextTypes.DEFAULT_TYPE, ticker: str):
    _inc_stat(context, "news_runs")
    provider, _ = _get_settings(context)
    status = await update.message.reply_text(f"🗞 Ищу сводки по {ticker}...")
    cached_news = _cache_get("news", ticker, NEWS_CACHE_TTL_SEC)
    if cached_news:
        payload = cached_news
    else:
        try:
            payload = await _run_with_timeout(get_ticker_news_payload, ticker)
        except asyncio.TimeoutError:
            await _safe_edit_status(status, "⏱ Источник новостей отвечает слишком долго. Попробуй позже.")
            return NEWS_QUERY
        except Exception:
            await _safe_edit_status(status, "⚠️ Ошибка при получении новостей. Попробуй позже.")
            return NEWS_QUERY
        _cache_set("news", ticker, payload, NEWS_CACHE_TTL_SEC)

    raw = payload.get("text", "")
    news_lag = payload.get("news_lag_human", "н/д")
    latest_news_utc = payload.get("latest_news_utc", "н/д")

    if raw.startswith("Ошибка"):
        await _safe_edit_status(status, raw)
        return NEWS_QUERY

    await _safe_edit_status(status, "🧠 AI анализирует контекст новостей...")
    try:
        summary = await _run_with_timeout(analyze_news, raw, provider, timeout=AI_ANALYSIS_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        summary = "⏱ AI-анализ новостей не успел завершиться."
    except Exception:
        summary = "⚠️ Ошибка AI-анализа новостей."

    result_text = (
        f"🗞 НОВОСТИ: {ticker}\n"
        f"Лаг новостей: {news_lag} (последняя: {latest_news_utc})\n\n"
        f"Сырые новости:\n{raw}\n\nAI-резюме:\n{summary}"
    )
    await _reply_long(update, result_text, reply_markup=_main_markup(context))
    return MENU

async def run_news(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    raw_text = update.message.text.strip()
    if _is_tickers_menu(raw_text):
        return await open_news_tickers(update, context)
    if _is_back(raw_text):
        return await cancel(update, context)

    text = _normalize_ticker(raw_text)
    if not validate_ticker(text):
        await update.message.reply_text("Некорректный тикер. Пример: BTC-USD, AAPL, EURUSD=X")
        return NEWS_QUERY
    return await run_news_for_ticker(update, context, text)


async def run_news_popular_ticker(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    raw_text = update.message.text.strip()
    if _is_main_menu(raw_text):
        context.user_data.pop("news_category", None)
        context.user_data.pop("news_page", None)
        return await start(update, context)
    if _is_back(raw_text):
        context.user_data.pop("news_category", None)
        context.user_data.pop("news_page", None)
        return await cancel(update, context)

    if _is_categories(raw_text):
        context.user_data["news_category"] = None
        context.user_data["news_page"] = 0
        await update.message.reply_text("Выбери категорию:", reply_markup=popular_categories_markup)
        return NEWS_PICK_TICKER

    if raw_text in POPULAR_TICKERS:
        context.user_data["news_category"] = raw_text
        context.user_data["news_page"] = 0
        await update.message.reply_text(
            f"Категория: {raw_text}. Выбери тикер для новостей (по 10 на странице):",
            reply_markup=_build_tickers_markup(raw_text, page=0),
        )
        return NEWS_PICK_TICKER

    selected_category = context.user_data.get("news_category")
    selected_page = context.user_data.get("news_page", 0)
    if not selected_category:
        await update.message.reply_text("Сначала выбери категорию.", reply_markup=popular_categories_markup)
        return NEWS_PICK_TICKER

    if _is_prev(raw_text):
        selected_page = _clamp_page(selected_category, selected_page - 1)
        context.user_data["news_page"] = selected_page
        await update.message.reply_text(
            f"Категория: {selected_category}. Страница {selected_page + 1}.",
            reply_markup=_build_tickers_markup(selected_category, selected_page),
        )
        return NEWS_PICK_TICKER

    if _is_next(raw_text):
        selected_page = _clamp_page(selected_category, selected_page + 1)
        context.user_data["news_page"] = selected_page
        await update.message.reply_text(
            f"Категория: {selected_category}. Страница {selected_page + 1}.",
            reply_markup=_build_tickers_markup(selected_category, selected_page),
        )
        return NEWS_PICK_TICKER

    if raw_text not in POPULAR_TICKERS.get(selected_category, []):
        await update.message.reply_text(
            "Выбери тикер кнопкой из текущей категории или вернись к категориям.",
            reply_markup=_build_tickers_markup(selected_category, selected_page),
        )
        return NEWS_PICK_TICKER

    ticker = _normalize_ticker(raw_text)
    context.user_data.pop("news_category", None)
    return await run_news_for_ticker(update, context, ticker)


