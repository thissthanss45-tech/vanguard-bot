"""handlers/analysis.py — AI analysis FSM handlers."""
from __future__ import annotations
import asyncio
import logging
import os
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from data_provider import get_market_data
from ai_engine import get_ai_prediction
from charts import build_price_chart
from forecast_tracker import append_snapshot_from_market_data
from utils import validate_ticker
from bot_globals import (
    MENU, ANALYZING, PICK_TICKER, MODELS,
    analysis_markup, analysis_result_markup, popular_categories_markup, models_markup,
    _deny_if_unauthorized, _is_back, _is_main_menu, _is_categories, _is_prev, _is_next,
    _is_tickers_menu, _clamp_page, _build_tickers_markup, _format_market_block,
    _run_with_timeout, _cache_get, _cache_set, _main_markup, _inc_stat, _get_settings,
    _get_alerts, _check_alert_trigger, _reply_long, _safe_edit_status, _normalize_ticker,
    POPULAR_TICKERS, MARKET_CACHE_TTL_SEC, AI_ANALYSIS_TIMEOUT_SEC, REQUEST_TIMEOUT_SEC,
    cancel, start,
)

async def ask_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Введите тикер для анализа (напр. BTC-USD или BZ=F)\nили нажмите кнопку Тикеры.",
        reply_markup=analysis_markup,
    )
    return ANALYZING


async def open_popular_tickers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["popular_category"] = None
    context.user_data["popular_page"] = 0
    await update.message.reply_text(
        "Выбери категорию популярных тикеров:",
        reply_markup=popular_categories_markup,
    )
    return PICK_TICKER


async def open_models(update: Update, context: ContextTypes.DEFAULT_TYPE):
    provider, _ = _get_settings(context)
    current_name = "🐋 DeepSeek" if provider == "deepseek" else "🦙 Llama"
    await update.message.reply_text(
        f"Текущая модель: {current_name}\nВыбери модель анализа:",
        reply_markup=models_markup,
    )
    return MODELS


async def handle_models(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if _is_back(text):
        return await cancel(update, context)

    if text == "🦙 Llama":
        context.user_data["provider"] = "groq"
        await update.message.reply_text("✅ Модель переключена: 🦙 Llama", reply_markup=models_markup)
        return MODELS
    if text == "🐋 DeepSeek":
        context.user_data["provider"] = "deepseek"
        await update.message.reply_text("✅ Модель переключена: 🐋 DeepSeek", reply_markup=models_markup)
        return MODELS

    await update.message.reply_text("Используй кнопки выбора модели.", reply_markup=models_markup)
    return MODELS


async def run_analysis_for_ticker(update: Update, context: ContextTypes.DEFAULT_TYPE, ticker: str):
    _inc_stat(context, "analysis_runs")
    provider, risk_profile = _get_settings(context)
    status = await update.message.reply_text(f"📡 Собираю данные по {ticker}...")
    cached_data = _cache_get("market", ticker, MARKET_CACHE_TTL_SEC)
    if cached_data:
        data = cached_data
    else:
        try:
            data = await _run_with_timeout(get_market_data, ticker)
        except asyncio.TimeoutError:
            await _safe_edit_status(status, "⏱ Источник рынка отвечает слишком долго. Попробуй еще раз через минуту.")
            return ANALYZING
        except Exception:
            await _safe_edit_status(status, "⚠️ Ошибка при получении рыночных данных. Попробуй позже.")
            return ANALYZING
        if data:
            _cache_set("market", ticker, data, MARKET_CACHE_TTL_SEC)

    if not data:
        await _safe_edit_status(status, "⚠️ Тикер не найден.")
        return ANALYZING

    try:
        append_snapshot_from_market_data(data)
    except Exception as exc:
        logging.warning("forecast snapshot append failed for %s: %s", ticker, exc)

    await _safe_edit_status(status, f"🧠 AI-провайдер {provider} строит прогноз...")
    try:
        ai_text = await _run_with_timeout(
            get_ai_prediction,
            data,
            provider,
            risk_profile,
            timeout=AI_ANALYSIS_TIMEOUT_SEC,
        )
    except asyncio.TimeoutError:
        ai_text = "⏱ AI-провайдер не ответил вовремя. Показываю только rule-based прогноз."
    except Exception:
        ai_text = "⚠️ Ошибка AI-анализа. Показываю локальный прогноз."

    final_text = (
        f"📋 ПРОГНОЗ: {ticker}\n\n"
        f"{_format_market_block(data)}\n\n"
        f"AI-разбор:\n{ai_text}"
    )
    context.user_data["last_analyzed_ticker"] = ticker
    alerts = _get_alerts(context)
    alert_cfg = alerts.get(ticker)
    if alert_cfg:
        triggered = _check_alert_trigger(alert_cfg, data)
        if triggered:
            await update.message.reply_text(triggered)

    await _reply_long(update, final_text, reply_markup=analysis_result_markup)

    chart_path = None
    try:
        chart_path = build_price_chart(data.get("chart_history"), ticker)
        if chart_path and os.path.exists(chart_path):
            with open(chart_path, "rb") as image_file:
                await update.message.reply_photo(photo=image_file, caption=f"📉 График {ticker}")
    except Exception as exc:
        logging.warning("chart send failed for %s: %s", ticker, exc)

    return MENU

async def run_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    raw_text = update.message.text.strip()
    if _is_tickers_menu(raw_text):
        return await open_popular_tickers(update, context)
    if _is_back(raw_text):
        return await cancel(update, context)

    text = _normalize_ticker(raw_text)
    if not validate_ticker(text):
        await update.message.reply_text("Некорректный тикер. Пример: BTC-USD, AAPL, EURUSD=X")
        return ANALYZING
    return await run_analysis_for_ticker(update, context, text)

async def run_popular_ticker_analysis(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    raw_text = update.message.text.strip()
    if _is_main_menu(raw_text):
        context.user_data.pop("popular_category", None)
        context.user_data.pop("popular_page", None)
        return await start(update, context)
    if _is_back(raw_text):
        context.user_data.pop("popular_category", None)
        context.user_data.pop("popular_page", None)
        return await cancel(update, context)

    if _is_categories(raw_text):
        context.user_data["popular_category"] = None
        context.user_data["popular_page"] = 0
        await update.message.reply_text("Выбери категорию:", reply_markup=popular_categories_markup)
        return PICK_TICKER

    if raw_text in POPULAR_TICKERS:
        context.user_data["popular_category"] = raw_text
        context.user_data["popular_page"] = 0
        await update.message.reply_text(
            f"Категория: {raw_text}. Выбери тикер (по 10 на странице):",
            reply_markup=_build_tickers_markup(raw_text, page=0),
        )
        return PICK_TICKER

    selected_category = context.user_data.get("popular_category")
    selected_page = context.user_data.get("popular_page", 0)
    if not selected_category:
        await update.message.reply_text("Сначала выбери категорию.", reply_markup=popular_categories_markup)
        return PICK_TICKER

    if _is_prev(raw_text):
        selected_page = _clamp_page(selected_category, selected_page - 1)
        context.user_data["popular_page"] = selected_page
        await update.message.reply_text(
            f"Категория: {selected_category}. Страница {selected_page + 1}.",
            reply_markup=_build_tickers_markup(selected_category, selected_page),
        )
        return PICK_TICKER

    if _is_next(raw_text):
        selected_page = _clamp_page(selected_category, selected_page + 1)
        context.user_data["popular_page"] = selected_page
        await update.message.reply_text(
            f"Категория: {selected_category}. Страница {selected_page + 1}.",
            reply_markup=_build_tickers_markup(selected_category, selected_page),
        )
        return PICK_TICKER

    if raw_text not in POPULAR_TICKERS.get(selected_category, []):
        await update.message.reply_text(
            "Выбери тикер кнопкой из текущей категории или вернись к категориям.",
            reply_markup=_build_tickers_markup(selected_category, selected_page),
        )
        return PICK_TICKER

    ticker = _normalize_ticker(raw_text)
    context.user_data.pop("popular_category", None)
    return await run_analysis_for_ticker(update, context, ticker)

