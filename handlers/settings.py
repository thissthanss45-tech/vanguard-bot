"""handlers/settings.py — Settings FSM handlers."""
from __future__ import annotations
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from bot_globals import (
    SETTINGS_STATE,
    settings_markup,
    _deny_if_unauthorized, _is_back, _get_settings,
    cancel,
)

async def open_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    provider, risk_profile = _get_settings(context)
    await update.message.reply_text(
        f"Текущие настройки:\nprovider={provider}\nrisk={risk_profile}\n\nВыбери новый режим:",
        reply_markup=settings_markup,
    )
    return SETTINGS_STATE


async def handle_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if _is_back(text):
        return await cancel(update, context)

    if text == "🧠 Провайдер: DeepSeek":
        context.user_data["provider"] = "deepseek"
    elif text == "🧠 Провайдер: Groq":
        context.user_data["provider"] = "groq"
    elif text == "🎯 Риск: Conservative":
        context.user_data["risk_profile"] = "conservative"
    elif text == "🎯 Риск: Balanced":
        context.user_data["risk_profile"] = "balanced"
    elif text == "🎯 Риск: Aggressive":
        context.user_data["risk_profile"] = "aggressive"
    else:
        await update.message.reply_text("Не понял команду. Используй кнопки.", reply_markup=settings_markup)
        return SETTINGS_STATE

    provider, risk_profile = _get_settings(context)
    await update.message.reply_text(
        f"✅ Сохранено: provider={provider}, risk={risk_profile}",
        reply_markup=settings_markup,
    )
    return SETTINGS_STATE

