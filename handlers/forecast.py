"""handlers/forecast.py — Forecast menu FSM handlers."""
from __future__ import annotations
import asyncio
import os
from telegram import Update
from telegram.ext import ContextTypes, ConversationHandler

from forecast_tracker import (
    build_matured_report, build_accuracy_stats, build_per_ticker_accuracy,
    export_matured_report_to_excel, purge_old_snapshots,
)
from bot_globals import (
    FORECAST_MENU,
    forecast_markup, _purge_confirm_markup,
    _deny_if_unauthorized, _is_back,
    _run_with_timeout, _reply_long, _safe_edit_status,
    REQUEST_TIMEOUT_SEC,
    cancel, help_command,
)

async def open_forecast_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    await update.message.reply_text(
        "Раздел Прогноз:\n"
        "• Анализ по тикерам — что получилось по D3 через 3+ дня\n"
        "• Точность прогнозов — агрегированная статистика правильных/неверных\n"
        "• Excel-отчет — выгрузка созревших прогнозов\n"
        "• Очистить историю — удалить старые снапшоты",
        reply_markup=forecast_markup,
    )
    return FORECAST_MENU


async def forecast_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END

    text = (update.message.text or "").strip()
    if _is_back(text):
        return await cancel(update, context)

    if text == "ℹ️ Помощь":
        await help_command(update, context)
        await update.message.reply_text("↩️ Возврат в раздел Прогноз.", reply_markup=forecast_markup)
        return FORECAST_MENU

    if text == "📈 Анализ по тикерам":
        status = await update.message.reply_text("📊 Собираю созревшие прогнозы D3...")
        try:
            report_text, _ = await _run_with_timeout(build_matured_report, 25, timeout=max(REQUEST_TIMEOUT_SEC, 60))
        except Exception:
            await _safe_edit_status(status, "⚠️ Не удалось собрать отчёт. Попробуй позже.")
            return FORECAST_MENU

        await _safe_edit_status(status, "✅ Готово.")
        await _reply_long(update, report_text, reply_markup=forecast_markup)
        return FORECAST_MENU

    if text == "📊 Точность прогнозов":
        status = await update.message.reply_text("📊 Считаю статистику точности...")
        try:
            stats_text = await _run_with_timeout(build_accuracy_stats, timeout=max(REQUEST_TIMEOUT_SEC, 60))
        except Exception:
            await _safe_edit_status(status, "⚠️ Не удалось собрать статистику. Попробуй позже.")
            return FORECAST_MENU
        await _safe_edit_status(status, "✅ Готово.")
        await _reply_long(update, stats_text, reply_markup=forecast_markup)
        return FORECAST_MENU

    if text == "📉 По тикерам":
        status = await update.message.reply_text("📉 Считаю точность по тикерам...")
        try:
            pt_text = await _run_with_timeout(build_per_ticker_accuracy, 1, timeout=max(REQUEST_TIMEOUT_SEC, 60))
        except Exception:
            await _safe_edit_status(status, "⚠️ Не удалось собрать статистику. Попробуй позже.")
            return FORECAST_MENU
        await _safe_edit_status(status, "✅ Готово.")
        await _reply_long(update, pt_text, reply_markup=forecast_markup)
        return FORECAST_MENU

    if text == "📥 Excel-отчет":
        status = await update.message.reply_text("📥 Формирую Excel-отчёт...")
        try:
            file_path = await _run_with_timeout(export_matured_report_to_excel, timeout=max(REQUEST_TIMEOUT_SEC, 90))
        except Exception:
            await _safe_edit_status(status, "⚠️ Ошибка формирования Excel-отчёта.")
            return FORECAST_MENU

        if not file_path or not os.path.exists(file_path):
            await _safe_edit_status(status, "Пока нет данных для Excel (нет созревших D3).")
            return FORECAST_MENU

        await _safe_edit_status(status, "✅ Excel готов. Отправляю файл...")
        with open(file_path, "rb") as fp:
            await update.message.reply_document(document=fp, filename=os.path.basename(file_path), reply_markup=forecast_markup)
        return FORECAST_MENU

    if text == "🗑️ Очистить историю":
        await update.message.reply_text(
            "🗑️ Выбери действие:\n"
            "• <b>Удалить старые (&gt;7 дней)</b> — оставит только свежие снапшоты\n"
            "• <b>Удалить всё</b> — полный сброс истории прогнозов",
            reply_markup=_purge_confirm_markup,
            parse_mode="HTML",
        )
        return FORECAST_MENU

    if text == "✅ Да, удалить старые (>7 дней)":
        removed, kept = purge_old_snapshots(keep_days=7)
        await update.message.reply_text(
            f"✅ Готово. Удалено: {removed} снапшотов. Осталось: {kept}.",
            reply_markup=forecast_markup,
        )
        return FORECAST_MENU

    if text == "🗑️ Удалить всё (сброс)":
        import os
        path = "data/forecast_snapshots.jsonl"
        open(path, "w").close()
        await update.message.reply_text(
            "✅ История прогнозов полностью очищена.",
            reply_markup=forecast_markup,
        )
        return FORECAST_MENU

    if text == "↩️ Отмена":
        await update.message.reply_text("Отменено.", reply_markup=forecast_markup)
        return FORECAST_MENU

    await update.message.reply_text("Используй кнопки раздела Прогноз.", reply_markup=forecast_markup)
    return FORECAST_MENU


