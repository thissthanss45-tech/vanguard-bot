"""
bot.py — точка входа. Собирает ConversationHandler и запускает приложение.
Вся бизнес-логика разнесена по:
  bot_globals.py          — константы, разметки, shared helpers
  handlers/portfolio.py   — портфель
  handlers/watchlist.py   — вотчлист
  handlers/analysis.py    — AI-анализ
  handlers/news.py        — новости
  handlers/signals.py     — сигналы
  handlers/forecast.py    — прогноз
  handlers/settings.py    — настройки
  handlers/commands.py    — команды + menu_router
"""
import datetime as _dt
from datetime import timezone
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler,
    MessageHandler, filters, ConversationHandler, PicklePersistence,
)
from bot_globals import (
    _acquire_single_instance_lock, APP_SETTINGS,
    MENU, ANALYZING, NEWS_QUERY, SIGNAL_QUERY, SETTINGS_STATE, MODELS,
    PICK_TICKER, NEWS_PICK_TICKER, FORECAST_MENU,
    WATCHLIST_MENU, WATCHLIST_ADD, WATCHLIST_REMOVE, WATCHLIST_PICK_TICKER,
    PORTFOLIO_MENU, PORTFOLIO_ADD, PORTFOLIO_CLOSE,
    start, help_command, cancel, reset_session,
    on_error, _job_check_alerts, _job_watchlist_digest,
)
from handlers.analysis import (
    ask_analysis, open_popular_tickers, open_models, handle_models,
    run_analysis, run_popular_ticker_analysis,
)
from handlers.news import ask_news, run_news, run_news_popular_ticker
from handlers.signals import ask_signal, run_signal, run_last_signal
from handlers.forecast import open_forecast_menu, forecast_router
from handlers.settings import open_settings, handle_settings
from handlers.watchlist import (
    open_watchlist, watchlist_router, watchlist_add_handler,
    watchlist_remove_handler, watchlist_pick_ticker_handler, watchlist_picker_callback,
)
from handlers.portfolio import (
    open_portfolio, portfolio_router, portfolio_add_handler, portfolio_close_handler,
)
from handlers.commands import (
    stats_command, lang_command, alert_command, compare_command,
    trade_command, portfolio_command, backtest_command, menu_router,
)


def main():
    if not _acquire_single_instance_lock():
        print("⚠️ Бот уже запущен в другом процессе. Останови старый процесс и запусти снова.")
        return

    import os
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан в .env")

    _persistence = PicklePersistence(filepath="data/bot_persistence.pkl")
    app = Application.builder().token(token).concurrent_updates(False).persistence(_persistence).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            MENU: [
                MessageHandler(filters.Regex('^📈 AI-Анализ$'), ask_analysis),
                MessageHandler(filters.Regex('^🎯 Сигнал по этому тикеру$'), run_last_signal),
                MessageHandler(filters.Regex('^⭐ В вотчлист$'), menu_router),
                MessageHandler(filters.Regex('^📌 Вотчлист$'), open_watchlist),
                MessageHandler(filters.Regex('^💼 Портфель$'), open_portfolio),
                MessageHandler(filters.Regex('^🧠 Модели$'), open_models),
                MessageHandler(filters.Regex('^🗞 Новости$'), ask_news),
                MessageHandler(filters.Regex('^⚙️ Настройки$'), open_settings),
                MessageHandler(filters.Regex('^📚 Тикеры$'), open_popular_tickers),
                MessageHandler(filters.Regex('^📊 Прогноз$'), open_forecast_menu),
                MessageHandler(filters.TEXT & ~filters.COMMAND, menu_router),
            ],
            ANALYZING: [
                MessageHandler(filters.Regex('^↩️ Назад$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, run_analysis),
            ],
            PICK_TICKER: [
                MessageHandler(filters.Regex('^↩️ Назад$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, run_popular_ticker_analysis),
            ],
            NEWS_QUERY: [
                MessageHandler(filters.Regex('^↩️ Назад$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, run_news),
            ],
            NEWS_PICK_TICKER: [
                MessageHandler(filters.Regex('^↩️ Назад$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, run_news_popular_ticker),
            ],
            SIGNAL_QUERY: [
                MessageHandler(filters.Regex('^↩️ Назад$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, run_signal),
            ],
            MODELS: [
                MessageHandler(filters.Regex('^↩️ Назад$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_models),
            ],
            SETTINGS_STATE: [
                MessageHandler(filters.Regex('^↩️ Назад$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_settings),
            ],
            FORECAST_MENU: [
                MessageHandler(filters.Regex('^↩️ Назад$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, forecast_router),
            ],
            WATCHLIST_MENU: [
                MessageHandler(filters.Regex('^↩️ Назад$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, watchlist_router),
            ],
            WATCHLIST_ADD: [
                MessageHandler(filters.Regex('^↩️ Назад$'), watchlist_add_handler),
                MessageHandler(filters.TEXT & ~filters.COMMAND, watchlist_add_handler),
            ],
            WATCHLIST_REMOVE: [
                MessageHandler(filters.Regex('^↩️ Назад$'), watchlist_remove_handler),
                MessageHandler(filters.TEXT & ~filters.COMMAND, watchlist_remove_handler),
            ],
            WATCHLIST_PICK_TICKER: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, watchlist_pick_ticker_handler),
            ],
            PORTFOLIO_MENU: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, portfolio_router),
            ],
            PORTFOLIO_ADD: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, portfolio_add_handler),
            ],
            PORTFOLIO_CLOSE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, portfolio_close_handler),
            ],
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            CommandHandler('help', help_command),
            CommandHandler('start', start),
            CommandHandler('srart', start),
            CommandHandler('reset', reset_session),
            CommandHandler('compare', compare_command),
            MessageHandler(filters.TEXT & ~filters.COMMAND, menu_router),
        ],
        allow_reentry=True,
    )

    app.add_handler(conv)
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('srart', start))
    app.add_handler(CommandHandler('help', help_command))
    app.add_handler(CommandHandler('reset', reset_session))
    app.add_handler(CommandHandler('stats', stats_command))
    app.add_handler(CommandHandler('lang', lang_command))
    app.add_handler(CommandHandler('alert', alert_command))
    app.add_handler(CommandHandler('compare', compare_command))
    app.add_handler(CommandHandler('trade', trade_command))
    app.add_handler(CommandHandler('portfolio', portfolio_command))
    app.add_handler(CommandHandler('backtest', backtest_command))
    app.add_handler(CallbackQueryHandler(watchlist_picker_callback, pattern="^wl_"), group=1)
    app.add_error_handler(on_error)

    jq = app.job_queue
    if jq:
        jq.run_repeating(_job_check_alerts, interval=900, first=60, name="check_alerts")
        jq.run_daily(
            _job_watchlist_digest,
            time=_dt.time(hour=8, minute=0, tzinfo=timezone.utc),
            name="watchlist_digest",
        )

    print("🚀 Бот запущен!")
    if APP_SETTINGS.use_webhook and APP_SETTINGS.webhook_url:
        app.run_webhook(
            listen="0.0.0.0",
            port=APP_SETTINGS.webhook_port,
            webhook_url=f"{APP_SETTINGS.webhook_url.rstrip('/')}{APP_SETTINGS.webhook_path}",
            url_path=APP_SETTINGS.webhook_path.lstrip("/"),
            drop_pending_updates=True,
        )
    else:
        app.run_polling(drop_pending_updates=True)


if __name__ == '__main__':
    main()
