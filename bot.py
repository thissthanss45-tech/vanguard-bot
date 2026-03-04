import os
import asyncio
import logging
import fcntl
from datetime import datetime, timezone
from dotenv import load_dotenv
from telegram import ReplyKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler

# Импортируем наши модули
from data_provider import get_market_data
from ai_engine import get_ai_prediction, analyze_news
from charts import build_price_chart
from cache_backend import build_cache
from config import SETTINGS as APP_SETTINGS
from forecast_tracker import append_snapshot_from_market_data, build_matured_report, export_matured_report_to_excel
from i18n import t
from logging_setup import setup_logging
from utils import normalize_nav_text, normalize_ticker, split_text, validate_ticker
try:
    from news_provider import get_ticker_news_payload
except ImportError:
    def get_ticker_news_payload(t):
        return {
            "text": "Модуль новостей не найден.",
            "news_lag_seconds": None,
            "news_lag_human": "н/д",
            "latest_news_utc": None,
            "news_count": 0,
        }

load_dotenv()
setup_logging(APP_SETTINGS.log_file, APP_SETTINGS.sentry_dsn)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.INFO)
REQUEST_TIMEOUT_SEC = APP_SETTINGS.request_timeout_sec
TICKERS_PER_PAGE = APP_SETTINGS.tickers_per_page
MAIN_MENU_BTN = "🏠 В главное меню"
MARKET_CACHE_TTL_SEC = APP_SETTINGS.market_cache_ttl_sec
NEWS_CACHE_TTL_SEC = APP_SETTINGS.news_cache_ttl_sec
AI_ANALYSIS_TIMEOUT_SEC = int(os.getenv("AI_ANALYSIS_TIMEOUT_SEC", str(max(REQUEST_TIMEOUT_SEC, APP_SETTINGS.ai_timeout_sec + 25))))

_cache = build_cache(APP_SETTINGS.cache_backend, APP_SETTINGS.cache_dir, APP_SETTINGS.redis_url)
_instance_lock = None


def _acquire_single_instance_lock(lock_path: str = "/tmp/vanguard_bot.lock") -> bool:
    global _instance_lock
    lock_file = open(lock_path, "w")
    try:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        lock_file.write(str(os.getpid()))
        lock_file.flush()
        _instance_lock = lock_file
        return True
    except BlockingIOError:
        lock_file.close()
        return False

# Состояния
MENU, ANALYZING, NEWS_QUERY, SIGNAL_QUERY, SETTINGS_STATE, MODELS, PICK_TICKER, NEWS_PICK_TICKER, FORECAST_MENU = range(9)

# Кнопки меню
main_keyboard = [
    ['📈 AI-Анализ', '🧠 Модели'],
    ['🗞 Новости', '⚙️ Настройки'],
    ['📚 Тикеры', '📊 Прогноз']
]
main_markup = ReplyKeyboardMarkup(main_keyboard, resize_keyboard=True)
back_markup = ReplyKeyboardMarkup([['↩️ Назад']], resize_keyboard=True)
analysis_markup = ReplyKeyboardMarkup(
    [
        ['📚 Тикеры'],
        ['↩️ Назад'],
    ],
    resize_keyboard=True,
)
analysis_result_markup = ReplyKeyboardMarkup(
    [
        ['🎯 Сигнал по этому тикеру'],
        ['↩️ Назад'],
    ],
    resize_keyboard=True,
)
news_markup = ReplyKeyboardMarkup(
    [
        ['📚 Тикеры'],
        ['↩️ Назад'],
    ],
    resize_keyboard=True,
)
POPULAR_TICKERS = {
    "🟣 Крипта": [
        "BTC-USD", "ETH-USD", "SOL-USD", "BNB-USD", "XRP-USD", "ADA-USD", "DOGE-USD", "AVAX-USD",
        "DOT-USD", "LINK-USD", "POL-USD", "LTC-USD", "BCH-USD", "TRX-USD", "TON-USD", "ATOM-USD",
        "NEAR-USD", "ETC-USD", "XLM-USD", "UNI-USD",
    ],
    "🔵 Акции": [
        "AAPL", "NVDA", "TSLA", "MSFT", "AMZN", "GOOGL", "META", "NFLX", "AMD", "INTC",
        "PLTR", "COIN", "UBER", "PYPL", "JPM", "BAC", "V", "MA", "DIS", "NKE",
        "XOM", "CVX", "JNJ", "PFE", "WMT", "COST", "KO", "PEP", "MCD", "BABA",
    ],
    "🟡 Сырьё": [
        "GC=F", "SI=F", "PL=F", "PA=F", "HG=F", "BZ=F", "CL=F", "NG=F", "RB=F", "HO=F",
        "ZC=F", "ZW=F", "ZS=F", "KC=F", "SB=F", "CC=F", "CT=F", "OJ=F",
    ],
    "🟢 Форекс": [
        "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "NZDUSD=X", "USDCAD=X", "USDCHF=X", "EURJPY=X",
        "EURGBP=X", "GBPJPY=X", "XAUUSD=X", "XAGUSD=X",
    ],
}
popular_categories_markup = ReplyKeyboardMarkup(
    [
        ['🟣 Крипта', '🔵 Акции'],
        ['🟡 Сырьё', '🟢 Форекс'],
        ['↩️ Назад'],
    ],
    resize_keyboard=True,
)
models_markup = ReplyKeyboardMarkup(
    [
        ['🦙 Llama', '🐋 DeepSeek'],
        ['↩️ Назад'],
    ],
    resize_keyboard=True,
)
settings_markup = ReplyKeyboardMarkup(
    [
        ['🧠 Провайдер: DeepSeek', '🧠 Провайдер: Groq'],
        ['🎯 Риск: Conservative', '🎯 Риск: Balanced', '🎯 Риск: Aggressive'],
        ['↩️ Назад'],
    ],
    resize_keyboard=True,
)

forecast_markup = ReplyKeyboardMarkup(
    [
        ['📈 Анализ по тикерам', '📥 Excel-отчет'],
        ['ℹ️ Помощь', '↩️ Назад'],
    ],
    resize_keyboard=True,
)


def _get_settings(context: ContextTypes.DEFAULT_TYPE):
    provider = context.user_data.get("provider", "deepseek")
    risk_profile = context.user_data.get("risk_profile", "balanced")
    return provider, risk_profile


def _normalize_ticker(text: str) -> str:
    return normalize_ticker(text)


def _normalize_nav_text(text: str) -> str:
    return normalize_nav_text(text)


def _is_back(text: str) -> bool:
    token = _normalize_nav_text(text)
    return token in {"назад", "↩ назад", "↩️ назад"} or token.endswith("назад")



def _is_categories(text: str) -> bool:
    token = _normalize_nav_text(text)
    return "категории" in token


def _is_prev(text: str) -> bool:
    token = _normalize_nav_text(text)
    return "предыдущ" in token


def _is_next(text: str) -> bool:
    token = _normalize_nav_text(text)
    return "следующ" in token


def _is_main_menu(text: str) -> bool:
    token = _normalize_nav_text(text)
    return "главное меню" in token


def _is_tickers_menu(text: str) -> bool:
    token = _normalize_nav_text(text)
    return "тикеры" in token


def _split_text(text: str, limit: int = 3900):
    return split_text(text, limit)


def _cache_get(namespace: str, key: str, ttl_sec: int):
    _ = ttl_sec
    return _cache.get(namespace, key)


def _cache_set(namespace: str, key: str, value, ttl_sec: int):
    _cache.set(namespace, key, value, ttl_sec)


def _is_authorized(update: Update) -> bool:
    if not APP_SETTINGS.auth_whitelist:
        return True
    user = update.effective_user
    if not user:
        return False
    return int(user.id) in APP_SETTINGS.auth_whitelist


async def _deny_if_unauthorized(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if _is_authorized(update):
        return False
    lang = context.user_data.get("lang", APP_SETTINGS.default_lang)
    await update.message.reply_text(t(lang, "unauthorized", "⛔ Доступ ограничен."))
    return True


def _main_markup(context: ContextTypes.DEFAULT_TYPE) -> ReplyKeyboardMarkup:
    _ = context
    keyboard = [
        ['📈 AI-Анализ', '🧠 Модели'],
        ['🗞 Новости', '⚙️ Настройки'],
        ['📚 Тикеры', '📊 Прогноз'],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def _inc_stat(context: ContextTypes.DEFAULT_TYPE, key: str):
    stats = context.bot_data.setdefault("usage_stats", {})
    stats[key] = int(stats.get(key, 0)) + 1


def _get_stats_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    stats = context.bot_data.get("usage_stats", {})
    if not stats:
        return "📊 Статистика пока пуста."
    lines = ["📊 Статистика использования:"]
    for key, value in sorted(stats.items(), key=lambda item: item[0]):
        lines.append(f"- {key}: {value}")
    return "\n".join(lines)


def _get_alerts(context: ContextTypes.DEFAULT_TYPE) -> dict:
    return context.user_data.setdefault("alerts", {})


def _check_alert_trigger(alert_cfg: dict, market_data: dict) -> str | None:
    threshold = alert_cfg.get("rsi_below")
    if threshold is None:
        return None
    rsi = market_data.get("rsi_14")
    if rsi is None:
        return None
    if float(rsi) < float(threshold):
        return f"🔔 Алерт: RSI14={rsi} ниже порога {threshold}"
    return None


def _clear_user_flow_state(context: ContextTypes.DEFAULT_TYPE):
    for key in [
        "popular_category",
        "popular_page",
        "news_category",
        "news_page",
    ]:
        context.user_data.pop(key, None)


async def _reply_long(update: Update, text: str, reply_markup=None):
    parts = _split_text(text)
    for i, part in enumerate(parts):
        await update.message.reply_text(part, reply_markup=reply_markup if i == len(parts) - 1 else None)


async def _safe_edit_status(status_message, text: str):
    try:
        await status_message.edit_text(text)
    except Exception:
        pass


def _build_tickers_markup(category: str, page: int = 0) -> ReplyKeyboardMarkup:
    tickers = POPULAR_TICKERS.get(category, [])
    total_pages = max(1, (len(tickers) + TICKERS_PER_PAGE - 1) // TICKERS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    start = page * TICKERS_PER_PAGE
    end = start + TICKERS_PER_PAGE
    page_items = tickers[start:end]

    rows = []
    for i in range(0, len(page_items), 2):
        rows.append(page_items[i:i + 2])

    nav_row = []
    if page > 0:
        nav_row.append('⬅️ Предыдущие')
    if page < total_pages - 1:
        nav_row.append('➡️ Следующие')
    nav_row.append(MAIN_MENU_BTN)
    if nav_row:
        rows.append(nav_row)

    rows.append(['↩️ Назад'])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _clamp_page(category: str, page: int) -> int:
    total = max(1, (len(POPULAR_TICKERS.get(category, [])) + TICKERS_PER_PAGE - 1) // TICKERS_PER_PAGE)
    return max(0, min(page, total - 1))


def _format_market_block(data: dict) -> str:
    rule = data["rule_forecast"]
    gate_line = f"\nФильтр: {rule['gate_reason']}" if rule.get("gate_reason") else ""
    factors = rule.get("score_breakdown", {})
    factors_line = ""
    if factors:
        sorted_items = sorted(factors.items(), key=lambda item: abs(float(item[1])), reverse=True)[:4]
        parts = [f"{name}: {value:+}" for name, value in sorted_items]
        factors_line = f"\nФакторы: {', '.join(parts)}"

    forecast_3d = data.get("forecast_3d", [])
    forecast_lines = ""
    if forecast_3d:
        rows = [
            f"Д{item['day']}: {item['bullish_probability']}%/{item['bearish_probability']}% ({item['bias']})"
            for item in forecast_3d
        ]
        forecast_lines = f"\nПрогноз 3 дня: {' | '.join(rows)}"

    analysis_dt = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    action_block = _build_action_block(rule)

    return (
        f"Дата анализа: {analysis_dt}\n"
        f"Тикер: {data['symbol']}\n"
        f"Что это: {data.get('instrument_description', data['symbol'])}\n"
        f"Лаг данных: {data.get('data_lag_human', 'н/д')} (срез: {data.get('last_candle_utc', 'н/д')})\n"
        f"Цена: {data['current_price']}\n"
        f"Изм 1д: {data['change_pct_1d']}%\n"
        f"SMA20/SMA50: {data['sma_20']} / {data['sma_50']}\n"
        f"EMA200: {data['ema_200']}\n"
        f"RSI14: {data['rsi_14']} | ATR14: {data['atr_14']} | ADX14: {data['adx_14']}\n"
        f"Волатильность(год): {data['annualized_volatility_pct']}%\n"
        f"Диапазон 20д: {data['low_20d']} - {data['high_20d']}\n"
        f"Корр. SPY/BTC (60д): {data.get('corr_with_spy_60d', 'н/д')} / {data.get('corr_with_btc_60d', 'н/д')}\n"
        f"Объем (ratio): {data['volume_ratio']}\n\n"
        f"Rule-based: {rule['bias']} | Режим: {rule.get('regime', 'н/д')}\n"
        f"Рост/Падение: {rule['bullish_probability']}% / {rule['bearish_probability']}%\n"
        f"Сигнал: {rule['action']}\n"
        f"Уверенность: {rule['confidence']}{gate_line}{factors_line}{forecast_lines}\n\n"
        f"{action_block}"
    )


def _derive_news_impact(news_summary: str) -> int:
    text = news_summary.lower()
    positive_words = ["позитив", "рост", "сильн", "оптим", "bull", "uptrend"]
    negative_words = ["негатив", "паден", "риск", "слаб", "bear", "downtrend"]

    score = 0
    for word in positive_words:
        if word in text:
            score += 1
    for word in negative_words:
        if word in text:
            score -= 1
    return score


def _compose_trade_signal(data: dict, news_summary: str) -> str:
    rule = data["rule_forecast"]
    if not rule.get("trade_allowed", True):
        gate_reason = rule.get("gate_reason") or "Фильтр качества не пропустил сделку"
        return (
            "Сводный сигнал: Ждать\n"
            f"Причина: {gate_reason}\n"
            f"Базовые вероятности: {rule['bullish_probability']}% / {rule['bearish_probability']}%"
        )

    base_prob = rule["bullish_probability"]
    impact = _derive_news_impact(news_summary)
    adjusted_prob = max(5, min(95, base_prob + impact * 3))

    if adjusted_prob >= 67:
        verdict = "Лонг-сценарий приоритетный"
        plan = "Вход частями, стоп ниже локальной поддержки"
    elif adjusted_prob <= 38:
        verdict = "Риск снижения повышен"
        plan = "Снижать размер позиции или ждать разворота"
    else:
        verdict = "Нейтральная зона"
        plan = "Работать от подтверждения, без агрессии"

    return (
        f"Сводный сигнал: {verdict}\n"
        f"Итоговая вероятность роста: {adjusted_prob}%\n"
        f"Вероятность снижения: {100 - adjusted_prob}%\n"
        f"План: {plan}"
    )


def _compact_signal_report(data: dict, news_summary: str, news_lag_human: str = "н/д") -> str:
    rule = data["rule_forecast"]
    impact = _derive_news_impact(news_summary)
    signal = _compose_trade_signal(data, news_summary)
    analysis_dt = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    action_block = _build_action_block(rule)

    if impact >= 2:
        news_tone = "Позитивный"
    elif impact <= -2:
        news_tone = "Негативный"
    else:
        news_tone = "Нейтральный"

    return (
        f"Дата анализа: {analysis_dt}\n"
        f"Актив: {data['symbol']}\n"
        f"Что это: {data.get('instrument_description', data['symbol'])}\n"
        f"Лаг данных: {data.get('data_lag_human', 'н/д')}\n"
        f"Лаг новостей: {news_lag_human}\n"
        f"Базовый bias: {rule['bias']} ({rule['bullish_probability']}%/{rule['bearish_probability']}%)\n"
        f"Новостной фон: {news_tone}\n"
        f"Уверенность: {rule['confidence']}\n\n"
        f"{signal}\n\n"
        f"{action_block}"
    )


def _build_action_block(rule: dict) -> str:
    bull = int(rule.get("bullish_probability", 50))
    bear = int(rule.get("bearish_probability", 50))
    trade_allowed = bool(rule.get("trade_allowed", True))

    if not trade_allowed:
        out_of_pos = "Вне позиции: ждать, новый вход не открывать до снятия фильтра качества."
        in_pos = "В позиции: сократить риск и подтянуть стоп, не усреднять."
    elif bull >= 62:
        out_of_pos = "Вне позиции: возможен вход частями после подтверждения импульса."
        in_pos = "В позиции: держать, частично фиксировать на ближайших целях."
    elif bear >= 62:
        out_of_pos = "Вне позиции: воздержаться от лонга, ждать разворотного подтверждения."
        in_pos = "В позиции: снизить объём и контролировать риск по стоп-уровню."
    else:
        out_of_pos = "Вне позиции: ждать более сильного преимущества по вероятностям."
        in_pos = "В позиции: удерживать только с консервативным риском, без наращивания."

    return (
        "Действия трейдера:\n"
        f"• {out_of_pos}\n"
        f"• {in_pos}"
    )


async def _run_with_timeout(func, *args, timeout: int = REQUEST_TIMEOUT_SEC):
    return await asyncio.wait_for(asyncio.to_thread(func, *args), timeout=timeout)


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logging.exception("Unhandled exception: %s", context.error)
    try:
        _clear_user_flow_state(context)
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text(
                "⚠️ Произошла временная ошибка. Попробуй ещё раз через несколько секунд.",
                reply_markup=_main_markup(context),
            )
    except Exception:
        pass

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    _inc_stat(context, "cmd_start")
    provider, risk_profile = _get_settings(context)
    lang = context.user_data.get("lang", APP_SETTINGS.default_lang)
    await update.message.reply_text(
        (
            f"{t(lang, 'menu_intro', 'Vanguard Bot запущен. Выберите модуль:')}\n"
            "Я даю тех.прогноз, AI-разбор и торговый сигнал по тикеру.\n"
            f"Текущие настройки: provider={provider}, risk={risk_profile}.\n\n"
            "Выберите модуль:"
        ),
        reply_markup=_main_markup(context)
    )
    return MENU


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    _inc_stat(context, "cmd_help")
    await update.message.reply_text(
        "Команды:\n"
        "/start — главное меню\n"
        "/help — помощь\n"
        "/reset — сбросить текущий режим\n"
        "/stats — статистика использования\n"
        "/lang ru|en — язык интерфейса\n"
        "/alert add BTC-USD 30 — алерт RSI<30\n"
        "/alert del BTC-USD — удалить алерт\n"
        "/alert list — список алертов\n"
        "/cancel — назад в меню\n\n"
        "Шпаргалка (быстро):\n"
        "1) 📈 AI-Анализ → выбери тикер\n"
        "2) Прочитай: лаг, вероятности, прогноз Д1/Д2/Д3\n"
        "3) 🎯 Сигнал по этому тикеру → краткий итог\n"
        "4) 🗞 Новости → проверка свежести фона\n"
        "5) /reset если застрял в режиме\n\n"
        "Логика:\n"
        "1) AI-Анализ — тех.снимок + AI-объяснение\n"
        "2) Торговый сигнал — объединяет технику и новости\n"
        "3) Тикеры — выбор по категориям: Крипта/Акции/Сырьё/Форекс\n"
        "4) Модели — выбор AI: Llama или DeepSeek\n"
        "5) Новости — сводка и оценка фона"
    )
    return MENU


async def open_forecast_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    await update.message.reply_text(
        "Раздел Прогноз:\n"
        "• Анализ по тикерам — что получилось по D3 через 3+ дня\n"
        "• Excel-отчет — выгрузка созревших прогнозов",
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

    await update.message.reply_text("Используй кнопки раздела Прогноз.", reply_markup=forecast_markup)
    return FORECAST_MENU


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
    if len(parts) < 2 or parts[1].lower() == "list":
        if not alerts:
            await update.message.reply_text("Алертов пока нет.")
            return
        lines = ["🔔 Текущие алерты:"]
        for ticker, cfg in alerts.items():
            lines.append(f"- {ticker}: RSI<{cfg.get('rsi_below', 'н/д')}")
        await update.message.reply_text("\n".join(lines))
        return

    action = parts[1].lower()
    if action == "add" and len(parts) >= 4:
        ticker = normalize_ticker(parts[2])
        if not validate_ticker(ticker):
            await update.message.reply_text("Некорректный тикер для алерта.")
            return
        try:
            threshold = float(parts[3])
        except ValueError:
            await update.message.reply_text("Порог RSI должен быть числом. Пример: /alert add BTC-USD 30")
            return
        alerts[ticker] = {"rsi_below": threshold}
        await update.message.reply_text(f"✅ Алерт добавлен: {ticker}, RSI<{threshold}")
        return

    if action == "del" and len(parts) >= 3:
        ticker = normalize_ticker(parts[2])
        removed = alerts.pop(ticker, None)
        if removed:
            await update.message.reply_text(f"🗑 Удалён алерт: {ticker}")
        else:
            await update.message.reply_text("Алерт не найден.")
        return

    await update.message.reply_text("Использование: /alert list | /alert add <TICKER> <RSI> | /alert del <TICKER>")


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
    if "помощ" in normalized:
        return await help_command(update, context)
    if "сигнал" in normalized:
        return await run_last_signal(update, context)
    if _is_back(text):
        return await cancel(update, context)

    await update.message.reply_text("Не понял команду. Выбери действие кнопкой ниже.", reply_markup=_main_markup(context))
    return MENU


async def reset_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    _inc_stat(context, "cmd_reset")
    _clear_user_flow_state(context)
    await update.message.reply_text("🔄 Режим сброшен.", reply_markup=_main_markup(context))
    return MENU

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


async def ask_signal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Введите тикер для итогового торгового сигнала:", reply_markup=back_markup)
    return SIGNAL_QUERY


async def run_signal_for_ticker(update: Update, context: ContextTypes.DEFAULT_TYPE, ticker: str):
    _inc_stat(context, "signal_runs")
    provider, _ = _get_settings(context)
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

    compact = _compact_signal_report(market, news_summary, news_lag_human=news_lag_human)
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

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    _clear_user_flow_state(context)
    await update.message.reply_text("↩️ Возврат в меню.", reply_markup=_main_markup(context))
    return MENU

def main():
    if not _acquire_single_instance_lock():
        print("⚠️ Бот уже запущен в другом процессе. Останови старый процесс и запусти снова.")
        return

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN не задан в .env")

    app = Application.builder().token(token).concurrent_updates(False).build()

    conv = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            MENU: [
                MessageHandler(filters.Regex('^📈 AI-Анализ$'), ask_analysis),
                MessageHandler(filters.Regex('^🎯 Сигнал по этому тикеру$'), run_last_signal),
                MessageHandler(filters.Regex('^🧠 Модели$'), open_models),
                MessageHandler(filters.Regex('^🗞 Новости$'), ask_news),
                MessageHandler(filters.Regex('^⚙️ Настройки$'), open_settings),
                MessageHandler(filters.Regex('^📚 Тикеры$'), open_popular_tickers),
                MessageHandler(filters.Regex('^📊 Прогноз$'), open_forecast_menu),
                MessageHandler(filters.TEXT & ~filters.COMMAND, menu_router),
            ],
            ANALYZING: [
                MessageHandler(filters.Regex('^↩️ Назад$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, run_analysis)
            ],
            PICK_TICKER: [
                MessageHandler(filters.Regex('^↩️ Назад$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, run_popular_ticker_analysis),
            ],
            NEWS_QUERY: [
                MessageHandler(filters.Regex('^↩️ Назад$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, run_news)
            ],
            NEWS_PICK_TICKER: [
                MessageHandler(filters.Regex('^↩️ Назад$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, run_news_popular_ticker),
            ],
            SIGNAL_QUERY: [
                MessageHandler(filters.Regex('^↩️ Назад$'), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, run_signal)
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
        },
        fallbacks=[
            CommandHandler('cancel', cancel),
            CommandHandler('help', help_command),
            CommandHandler('start', start),
            CommandHandler('srart', start),
            CommandHandler('reset', reset_session),
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
    app.add_error_handler(on_error)
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