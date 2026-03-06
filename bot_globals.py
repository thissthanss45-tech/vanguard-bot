import os
import asyncio
import logging
import fcntl
from datetime import datetime, timezone
from dotenv import load_dotenv
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler, PicklePersistence

# Импортируем наши модули
from data_provider import get_market_data
from ai_engine import get_ai_prediction, analyze_news
from charts import build_price_chart
from cache_backend import build_cache
from config import SETTINGS as APP_SETTINGS
from forecast_tracker import append_snapshot_from_market_data, build_accuracy_stats, build_matured_report, build_per_ticker_accuracy, build_ticker_backtest, export_matured_report_to_excel, purge_old_snapshots
from portfolio_tracker import trade_add, trade_close, trade_close_with_pnl, trade_list, portfolio_summary
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
MENU, ANALYZING, NEWS_QUERY, SIGNAL_QUERY, SETTINGS_STATE, MODELS, PICK_TICKER, NEWS_PICK_TICKER, FORECAST_MENU, WATCHLIST_MENU, WATCHLIST_ADD, WATCHLIST_REMOVE, WATCHLIST_PICK_TICKER, PORTFOLIO_MENU, PORTFOLIO_ADD, PORTFOLIO_CLOSE = range(16)

# Кнопки меню
main_keyboard = [
    ['📈 AI-Анализ', '🧠 Модели'],
    ['🗞 Новости', '⚙️ Настройки'],
    ['📌 Вотчлист', '📊 Прогноз'],
    ['💼 Портфель'],
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
        ['🎯 Сигнал по этому тикеру', '⭐ В вотчлист'],
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
        ['📊 Точность прогнозов', '📉 По тикерам'],
        ['ℹ️ Помощь', '🗑️ Очистить историю'],
        ['↩️ Назад'],
    ],
    resize_keyboard=True,
)

# Markup для подтверждения очистки
_purge_confirm_markup = ReplyKeyboardMarkup(
    [
        ['✅ Да, удалить старые (>7 дней)', '↩️ Отмена'],
        ['🗑️ Удалить всё (сброс)'],
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
        ['📌 Вотчлист', '📊 Прогноз'],
        ['💼 Портфель'],
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


def _sync_alerts_to_bot_data(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Copies current user's alerts into bot_data map (used by background job)."""
    chat_id = str(context._user_id if hasattr(context, "_user_id") else "")
    if not chat_id:
        try:
            # Works during message handlers
            chat_id = str(context.user_data.get("_chat_id", ""))
        except Exception:
            return
    if not chat_id:
        return
    alerts = context.user_data.get("alerts", {})
    context.bot_data.setdefault("user_alerts_map", {})[chat_id] = dict(alerts)


def _sync_wl_to_bot_data(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> None:
    """Copies current user's watchlist into bot_data map (used by background job)."""
    tickers = _wl_load(user_id)
    context.bot_data.setdefault("user_wl_map", {})[str(user_id)] = tickers


def _check_alert_trigger(alert_cfg: dict, market_data: dict) -> str | None:
    """Returns alert message if any condition is met, else None."""
    msgs = []
    price = market_data.get("current_price")
    rsi   = market_data.get("rsi_14")
    ticker = market_data.get("symbol", "?")

    threshold = alert_cfg.get("rsi_below")
    if threshold is not None and rsi is not None and float(rsi) < float(threshold):
        msgs.append(f"RSI14={rsi:.1f} ниже {threshold}")

    rsi_above = alert_cfg.get("rsi_above")
    if rsi_above is not None and rsi is not None and float(rsi) > float(rsi_above):
        msgs.append(f"RSI14={rsi:.1f} выше {rsi_above}")

    price_above = alert_cfg.get("price_above")
    if price_above is not None and price is not None and float(price) > float(price_above):
        msgs.append(f"Цена {price} > {price_above}")

    price_below = alert_cfg.get("price_below")
    if price_below is not None and price is not None and float(price) < float(price_below):
        msgs.append(f"Цена {price} < {price_below}")

    if msgs:
        return f"🔔 Алерт {ticker}: " + " | ".join(msgs)
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


def _build_wl_tickers_markup(category: str, page: int, user_tickers: list) -> ReplyKeyboardMarkup:
    """Like _build_tickers_markup but marks already-added tickers with ✅."""
    tickers = POPULAR_TICKERS.get(category, [])
    total_pages = max(1, (len(tickers) + TICKERS_PER_PAGE - 1) // TICKERS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    start = page * TICKERS_PER_PAGE
    page_items = tickers[start:start + TICKERS_PER_PAGE]

    labeled = [f"✅{t}" if t in user_tickers else t for t in page_items]
    rows = []
    for i in range(0, len(labeled), 2):
        rows.append(labeled[i:i + 2])

    nav_row = []
    if page > 0:
        nav_row.append('⬅️ Предыдущие')
    if page < total_pages - 1:
        nav_row.append('➡️ Следующие')
    if nav_row:
        rows.append(nav_row)
    rows.append(['🗂 Категории', '↩️ Назад'])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _format_atr_levels(data: dict, risk_profile: str = "balanced") -> str:
    """Calculate SL/TP levels from ATR; return formatted string."""
    price  = data.get("current_price")
    atr    = data.get("atr_14")
    if not price or not atr or atr == 0:
        return ""
    rule   = data.get("rule_forecast", {})
    bias   = rule.get("bias", "Нейтральный")

    _mult = {"conservative": 1.0, "balanced": 1.5, "aggressive": 2.0}
    m = _mult.get(risk_profile, 1.5)

    sl_dist  = round(atr * m, 2)
    tp1_dist = round(atr * m * 1.5, 2)
    tp2_dist = round(atr * m * 2.5, 2)
    rr_ratio = round(tp1_dist / sl_dist, 1) if sl_dist else 0

    if bias == "Бычий":
        sl   = round(price - sl_dist, 4)
        tp1  = round(price + tp1_dist, 4)
        tp2  = round(price + tp2_dist, 4)
        direction = "ЛОНГ"
    elif bias == "Медвежий":
        sl   = round(price + sl_dist, 4)
        tp1  = round(price - tp1_dist, 4)
        tp2  = round(price - tp2_dist, 4)
        direction = "ШОРТ"
    else:
        # Neutral — show both sides
        return (
            f"ATR14: {atr} | Профиль: {risk_profile}\n"
            f"Зона лонг SL/TP: -{sl_dist} / +{tp1_dist} / +{tp2_dist}\n"
            f"R/R ≈ 1:{rr_ratio}"
        )

    return (
        f"─── {direction} (ATR-уровни, {risk_profile}) ───\n"
        f"SL:  {sl}  (ATR×{m})\n"
        f"TP1: {tp1}  (R/R ≈ 1:{rr_ratio})\n"
        f"TP2: {tp2}  (ATR×{round(m*2.5,1)})\n"
        f"ATR14: {atr}"
    )


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

    # ─── Extended indicators ───────────────────────────────────
    _bb    = data.get("bollinger", {})
    _stoch = data.get("stochastic", {})
    _obv   = data.get("obv", {})
    _sar   = data.get("sar", {})
    _sr    = data.get("support_resistance", {})

    ext_lines = ""
    if _bb:
        pct_b = _bb.get("bb_pct_b", "н/д")
        bw    = _bb.get("bb_bandwidth", "н/д")
        pct_b_str = f"{pct_b:.3f}" if isinstance(pct_b, float) else str(pct_b)
        bw_str    = f"{bw:.2f}%" if isinstance(bw, float) else str(bw)
        ext_lines += f"BB: %B={pct_b_str}, BW={bw_str} | верх={_bb.get('bb_upper','н/д')} / низ={_bb.get('bb_lower','н/д')}\n"
    if _stoch:
        k = _stoch.get("stoch_k", "н/д")
        d = _stoch.get("stoch_d", "н/д")
        k_str = f"{k:.1f}" if isinstance(k, float) else str(k)
        d_str = f"{d:.1f}" if isinstance(d, float) else str(d)
        ext_lines += f"Stoch: K={k_str} D={d_str} ({_stoch.get('stoch_signal','н/д')})\n"
    if _obv:
        ext_lines += f"OBV: {_obv.get('obv_trend','н/д')}\n"
    if _sar:
        ext_lines += f"SAR: {_sar.get('sar_value','н/д')} ({_sar.get('sar_trend','н/д')})\n"
    if _sr:
        sup  = _sr.get("nearest_support", "н/д")
        res  = _sr.get("nearest_resistance", "н/д")
        d_s  = _sr.get("dist_to_support_pct", 0)
        d_r  = _sr.get("dist_to_resistance_pct", 0)
        d_s_str = f"{d_s:.1f}" if isinstance(d_s, float) else str(d_s)
        d_r_str = f"{d_r:.1f}" if isinstance(d_r, float) else str(d_r)
        ext_lines += (
            f"Поддержка: {sup} (-{d_s_str}%) | Сопротивление: {res} (+{d_r_str}%)\n"
            f"Пивот: {_sr.get('pivot_classic','н/д')}\n"
        )

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
        f"Объем (ratio): {data['volume_ratio']}\n"
        + (ext_lines and f"\n{ext_lines}") +
        f"\nRule-based: {rule['bias']} | Режим: {rule.get('regime', 'н/д')}"
        + (" ⚠️ торг. заблокирован" if not rule.get("trade_allowed", True) else "") + "\n"
        f"Рост/Падение: {rule['bullish_probability']}% / {rule['bearish_probability']}%\n"
        f"Сигнал: {rule['action']}\n"
        f"Уверенность: {rule['confidence']}{gate_line}{factors_line}{forecast_lines}\n"
        + (lambda p=data['current_price'], a=data['atr_14'], b=rule.get('bias'):
           f"SL≈{round(p-a*1.5,4)} | TP1≈{round(p+a*2.25,4)} (ATR-ориентир)\n" if b=='Бычий'
           else (f"SL≈{round(p+a*1.5,4)} | TP1≈{round(p-a*2.25,4)} (ATR-ориентир)\n" if b=='Медвежий'
           else ""))() +
        f"\n{action_block}"
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


def _compact_signal_report(data: dict, news_summary: str, news_lag_human: str = "н/д", risk_profile: str = "balanced") -> str:
    rule = data["rule_forecast"]
    impact = _derive_news_impact(news_summary)
    signal = _compose_trade_signal(data, news_summary)
    analysis_dt = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    action_block = _build_action_block(rule)
    atr_block = _format_atr_levels(data, risk_profile)

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
        + (f"{atr_block}\n\n" if atr_block else "")
        + f"{action_block}"
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


# ──────────────────────── WATCHLIST ────────────────────────
import json as _json_mod
from pathlib import Path as _Path

_WATCHLIST_DIR = _Path("data")
_WATCHLIST_MAX = 20

# Пресеты тикеров для быстрого добавления в вотчлист
_WL_PRESET_TICKERS: dict[str, list[str]] = {
    "📈 Акции США": ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "TSLA", "META", "NFLX", "AMD", "JPM", "V", "WMT"],
    "₿ Крипто":    ["BTC-USD", "ETH-USD", "SOL-USD", "BNB-USD", "XRP-USD", "DOGE-USD", "ADA-USD", "AVAX-USD"],
    "💱 Форекс":   ["EURUSD=X", "GBPUSD=X", "USDJPY=X", "USDCHF=X", "AUDUSD=X", "USDCAD=X"],
    "📦 ETF/Инд.": ["SPY", "QQQ", "GLD", "TLT", "IWM", "EEM"],
}

watchlist_menu_markup = ReplyKeyboardMarkup(
    [
        ['🔍 Скан вотчлиста'],
        ['➕ Добавить тикер', '🗑️ Удалить тикер'],
        ['📋 Мой список', '↩️ Назад'],
    ],
    resize_keyboard=True,
)

portfolio_menu_markup = ReplyKeyboardMarkup(
    [
        ['📊 P&L портфель', '📋 Мои сделки'],
        ['➕ Добавить сделку', '❌ Закрыть сделку'],
        ['📈 Бэктест', '↩️ Назад'],
    ],
    resize_keyboard=True,
)


def _wl_path(user_id: int) -> _Path:
    return _WATCHLIST_DIR / f"watchlist_{user_id}.json"


def _wl_load(user_id: int) -> list:
    p = _wl_path(user_id)
    if p.exists():
        try:
            return _json_mod.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return []


def _wl_save(user_id: int, tickers: list) -> None:
    _WATCHLIST_DIR.mkdir(parents=True, exist_ok=True)
    _wl_path(user_id).write_text(_json_mod.dumps(tickers, ensure_ascii=False), encoding="utf-8")


def _wl_add(user_id: int, ticker: str) -> str:
    """Returns status message."""
    lst = _wl_load(user_id)
    if ticker in lst:
        return f"⚠️ {ticker} уже в вотчлисте."
    if len(lst) >= _WATCHLIST_MAX:
        return f"⚠️ Вотчлист полон (макс. {_WATCHLIST_MAX} тикеров). Удали лишние."
    lst.append(ticker)
    _wl_save(user_id, lst)
    return f"✅ {ticker} добавлен в вотчлист."


def _wl_remove(user_id: int, ticker: str) -> str:
    lst = _wl_load(user_id)
    if ticker not in lst:
        return f"⚠️ {ticker} не найден в вотчлисте."
    lst.remove(ticker)
    _wl_save(user_id, lst)
    return f"✅ {ticker} удалён из вотчлиста."


def _wl_format_list(tickers: list) -> str:
    if not tickers:
        return "📌 Вотчлист пуст.\n\nДобавь тикеры кнопкой ➕ Добавить тикер."
    lines = [f"📌 Вотчлист ({len(tickers)} тикеров):\n"]
    for i, t in enumerate(tickers, 1):
        lines.append(f"  {i}. {t}")
    return "\n".join(lines)


def _wl_remove_markup(tickers: list) -> ReplyKeyboardMarkup:
    """Keyboard where each button is ❌ TICKER for easy removal."""
    rows = []
    row = []
    for t in tickers:
        row.append(f"❌ {t}")
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append(['↩️ Назад'])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def _wl_picker_markup(user_tickers: list) -> InlineKeyboardMarkup:
    """Inline keyboard with preset tickers grouped by category.
    Already-added tickers shown with ✅, rest without."""
    buttons: list[list[InlineKeyboardButton]] = []
    for category, tickers in _WL_PRESET_TICKERS.items():
        # Category header row (non-action, just label)
        buttons.append([InlineKeyboardButton(category, callback_data="wl_noop")])
        row: list[InlineKeyboardButton] = []
        for ticker in tickers:
            if ticker in user_tickers:
                label = f"✅ {ticker}"
                cb = "wl_noop"   # already added, tap does nothing
            else:
                label = ticker
                cb = f"wl_add:{ticker}"
            row.append(InlineKeyboardButton(label, callback_data=cb))
            if len(row) == 3:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)
    buttons.append([InlineKeyboardButton("✏️ Ввести вручную", callback_data="wl_manual"),
                    InlineKeyboardButton("❌ Закрыть", callback_data="wl_close")])
    return InlineKeyboardMarkup(buttons)



# ──────────────────────────────────────────
# Base handlers (used by many other handlers)
# ──────────────────────────────────────────
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
        "📖 *VANGUARD BOT — СПРАВКА*\n\n"
        "*Команды:*\n"
        "/start — главное меню\n"
        "/help — эта справка\n"
        "/reset — сбросить текущий режим\n"
        "/stats — статистика использования\n"
        "/lang ru|en — язык интерфейса\n"
        "/cancel — назад в меню\n\n"
        "*Алерты (RSI):*\n"
        "`/alert add BTC-USD 30` — добавить алерт RSI ≤ 30\n"
        "`/alert add AAPL 75` — добавить алерт RSI ≥ 75\n"
        "`/alert del BTC-USD` — удалить алерт\n"
        "`/alert list` — список алертов\n"
        "Проверка каждые 15 минут. Уведомление при пересечении порога.\n\n"
        "*Сравнение тикеров:*\n"
        "`/compare AAPL MSFT` — сравнить два тикера рядом\n\n"
        "*Портфель и бэктест:*\n"
        "`/trade add AAPL buy 185.50 5` — добавить ЛОНГ\n"
        "`/trade add BTC-USD sell 45000 0.5` — добавить ШОРТ\n"
        "`/trade close N` — закрыть сделку №N\n"
        "`/trade list` — список открытых сделок\n"
        "`/portfolio` — P&L по текущим ценам\n"
        "`/backtest AAPL` — точность прогнозов по тикеру\n"
        "`/backtest` — общая таблица по всем тикерам\n\n"
        "*Вотчлист (📌):*\n"
        "• Добавить тикер — выбор категории → страница → тикер\n"
        "• Скан вотчлиста — RSI, bias, вероятности по всем тикерам\n"
        "• Мой список — просмотреть добавленные тикеры\n"
        "• Утренний дайджест в 08:00 UTC автоматически\n\n"
        "*Модули (кнопки):*\n"
        "📈 AI-Анализ — тех. снимок + AI-объяснение\n"
        "  Индикаторы: EMA200, SMA20/50, RSI, ADX, MACD, BollingerBands,\n"
        "  Stochastic, OBV, Parabolic SAR, мультифрейм, Fear&Greed, объём\n"
        "🎯 Сигнал — краткий торговый вывод с уровнями SL/TP\n"
        "🗞 Новости — сводка и оценка фона по тикеру\n"
        "📊 Прогноз — история D3-прогнозов, точность по тикерам, Excel-отчёт\n"
        "🧠 Модели — выбор AI: Llama / DeepSeek\n\n"
        "*Быстрый старт:*\n"
        "1) 📈 AI-Анализ → введи тикер (или выбери)\n"
        "2) Читай: вероятности, bias, прогноз Д1/Д2/Д3\n"
        "3) 🎯 Сигнал → краткий итог\n"
        "4) 🗞 Новости → проверь фон\n"
        "5) /reset — если застрял в режиме",
        parse_mode="Markdown",
    )
    return MENU



async def reset_session(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    _inc_stat(context, "cmd_reset")
    _clear_user_flow_state(context)
    await update.message.reply_text("🔄 Режим сброшен.", reply_markup=_main_markup(context))
    return MENU


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if await _deny_if_unauthorized(update, context):
        return ConversationHandler.END
    _clear_user_flow_state(context)
    await update.message.reply_text("↩️ Возврат в меню.", reply_markup=_main_markup(context))
    return MENU


# ──────────────────────── ФОНОВЫЕ ЗАДАЧИ ────────────────────────
async def _job_check_alerts(context) -> None:
    """
    Runs every 15 minutes. Checks all user alerts against live market data.
    Sends a message to the user when a condition is triggered.
    Tracks fired alerts to avoid re-firing until condition clears.
    """
    try:
        # user_data is per-user — job_context stores all users' data in bot_data
        # We iterate bot_data for all user ids with alerts
        users_alerts = context.bot_data.get("user_alerts_map", {})
        for chat_id_str, alerts_snapshot in list(users_alerts.items()):
            if not alerts_snapshot:
                continue
            fired_set = context.bot_data.setdefault("alerts_fired", {}).setdefault(chat_id_str, set())
            for ticker, cfg in alerts_snapshot.items():
                try:
                    data = await asyncio.to_thread(get_market_data, ticker)
                except Exception:
                    continue
                if not data:
                    continue
                msg = _check_alert_trigger(cfg, data)
                alert_key = f"{ticker}:{hash(str(sorted(cfg.items())))}"
                if msg and alert_key not in fired_set:
                    fired_set.add(alert_key)
                    try:
                        await context.bot.send_message(chat_id=int(chat_id_str), text=msg)
                    except Exception:
                        pass
                elif not msg and alert_key in fired_set:
                    # Condition cleared — reset so it can fire again
                    fired_set.discard(alert_key)
    except Exception as exc:
        logging.warning("_job_check_alerts failed: %s", exc)


async def _job_watchlist_digest(context) -> None:
    """
    Runs daily at 08:00 UTC.
    Sends a brief watchlist scan to each user who has a watchlist.
    """
    try:
        user_wl_map = context.bot_data.get("user_wl_map", {})
        _BIAS_EMOJI = {"Бычий": "🐂", "Медвежий": "🐻", "Нейтральный": "⚖️"}
        for chat_id_str, tickers in list(user_wl_map.items()):
            if not tickers:
                continue
            rows = []
            for ticker in tickers:
                try:
                    data = await asyncio.to_thread(get_market_data, ticker)
                except Exception:
                    rows.append(f"  ❓ {ticker:10s} — ошибка")
                    continue
                if not data:
                    rows.append(f"  ❓ {ticker:10s} — нет данных")
                    continue
                rule = data.get("rule_forecast", {})
                bias = rule.get("bias", "?")
                bull = rule.get("bullish_probability", "?")
                rsi  = data.get("rsi_14", "?")
                emoji = _BIAS_EMOJI.get(bias, "⚖️")
                rsi_str = f"{rsi:.1f}" if isinstance(rsi, float) else str(rsi)
                rows.append(f"  {emoji} {ticker:10s} | {bias:10s} | ↑{bull}% | RSI {rsi_str}")
            if rows:
                now_utc = datetime.now(timezone.utc).strftime("%d.%m %H:%M UTC")
                text = f"📌 Утренний дайджест вотчлиста — {now_utc}\n" + "─" * 44 + "\n" + "\n".join(rows)
                try:
                    await context.bot.send_message(chat_id=int(chat_id_str), text=text[:3900])
                except Exception:
                    pass
    except Exception as exc:
        logging.warning("_job_watchlist_digest failed: %s", exc)


def _sync_user_alerts_map(context) -> None:
    """Called after every alert_command to keep bot_data.user_alerts_map in sync."""
    pass  # user_data changes are tracked via patched open_watchlist and alert_command

# ──────────────────────── END ФОНОВЫЕ ЗАДАЧИ ────────────────────────


