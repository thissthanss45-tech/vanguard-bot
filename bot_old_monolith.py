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

    # ── Фоновые задачи ──
    jq = app.job_queue
    if jq:
        # Проверка алертов каждые 15 минут
        jq.run_repeating(_job_check_alerts, interval=900, first=60, name="check_alerts")
        # Утренний дайджест вотчлиста — каждый день в 08:00 UTC
        import datetime as _dt
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