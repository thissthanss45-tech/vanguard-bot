import logging
import os
import time

from dotenv import load_dotenv
from openai import OpenAI
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config import SETTINGS

load_dotenv()
logger = logging.getLogger(__name__)
AI_PROVIDER_TIMEOUT_SEC = float(os.getenv("AI_PROVIDER_TIMEOUT_SEC", "22"))
AI_TOTAL_BUDGET_SEC = float(os.getenv("AI_TOTAL_BUDGET_SEC", "24"))
DEEPSEEK_STRICT = os.getenv("DEEPSEEK_STRICT", "true").lower() in {"1", "true", "yes", "on"}
DEEPSEEK_FAST_PROMPT = os.getenv("DEEPSEEK_FAST_PROMPT", "true").lower() in {"1", "true", "yes", "on"}
DEEPSEEK_REQUIRE_SUCCESS = os.getenv("DEEPSEEK_REQUIRE_SUCCESS", "true").lower() in {"1", "true", "yes", "on"}
DEEPSEEK_MAX_TOKENS = int(os.getenv("DEEPSEEK_MAX_TOKENS", "280"))
GROQ_MAX_TOKENS = int(os.getenv("GROQ_MAX_TOKENS", "520"))
CLAUDE_MAX_TOKENS = int(os.getenv("CLAUDE_MAX_TOKENS", "700"))


PRO_MARKET_PROMPT_TEMPLATE = """Ты — профессиональный количественный аналитик и трейдер с 15+ годами опыта (акции/крипто/фьючерсы/форекс).

Ограничения роли:
- Не давай прямых рекомендаций (купить/продать/входить сейчас).
- Уровни (вход/цели/стоп) допустимы только как гипотетические сценарные ориентиры.
- Основывайся только на входных данных. Без галлюцинаций.

Структура ответа (строго, максимум 420 слов):
1) Краткий обзор рынка (2-3 предложения) с учетом risk_profile={risk_profile}.
2) Технический анализ (4-6 bullets): цена vs SMA20/SMA50/EMA200, RSI14/ATR14/ADX14, 1d change, volume ratio, range 20d, режим.
3) Вероятностные сценарии (бычий/медвежий/нейтральный), сумма = 100%. База: rule_forecast, корректировка не более ±5 п.п.
4) Сценарные уровни и риски (markdown-таблица): Уровень | Цена | Тип | Комментарий.
5) Гипотетический план 1-3 дня (3-4 предложения), R:R минимум по профилю:
    - conservative >= 1:2.5
    - balanced >= 1:2
    - aggressive >= 1:1.5
    + 2 условия инвалидации.
6) Если не хватает критичных полей — раздел "Ограничения данных" и фраза "Недостаточно данных для полноценного анализа".

Входные данные:
- symbol: {symbol}
- data_lag_human: {data_lag_human}
- last_candle_utc: {last_candle_utc}
- current_price: {current_price}
- change_pct_1d: {change_pct_1d}
- sma_20: {sma_20}
- sma_50: {sma_50}
- ema_200: {ema_200}
- rsi_14: {rsi_14}
- atr_14: {atr_14}
- adx_14: {adx_14}
- annualized_volatility_pct: {annualized_volatility_pct}
- low_20d/high_20d: {low_20d}/{high_20d}
- corr_with_spy_60d: {corr_with_spy_60d}
- corr_with_btc_60d: {corr_with_btc_60d}
- rule_forecast: bias={bias}, bull={bull}%, bear={bear}%, action={action}, confidence={confidence}, regime={regime}, trade_allowed={trade_allowed}, gate_reason={gate_reason}
- forecast_3d: {forecast_3d_text}
- backtest: trades={trades}, win_rate={win_rate}%, total_return_pct={total_return_pct}%

Финальная строка обязательна:
"Это не инвестиционный совет. Рынки рискованны, используйте собственный анализ."
"""


def _openai_client(provider: str):
    if provider == "deepseek":
        api_key = os.getenv("DEEPSEEK_API_KEY")
        if not api_key:
            return None
        return OpenAI(
            api_key=api_key,
            base_url="https://api.deepseek.com",
            timeout=AI_PROVIDER_TIMEOUT_SEC,
            max_retries=0,
        )

    if provider == "groq":
        api_key = os.getenv("GROQ_API_KEY")
        if not api_key:
            return None
        return OpenAI(
            api_key=api_key,
            base_url="https://api.groq.com/openai/v1",
            timeout=AI_PROVIDER_TIMEOUT_SEC,
            max_retries=0,
        )

    return None


def _local_prediction_fallback(market_data):
    rule = market_data["rule_forecast"]
    return (
        "AI-провайдер недоступен, используется локальный прогноз.\n"
        f"Режим: {rule['bias']}\n"
        f"Вероятность роста: {rule['bullish_probability']}%\n"
        f"Вероятность снижения: {rule['bearish_probability']}%\n"
        f"Сигнал: {rule['action']}\n"
        f"Уверенность: {rule['confidence']}"
    )


def _pick_model(provider: str) -> str:
    if provider == "deepseek":
        return os.getenv("DEEPSEEK_MODEL", "deepseek-chat")
    if provider == "groq":
        return os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
    return ""


def _provider_chain(preferred: str):
    preferred = (preferred or "deepseek").lower()
    if preferred == "deepseek" and DEEPSEEK_STRICT:
        return ["deepseek"]
    chain = []
    if preferred in {"deepseek", "groq", "claude"}:
        chain.append(preferred)
    for item in ["deepseek", "groq", "claude"]:
        if item not in chain:
            chain.append(item)
    return chain


@retry(
    stop=stop_after_attempt(1),
    wait=wait_exponential(multiplier=0.2, min=0.2, max=1),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def _call_openai_chat(provider: str, prompt: str) -> str:
    client = _openai_client(provider)
    if client is None:
        raise RuntimeError(f"No client configured for {provider}")

    max_tokens = DEEPSEEK_MAX_TOKENS if provider == "deepseek" else GROQ_MAX_TOKENS
    response = client.chat.completions.create(
        model=_pick_model(provider),
        messages=[
            {"role": "system", "content": "Ты эксперт-аналитик биржи. Пиши конкретно и численно."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
        top_p=0.9,
        max_tokens=max_tokens,
    )
    return response.choices[0].message.content


def _call_claude(prompt: str) -> str:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not configured")

    import anthropic

    client = anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model=os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest"),
        max_tokens=CLAUDE_MAX_TOKENS,
        temperature=0.2,
        system="Ты эксперт-аналитик биржи. Пиши конкретно и численно.",
        messages=[{"role": "user", "content": prompt}],
    )
    for part in msg.content:
        if getattr(part, "type", "") == "text":
            return part.text
    raise RuntimeError("Claude response has no text")


def _safe_call_provider(provider: str, prompt: str) -> str:
    try:
        if provider == "claude":
            return _call_claude(prompt)
        return _call_openai_chat(provider, prompt)
    except Exception as exc:
        raise


def get_ai_prediction(market_data, provider="deepseek", risk_profile="balanced"):
    rule = market_data["rule_forecast"]
    forecast_3d = market_data.get("forecast_3d", [])
    bt = market_data.get("backtest", {})

    forecast_3d_text = (
        " | ".join(
            [
                f"Д{item['day']}: {item['bullish_probability']}%/{item['bearish_probability']}% ({item['bias']})"
                for item in forecast_3d
            ]
        )
        if forecast_3d
        else "н/д"
    )

    long_prompt = PRO_MARKET_PROMPT_TEMPLATE.format(
        risk_profile=risk_profile,
        symbol=market_data.get("symbol", "н/д"),
        data_lag_human=market_data.get("data_lag_human", "н/д"),
        last_candle_utc=market_data.get("last_candle_utc", "н/д"),
        current_price=market_data.get("current_price", "н/д"),
        change_pct_1d=market_data.get("change_pct_1d", "н/д"),
        sma_20=market_data.get("sma_20", "н/д"),
        sma_50=market_data.get("sma_50", "н/д"),
        ema_200=market_data.get("ema_200", "н/д"),
        rsi_14=market_data.get("rsi_14", "н/д"),
        atr_14=market_data.get("atr_14", "н/д"),
        adx_14=market_data.get("adx_14", "н/д"),
        annualized_volatility_pct=market_data.get("annualized_volatility_pct", "н/д"),
        low_20d=market_data.get("low_20d", "н/д"),
        high_20d=market_data.get("high_20d", "н/д"),
        corr_with_spy_60d=market_data.get("corr_with_spy_60d", "н/д"),
        corr_with_btc_60d=market_data.get("corr_with_btc_60d", "н/д"),
        bias=rule.get("bias", "н/д"),
        bull=rule.get("bullish_probability", "н/д"),
        bear=rule.get("bearish_probability", "н/д"),
        action=rule.get("action", "н/д"),
        confidence=rule.get("confidence", "н/д"),
        regime=rule.get("regime", "н/д"),
        trade_allowed=rule.get("trade_allowed", True),
        gate_reason=rule.get("gate_reason", "нет") or "нет",
        forecast_3d_text=forecast_3d_text,
        trades=bt.get("trades", 0),
        win_rate=bt.get("win_rate", 0),
        total_return_pct=bt.get("total_return_pct", 0),
    )

    fast_prompt = (
        "Роль: кванта-аналитик. Русский язык. Без воды. Без сигналов купить/продать.\n"
        "Дай КРАТКИЙ отчёт (до 220 слов) строго в 5 блоках:\n"
        "1) Обзор (2 предложения).\n"
        "2) Техника (4 bullets).\n"
        "3) Сценарии: Быч/Медв/Нейтр, сумма=100%.\n"
        "4) Сценарные уровни таблицей: Уровень|Цена|Тип|Комментарий.\n"
        "5) План 1-3 дня + 2 условия инвалидации.\n"
        "Финал строкой: Это не инвестиционный совет.\n\n"
        f"risk={risk_profile}; sym={market_data.get('symbol','н/д')}; px={market_data.get('current_price','н/д')}; d1={market_data.get('change_pct_1d','н/д')}%; "
        f"S20={market_data.get('sma_20','н/д')}; S50={market_data.get('sma_50','н/д')}; E200={market_data.get('ema_200','н/д')}; "
        f"RSI={market_data.get('rsi_14','н/д')}; ATR={market_data.get('atr_14','н/д')}; ADX={market_data.get('adx_14','н/д')}; vol={market_data.get('annualized_volatility_pct','н/д')}%; "
        f"R20={market_data.get('low_20d','н/д')}-{market_data.get('high_20d','н/д')}; corrSPY={market_data.get('corr_with_spy_60d','н/д')}; corrBTC={market_data.get('corr_with_btc_60d','н/д')}; "
        f"rule(bias={rule.get('bias','н/д')},bull={rule.get('bullish_probability','н/д')},bear={rule.get('bearish_probability','н/д')},conf={rule.get('confidence','н/д')},regime={rule.get('regime','н/д')},gate={rule.get('gate_reason','нет')}); "
        f"f3d={forecast_3d_text}"
    )

    prompt = fast_prompt if (provider == "deepseek" and DEEPSEEK_FAST_PROMPT) else long_prompt

    started_at = time.monotonic()
    errors = []
    for current_provider in _provider_chain(provider):
        elapsed = time.monotonic() - started_at
        if elapsed >= AI_TOTAL_BUDGET_SEC:
            errors.append("time budget exceeded")
            break
        try:
            return _safe_call_provider(current_provider, prompt)
        except Exception as exc:
            logger.warning("AI provider failed (%s): %s", current_provider, exc)
            errors.append(f"{current_provider}: {exc}")

    error_text = "; ".join(errors) if errors else "нет доступных AI-провайдеров"

    if provider == "deepseek" and DEEPSEEK_STRICT and DEEPSEEK_REQUIRE_SUCCESS:
        return (
            "Ошибка AI-анализа (DeepSeek не вернул ответ вовремя).\n"
            f"Техническая причина: {error_text}.\n"
            "Источник ответа: DeepSeek отсутствует (fallback отключён)."
        )

    return f"Ошибка AI-анализа ({error_text}).\n\n{_local_prediction_fallback(market_data)}"


def analyze_news(news_text, provider="groq"):
    clipped_news = (news_text or "")[:6000]

    prompt = f"""
Проанализируй новости и верни:
1) 3 кратких пункта по сути,
2) Тон: Позитивно / Негативно / Нейтрально,
3) Вероятное влияние на цену в % диапазоне на 1-3 дня,
4) Краткий risk-reward комментарий.

Новости:
{clipped_news}
"""

    started_at = time.monotonic()
    errors = []
    for current_provider in _provider_chain(provider):
        elapsed = time.monotonic() - started_at
        if elapsed >= AI_TOTAL_BUDGET_SEC:
            errors.append("time budget exceeded")
            break
        try:
            return _safe_call_provider(current_provider, prompt)
        except Exception as exc:
            logger.warning("News provider failed (%s): %s", current_provider, exc)
            errors.append(f"{current_provider}: {exc}")

    return f"Ошибка анализа новостей: {'; '.join(errors) if errors else 'нет доступных AI-провайдеров'}"
