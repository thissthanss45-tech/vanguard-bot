import logging
from datetime import datetime, timezone

import yfinance as yf
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from utils import format_lag

logger = logging.getLogger(__name__)
_SENTIMENT_PIPELINE = None


def _extract_news_item(item):
    content = item.get("content") if isinstance(item, dict) else None
    if not isinstance(content, dict):
        return None

    title = content.get("title") or "Без заголовка"
    provider = content.get("provider")
    publisher = provider.get("displayName") if isinstance(provider, dict) else None
    if not publisher:
        publisher = content.get("publisher") or "Источник не указан"

    canonical = content.get("canonicalUrl")
    click_through = content.get("clickThroughUrl")
    link = canonical.get("url") if isinstance(canonical, dict) else None
    if not link:
        link = click_through.get("url") if isinstance(click_through, dict) else None
    if not link:
        link = content.get("previewUrl") or "Ссылка недоступна"

    summary = content.get("summary") or content.get("description") or ""
    summary = str(summary).replace("\n", " ").strip()
    pub_raw = content.get("pubDate") or content.get("displayTime")

    return {
        "title": str(title).strip(),
        "publisher": str(publisher).strip(),
        "link": str(link).strip(),
        "summary": summary,
        "pub_raw": pub_raw,
    }


def _to_datetime_utc(pub_raw):
    if pub_raw is None:
        return None

    try:
        if isinstance(pub_raw, (int, float)):
            ts = float(pub_raw)
            if ts > 10_000_000_000:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)

        text = str(pub_raw).strip()
        if not text:
            return None

        if text.isdigit():
            ts = float(text)
            if ts > 10_000_000_000:
                ts /= 1000.0
            return datetime.fromtimestamp(ts, tz=timezone.utc)

        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def _sentiment_pipeline():
    global _SENTIMENT_PIPELINE
    if _SENTIMENT_PIPELINE is not None:
        return _SENTIMENT_PIPELINE

    try:
        from transformers import pipeline

        _SENTIMENT_PIPELINE = pipeline("sentiment-analysis")
    except Exception as exc:
        logger.warning("transformers sentiment unavailable: %s", exc)
        _SENTIMENT_PIPELINE = False
    return _SENTIMENT_PIPELINE


def _calc_sentiment(text: str) -> dict:
    model = _sentiment_pipeline()
    if not model:
        lowered = (text or "").lower()
        positive_words = ["growth", "beat", "upgrade", "bull", "рост", "позитив"]
        negative_words = ["downgrade", "miss", "risk", "bear", "падение", "негатив"]
        score = sum(word in lowered for word in positive_words) - sum(word in lowered for word in negative_words)
        if score > 0:
            return {"label": "POSITIVE", "score": 0.55}
        if score < 0:
            return {"label": "NEGATIVE", "score": 0.55}
        return {"label": "NEUTRAL", "score": 0.5}

    try:
        result = model(text[:512])[0]
        label = str(result.get("label", "NEUTRAL")).upper()
        return {"label": label, "score": float(result.get("score", 0.5))}
    except Exception as exc:
        logger.warning("sentiment model failed: %s", exc)
        return {"label": "NEUTRAL", "score": 0.5}


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=8),
    retry=retry_if_exception_type(Exception),
    reraise=True,
)
def _load_news(ticker_symbol: str):
    ticker = yf.Ticker(ticker_symbol)
    return ticker.news


def get_ticker_news_payload(ticker_symbol):
    try:
        news = _load_news(ticker_symbol)
        if not news:
            return {
                "text": "Новостей по данному тикеру за последнее время не найдено.",
                "news_lag_seconds": None,
                "news_lag_human": "н/д",
                "latest_news_utc": None,
                "news_count": 0,
                "sentiment": {"label": "NEUTRAL", "score": 0.5},
            }

        formatted_news = []
        news_times = []
        sentiment_buffer = []

        for item in news[:8]:
            parsed = _extract_news_item(item)
            if not parsed:
                continue

            published_dt = _to_datetime_utc(parsed.get("pub_raw"))
            if published_dt:
                news_times.append(published_dt)
                pub_str = published_dt.strftime("%Y-%m-%d %H:%M UTC")
            else:
                pub_str = "время н/д"

            summary_part = f"\n📝 {parsed['summary'][:220]}" if parsed["summary"] else ""
            formatted_news.append(
                f"🔹 {parsed['title']} ({parsed['publisher']})\n"
                f"🕒 {pub_str}\n"
                f"🔗 {parsed['link']}{summary_part}"
            )
            sentiment_buffer.append(f"{parsed['title']} {parsed['summary']}")

        if not formatted_news:
            return {
                "text": "Новости найдены, но формат источника изменился и их не удалось распарсить.",
                "news_lag_seconds": None,
                "news_lag_human": "н/д",
                "latest_news_utc": None,
                "news_count": 0,
                "sentiment": {"label": "NEUTRAL", "score": 0.5},
            }

        latest_news = max(news_times) if news_times else None
        if latest_news:
            lag_seconds = int(max(0, (datetime.now(timezone.utc) - latest_news).total_seconds()))
            lag_human = format_lag(lag_seconds)
            latest_news_utc = latest_news.strftime("%Y-%m-%d %H:%M UTC")
        else:
            lag_seconds = None
            lag_human = "н/д"
            latest_news_utc = None

        sentiment = _calc_sentiment("\n".join(sentiment_buffer))

        return {
            "text": "\n\n".join(formatted_news),
            "news_lag_seconds": lag_seconds,
            "news_lag_human": lag_human,
            "latest_news_utc": latest_news_utc,
            "news_count": len(formatted_news),
            "sentiment": sentiment,
        }
    except Exception as exc:
        logger.exception("news error for %s: %s", ticker_symbol, exc)
        return {
            "text": f"Ошибка при получении новостей: {exc}",
            "news_lag_seconds": None,
            "news_lag_human": "н/д",
            "latest_news_utc": None,
            "news_count": 0,
            "sentiment": {"label": "NEUTRAL", "score": 0.5},
        }


def get_ticker_news(ticker_symbol):
    payload = get_ticker_news_payload(ticker_symbol)
    return payload["text"]


if __name__ == "__main__":
    print(get_ticker_news("BZ=F"))
