import re
from datetime import datetime, timezone

TICKER_RE = re.compile(r"^[A-Z0-9=\-\.\^]{1,20}$")


def format_lag(delta_seconds: float) -> str:
    if delta_seconds < 0:
        return "0 мин"
    minutes = int(delta_seconds // 60)
    if minutes < 60:
        return f"{minutes} мин"
    hours = minutes // 60
    if hours < 24:
        return f"{hours} ч {minutes % 60} мин"
    days = hours // 24
    return f"{days} д {hours % 24} ч"


def normalize_nav_text(text: str) -> str:
    cleaned = (text or "").replace("\ufe0f", "").strip()
    return " ".join(cleaned.split()).lower()


def normalize_ticker(text: str) -> str:
    return (text or "").strip().upper().replace(" ", "")


def validate_ticker(text: str) -> bool:
    ticker = normalize_ticker(text)
    return bool(TICKER_RE.fullmatch(ticker))


def split_text(text: str, limit: int = 3900) -> list[str]:
    chunks = []
    current = (text or "").strip()
    while len(current) > limit:
        split_at = current.rfind("\n", 0, limit)
        if split_at < 1:
            split_at = limit
        chunks.append(current[:split_at])
        current = current[split_at:].lstrip("\n")
    if current:
        chunks.append(current)
    return chunks


def utc_now_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
