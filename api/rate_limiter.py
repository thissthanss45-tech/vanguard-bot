"""Tier-based rate limiter: считаем запросы в БД за сегодня.

free:       10  req/day
pro:       200  req/day
enterprise: ∞  (no limit)
"""
from __future__ import annotations

import time
from datetime import datetime, timezone

from fastapi import Depends, HTTPException, status
from sqlalchemy.orm import Session

from api.auth import get_current_key
from api.database import get_db
from api.models import ApiKey, UsageLog


def _requests_today(db: Session, key_id: int) -> int:
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return (
        db.query(UsageLog)
        .filter(UsageLog.api_key_id == key_id, UsageLog.created_at >= today_start)
        .count()
    )


def check_rate_limit(
    key: ApiKey = Depends(get_current_key),
    db: Session = Depends(get_db),
) -> ApiKey:
    """FastAPI dependency: проверяет лимит, бросает 429 при превышении."""
    if key.tier == "enterprise":
        return key  # без лимита

    if key.id == 0:
        return key  # admin virtual key

    used = _requests_today(db, key.id)
    limit = key.daily_limit

    if used >= limit:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=(
                f"Daily limit reached: {used}/{limit} requests today. "
                f"Upgrade to Pro at /pricing for 200 req/day."
            ),
            headers={"X-RateLimit-Limit": str(limit), "X-RateLimit-Remaining": "0"},
        )

    return key


def log_usage(
    db: Session,
    key: ApiKey,
    endpoint: str,
    ticker: str | None,
    status_code: int,
    latency_ms: int,
    error: str | None = None,
) -> None:
    """Записываем запрос в usage_logs."""
    if key.id == 0:
        return  # admin virtual key — не логируем
    entry = UsageLog(
        api_key_id=key.id,
        endpoint=endpoint,
        ticker=ticker,
        status_code=status_code,
        latency_ms=latency_ms,
        error=error,
    )
    db.add(entry)
    db.commit()
