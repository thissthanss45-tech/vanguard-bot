"""SQLAlchemy модели: ApiKey, UsageLog."""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum as PyEnum

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from api.database import Base


class Tier(str, PyEnum):
    free = "free"          # 10 запросов/день, только /forecast
    pro = "pro"            # 200 запросов/день, всё
    enterprise = "enterprise"  # без лимита, всё + priority


TIER_DAILY_LIMITS: dict[str, int] = {
    "free": 10,
    "pro": 200,
    "enterprise": 999_999,
}

TIER_PRICE_USD: dict[str, float] = {
    "free": 0.0,
    "pro": 29.0,
    "enterprise": 149.0,
}


class ApiKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    key: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    label: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    owner_email: Mapped[str] = mapped_column(String(256), nullable=False, default="")
    tier: Mapped[str] = mapped_column(String(32), nullable=False, default=Tier.free)
    is_active: Mapped[bool] = mapped_column(default=True)
    is_admin: Mapped[bool] = mapped_column(default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Stripe subscription id (nullable — при ручном создании)
    stripe_subscription_id: Mapped[str | None] = mapped_column(String(128), nullable=True)

    usage_logs: Mapped[list[UsageLog]] = relationship("UsageLog", back_populates="api_key")

    @property
    def daily_limit(self) -> int:
        return TIER_DAILY_LIMITS.get(self.tier, 10)


class UsageLog(Base):
    __tablename__ = "usage_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    api_key_id: Mapped[int] = mapped_column(ForeignKey("api_keys.id"), nullable=False)
    endpoint: Mapped[str] = mapped_column(String(128), nullable=False)
    ticker: Mapped[str | None] = mapped_column(String(32), nullable=True)
    status_code: Mapped[int] = mapped_column(Integer, nullable=False, default=200)
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        index=True,
    )
    error: Mapped[str | None] = mapped_column(Text, nullable=True)

    api_key: Mapped[ApiKey] = relationship("ApiKey", back_populates="usage_logs")

    __table_args__ = (
        Index("ix_usage_key_date", "api_key_id", "created_at"),
    )
