"""Stripe webhook: обработка событий подписки.

Поддерживаемые события:
  customer.subscription.created   → tier = pro или enterprise
  customer.subscription.updated   → изменение тира
  customer.subscription.deleted   → tier = free
  invoice.payment_failed           → уведомление (логируем)

Настройка:
  STRIPE_WEBHOOK_SECRET=whsec_...  в .env
  STRIPE_API_KEY=sk_live_...       в .env

Без STRIPE_WEBHOOK_SECRET webhook выключен (возвращает 503).
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
import time

from fastapi import APIRouter, Header, HTTPException, Request, status
from sqlalchemy.orm import Session
from fastapi import Depends

from api.database import get_db
from api.models import ApiKey

logger = logging.getLogger(__name__)

STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")

# Маппинг Stripe price_id / product_id → тир
# Заполни под свои продукты в Stripe Dashboard
PRICE_TIER_MAP: dict[str, str] = {
    os.getenv("STRIPE_PRICE_PRO", "price_pro_placeholder"):        "pro",
    os.getenv("STRIPE_PRICE_ENTERPRISE", "price_ent_placeholder"): "enterprise",
}

router = APIRouter(prefix="/webhook", tags=["billing"])


def _verify_stripe_signature(payload: bytes, sig_header: str, secret: str) -> bool:
    """Проверяем подпись Stripe webhook (HMAC-SHA256)."""
    try:
        parts = {k: v for item in sig_header.split(",") for k, v in [item.split("=", 1)]}
        timestamp = parts.get("t", "")
        signatures = [v for k, v in parts.items() if k == "v1"]
        signed_payload = f"{timestamp}.".encode() + payload
        expected = hmac.new(secret.encode(), signed_payload, hashlib.sha256).hexdigest()
        return any(hmac.compare_digest(expected, sig) for sig in signatures)
    except Exception:
        return False


def _get_tier_for_subscription(event_data: dict) -> str:
    """Определяем тир по items[0].price.id из subscription объекта."""
    items = event_data.get("items", {}).get("data", [])
    if items:
        price_id = items[0].get("price", {}).get("id", "")
        return PRICE_TIER_MAP.get(price_id, "pro")  # default pro если неизвестный price
    return "pro"


def _find_key_by_stripe_sub(db: Session, sub_id: str) -> ApiKey | None:
    return db.query(ApiKey).filter(ApiKey.stripe_subscription_id == sub_id).first()


def _find_or_create_key(db: Session, email: str, sub_id: str, tier: str) -> ApiKey:
    """Ищем ключ по email или sub_id, создаём если нет."""
    from api.auth import generate_api_key
    key_obj = db.query(ApiKey).filter(ApiKey.owner_email == email).first()
    if not key_obj:
        key_obj = ApiKey(
            key=generate_api_key(),
            label=f"stripe-{tier}",
            owner_email=email,
            tier=tier,
            stripe_subscription_id=sub_id,
        )
        db.add(key_obj)
        logger.info("🔑 new API key created via Stripe", extra={"email": email, "tier": tier})
    else:
        key_obj.tier = tier
        key_obj.stripe_subscription_id = sub_id
        key_obj.is_active = True
        logger.info("🔄 API key tier updated via Stripe", extra={"email": email, "tier": tier})
    db.commit()
    db.refresh(key_obj)
    return key_obj


@router.post(
    "/stripe",
    status_code=status.HTTP_200_OK,
    summary="Stripe webhook receiver",
    include_in_schema=False,  # не показываем в OpenAPI docs
)
async def stripe_webhook(
    request: Request,
    stripe_signature: str = Header(alias="stripe-signature", default=""),
    db: Session = Depends(get_db),
):
    if not STRIPE_WEBHOOK_SECRET:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Stripe webhook is not configured (STRIPE_WEBHOOK_SECRET missing).",
        )

    payload = await request.body()

    if not _verify_stripe_signature(payload, stripe_signature, STRIPE_WEBHOOK_SECRET):
        logger.warning("❌ Stripe webhook signature mismatch")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid Stripe signature.",
        )

    # Stripe не требует json-парсинга через library (без stripe-python SDK)
    import json
    try:
        event = json.loads(payload)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload.")

    event_type = event.get("type", "")
    event_data = event.get("data", {}).get("object", {})

    logger.info("✅ Stripe event received", extra={"event_type": event_type})

    if event_type == "customer.subscription.created":
        sub_id = event_data.get("id", "")
        tier = _get_tier_for_subscription(event_data)
        email = event_data.get("metadata", {}).get("email", "")
        if email:
            key_obj = _find_or_create_key(db, email, sub_id, tier)
            logger.info("✅ subscription created", extra={"email": email, "tier": tier, "key": key_obj.key[:12] + "***"})

    elif event_type == "customer.subscription.updated":
        sub_id = event_data.get("id", "")
        tier = _get_tier_for_subscription(event_data)
        key_obj = _find_key_by_stripe_sub(db, sub_id)
        if key_obj:
            key_obj.tier = tier
            db.commit()
            logger.info("🔄 subscription updated", extra={"sub_id": sub_id, "new_tier": tier})

    elif event_type == "customer.subscription.deleted":
        sub_id = event_data.get("id", "")
        key_obj = _find_key_by_stripe_sub(db, sub_id)
        if key_obj:
            key_obj.tier = "free"
            key_obj.stripe_subscription_id = None
            db.commit()
            logger.info("⬇️ subscription cancelled → downgraded to free", extra={"sub_id": sub_id})

    elif event_type == "invoice.payment_failed":
        sub_id = event_data.get("subscription", "")
        logger.warning("⚠️ payment failed", extra={"sub_id": sub_id})

    else:
        logger.debug("ℹ️ unhandled Stripe event", extra={"event_type": event_type})

    return {"received": True}
