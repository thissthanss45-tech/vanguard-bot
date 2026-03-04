"""API Key management endpoints.

POST   /api/v1/keys            — создать ключ (admin only)
GET    /api/v1/keys/me         — мой ключ + usage
GET    /api/v1/keys/me/usage   — история запросов
DELETE /api/v1/keys/{key_id}   — деактивировать (admin only)
GET    /api/v1/keys            — список всех ключей (admin only)
"""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from api.auth import generate_api_key, get_current_key, mask_key, require_admin
from api.database import get_db
from api.models import ApiKey
from api.rate_limiter import _requests_today
from api.schemas import (
    ApiKeyCreate,
    ApiKeyInfo,
    ApiKeyResponse,
    UsageEntry,
    UsageStatsResponse,
)

router = APIRouter(prefix="/keys", tags=["api-keys"])


@router.post(
    "",
    response_model=ApiKeyResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create API key (admin only)",
)
def create_key(
    payload: ApiKeyCreate,
    admin: ApiKey = Depends(require_admin),
    db: Session = Depends(get_db),
):
    key_str = generate_api_key()
    key_obj = ApiKey(
        key=key_str,
        label=payload.label,
        owner_email=payload.owner_email,
        tier=payload.tier,
        is_admin=payload.is_admin,
    )
    db.add(key_obj)
    db.commit()
    db.refresh(key_obj)
    return ApiKeyResponse(
        id=key_obj.id,
        key=key_obj.key,   # показываем один раз при создании
        label=key_obj.label,
        owner_email=key_obj.owner_email,
        tier=key_obj.tier,
        is_active=key_obj.is_active,
        is_admin=key_obj.is_admin,
        daily_limit=key_obj.daily_limit,
        created_at=key_obj.created_at,
        expires_at=key_obj.expires_at,
    )


@router.get(
    "/me",
    response_model=ApiKeyInfo,
    summary="My key info + daily usage",
)
def get_my_key(
    key: ApiKey = Depends(get_current_key),
    db: Session = Depends(get_db),
):
    used = _requests_today(db, key.id) if key.id != 0 else 0
    remaining = max(0, key.daily_limit - used) if key.tier != "enterprise" else 999_999
    return ApiKeyInfo(
        id=key.id,
        key_masked=mask_key(key.key),
        label=key.label,
        tier=key.tier,
        is_active=key.is_active,
        daily_limit=key.daily_limit,
        requests_today=used,
        remaining_today=remaining,
        created_at=key.created_at,
        expires_at=key.expires_at,
    )


@router.get(
    "/me/usage",
    response_model=UsageStatsResponse,
    summary="My usage history",
)
def get_my_usage(
    limit: int = Query(default=50, ge=1, le=200),
    key: ApiKey = Depends(get_current_key),
    db: Session = Depends(get_db),
):
    from api.models import UsageLog

    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    used_today = _requests_today(db, key.id) if key.id != 0 else 0
    total = db.query(UsageLog).filter(UsageLog.api_key_id == key.id).count() if key.id != 0 else 0
    recent = (
        db.query(UsageLog)
        .filter(UsageLog.api_key_id == key.id)
        .order_by(UsageLog.created_at.desc())
        .limit(limit)
        .all()
        if key.id != 0
        else []
    )
    remaining = max(0, key.daily_limit - used_today) if key.tier != "enterprise" else 999_999

    return UsageStatsResponse(
        requests_today=used_today,
        requests_total=total,
        daily_limit=key.daily_limit,
        remaining_today=remaining,
        recent=[UsageEntry.model_validate(r) for r in recent],
    )


@router.get(
    "",
    response_model=list[ApiKeyInfo],
    summary="List all API keys (admin only)",
)
def list_keys(
    active_only: bool = Query(default=True),
    admin: ApiKey = Depends(require_admin),
    db: Session = Depends(get_db),
):
    query = db.query(ApiKey)
    if active_only:
        query = query.filter(ApiKey.is_active == True)  # noqa: E712
    keys = query.order_by(ApiKey.created_at.desc()).all()
    result = []
    for k in keys:
        used = _requests_today(db, k.id)
        remaining = max(0, k.daily_limit - used) if k.tier != "enterprise" else 999_999
        result.append(
            ApiKeyInfo(
                id=k.id,
                key_masked=mask_key(k.key),
                label=k.label,
                tier=k.tier,
                is_active=k.is_active,
                daily_limit=k.daily_limit,
                requests_today=used,
                remaining_today=remaining,
                created_at=k.created_at,
                expires_at=k.expires_at,
            )
        )
    return result


@router.delete(
    "/{key_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Deactivate API key (admin only)",
)
def deactivate_key(
    key_id: int,
    admin: ApiKey = Depends(require_admin),
    db: Session = Depends(get_db),
):
    key_obj = db.query(ApiKey).filter(ApiKey.id == key_id).first()
    if not key_obj:
        raise HTTPException(status_code=404, detail="API key not found.")
    key_obj.is_active = False
    db.commit()
