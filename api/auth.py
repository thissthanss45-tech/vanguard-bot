"""API Key аутентификация.

Ключ передаётся через заголовок:  X-API-Key: vgd_<token>
или через query параметр:          ?api_key=vgd_<token>  (удобно для тестов)
"""
from __future__ import annotations

import hashlib
import os
import secrets
from collections.abc import Callable
from datetime import datetime, timezone

from fastapi import Depends, HTTPException, Security, status
from fastapi.security.api_key import APIKeyHeader, APIKeyQuery
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import ApiKey

API_KEY_PREFIX = "vgd_"
_ADMIN_KEY = os.getenv("API_ADMIN_KEY", "")  # мастер-ключ из .env

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
_api_key_query  = APIKeyQuery(name="api_key",    auto_error=False)


def generate_api_key() -> str:
    """Генерируем случайный 40-символьный токен с префиксом vgd_."""
    token = secrets.token_urlsafe(30)
    return f"{API_KEY_PREFIX}{token}"


def mask_key(key: str) -> str:
    """vgd_ABCDEFxxxx → vgd_ABCD***xxxx."""
    if len(key) <= 10:
        return key[:4] + "***"
    return key[:8] + "***" + key[-4:]


def _resolve_key(
    header_key: str | None = Security(_api_key_header),
    query_key:  str | None = Security(_api_key_query),
    db: Session = Depends(get_db),
) -> ApiKey:
    raw_key = header_key or query_key
    if not raw_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key missing. Pass X-API-Key header or ?api_key= query.",
        )

    # Мастер-ключ из .env (для создания ключей / admin)
    if _ADMIN_KEY and raw_key == _ADMIN_KEY:
        # Возвращаем виртуальный admin объект
        fake = ApiKey()
        fake.id = 0
        fake.key = raw_key
        fake.label = "admin"
        fake.owner_email = "admin"
        fake.tier = "enterprise"
        fake.is_active = True
        fake.is_admin = True
        fake.created_at = datetime.now(timezone.utc)
        fake.expires_at = None
        return fake

    key_obj = db.query(ApiKey).filter(ApiKey.key == raw_key, ApiKey.is_active == True).first()
    if not key_obj:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or inactive API key.",
        )

    # Проверяем TTL
    if key_obj.expires_at and key_obj.expires_at < datetime.now(timezone.utc):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key expired.",
        )

    return key_obj


def get_current_key(key: ApiKey = Depends(_resolve_key)) -> ApiKey:
    return key


def require_admin(key: ApiKey = Depends(get_current_key)) -> ApiKey:
    if not key.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required.",
        )
    return key


def require_tier(min_tier: str) -> Callable[..., ApiKey]:
    """Dependency factory: проверяем что tier >= min_tier.
    Порядок тиеров: free < pro < enterprise.
    """
    _order = {"free": 0, "pro": 1, "enterprise": 2}

    def _check(key: ApiKey = Depends(get_current_key)) -> ApiKey:
        if _order.get(key.tier, 0) < _order.get(min_tier, 0):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"This endpoint requires '{min_tier}' tier or higher. "
                       f"Your tier: '{key.tier}'. Upgrade at /pricing.",
            )
        return key

    return _check
