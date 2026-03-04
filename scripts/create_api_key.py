#!/usr/bin/env python3
"""CLI-скрипт для создания API-ключей напрямую в БД (без HTTP-запросов).

Использование:
    # Первый запуск — создать мастер-ключ admin
    python scripts/create_api_key.py --label "admin" --email owner@example.com --tier enterprise --admin

    # Создать демо-ключ
    python scripts/create_api_key.py --label "demo" --email demo@example.com --tier free

    # Посмотреть все ключи
    python scripts/create_api_key.py --list

    # Деактивировать ключ по ID
    python scripts/create_api_key.py --revoke 3
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Позволяет запускать из корня проекта без установки пакета
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.auth import generate_api_key, mask_key
from api.database import SessionLocal, init_db
from api.models import ApiKey, Tier


def create_key(
    label: str,
    email: str | None,
    tier: Tier,
    is_admin: bool = False,
) -> tuple[ApiKey, str]:
    """Создаёт ключ в БД, возвращает (объект, raw_key)."""
    raw = generate_api_key()
    obj = ApiKey(
        key=raw,
        label=label,
        email=email,
        tier=tier,
        is_admin=is_admin,
        is_active=True,
    )
    with SessionLocal() as db:
        db.add(obj)
        db.commit()
        db.refresh(obj)
    return obj, raw


def list_keys() -> None:
    with SessionLocal() as db:
        keys = db.query(ApiKey).order_by(ApiKey.id).all()
    if not keys:
        print("(нет ключей)")
        return
    header = f"{'ID':>4}  {'Label':<20}  {'Tier':<12}  {'Admin':<6}  {'Active':<7}  {'Key (masked)'}"
    print(header)
    print("-" * len(header))
    for k in keys:
        print(
            f"{k.id:>4}  {k.label:<20}  {k.tier.value:<12}  "
            f"{'yes' if k.is_admin else 'no':<6}  "
            f"{'yes' if k.is_active else 'no':<7}  "
            f"{mask_key(k.key)}"
        )


def revoke_key(key_id: int) -> None:
    with SessionLocal() as db:
        key = db.query(ApiKey).filter(ApiKey.id == key_id).first()
        if not key:
            print(f"❌ Ключ с ID={key_id} не найден")
            return
        key.is_active = False
        db.commit()
        print(f"✅ Ключ #{key_id} ({key.label}) деактивирован")


def main() -> None:
    parser = argparse.ArgumentParser(description="Управление API-ключами Vanguard Bot")
    parser.add_argument("--label",  help="Метка ключа (имя клиента/сервиса)")
    parser.add_argument("--email",  help="Email владельца ключа")
    parser.add_argument("--tier",   choices=[t.value for t in Tier], default="free", help="Тир доступа")
    parser.add_argument("--admin",  action="store_true", help="Дать права администратора (создание ключей)")
    parser.add_argument("--list",   action="store_true", help="Показать все ключи")
    parser.add_argument("--revoke", type=int, metavar="ID", help="Деактивировать ключ по ID")
    args = parser.parse_args()

    init_db()

    if args.list:
        list_keys()
        return

    if args.revoke is not None:
        revoke_key(args.revoke)
        return

    if not args.label:
        parser.error("Укажи --label для создания нового ключа, или --list / --revoke")

    tier = Tier(args.tier)
    obj, raw = create_key(
        label=args.label,
        email=args.email,
        tier=tier,
        is_admin=args.admin,
    )

    print(f"\n✅ API-ключ создан:")
    print(f"   ID    : {obj.id}")
    print(f"   Label : {obj.label}")
    print(f"   Email : {obj.email or '-'}")
    print(f"   Tier  : {obj.tier.value}")
    print(f"   Admin : {'да' if obj.is_admin else 'нет'}")
    print(f"\n   🔑 KEY  : {raw}")
    print(f"\n   ⚠️  Сохрани этот ключ — он отображается ОДИН РАЗ!\n")


if __name__ == "__main__":
    main()
