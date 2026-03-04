"""Database setup: SQLAlchemy + SQLite (dev) / PostgreSQL (prod via DATABASE_URL)."""
from __future__ import annotations

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

DATABASE_URL = os.getenv("API_DATABASE_URL", "sqlite:///./vanguard_api.db")

# SQLite: один поток
connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}

engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    """Создать таблицы если не существуют."""
    from api.models import ApiKey, UsageLog  # noqa: F401 — импорт нужен для создания таблиц
    Base.metadata.create_all(bind=engine)
