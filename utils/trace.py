"""Structured logging helpers: trace_id via contextvars.

Использование в PTB-хендлерах:
    from utils.trace import get_trace_id, new_trace_id

    # В ConversationHandler/callback:
    logger.info("handler: %s | trace=%s", ticker, get_trace_id())

В API (FastAPI) trace_id устанавливается в RequestContextMiddleware.
"""
from __future__ import annotations

import uuid
from contextvars import ContextVar

_trace_id_var: ContextVar[str] = ContextVar("trace_id", default="-")


def new_trace_id() -> str:
    """Генерирует новый trace_id, сохраняет в contextvar и возвращает его."""
    tid = uuid.uuid4().hex[:16]
    _trace_id_var.set(tid)
    return tid


def set_trace_id(tid: str) -> None:
    """Устанавливает внешний trace_id (например из заголовка X-Request-ID)."""
    _trace_id_var.set(tid[:32])


def get_trace_id() -> str:
    """Возвращает текущий trace_id или '-' если не установлен."""
    return _trace_id_var.get()
