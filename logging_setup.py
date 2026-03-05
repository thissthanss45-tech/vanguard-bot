import logging
from logging.handlers import RotatingFileHandler

from utils.trace import get_trace_id


class _TraceFilter(logging.Filter):
    """Добавляет trace_id из contextvars в каждую лог-запись."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = get_trace_id()  # type: ignore[attr-defined]
        return True


def setup_logging(log_file: str, sentry_dsn: str = "") -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    for handler in list(root.handlers):
        root.removeHandler(handler)

    # Формат включает trace_id
    fmt = "%(asctime)s [%(levelname)s] %(name)s | trace=%(trace_id)s | %(message)s"
    formatter = logging.Formatter(fmt)
    trace_filter = _TraceFilter()

    stream = logging.StreamHandler()
    stream.setFormatter(formatter)
    stream.addFilter(trace_filter)
    root.addHandler(stream)

    file_handler = RotatingFileHandler(
        log_file, maxBytes=2_000_000, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.addFilter(trace_filter)
    root.addHandler(file_handler)

    if sentry_dsn:
        try:
            import sentry_sdk

            sentry_sdk.init(dsn=sentry_dsn, traces_sample_rate=0.1)
            logging.getLogger(__name__).info("Sentry initialized")
        except Exception as exc:
            logging.getLogger(__name__).warning("Sentry init failed: %s", exc)

