import os
from dataclasses import dataclass
from pathlib import Path

try:
    import yaml
except Exception:
    yaml = None


@dataclass(frozen=True)
class Settings:
    request_timeout_sec: int = 45
    tickers_per_page: int = 10
    market_cache_ttl_sec: int = 90
    news_cache_ttl_sec: int = 120
    ai_timeout_sec: int = 35
    log_file: str = "vanguard_bot.log"
    sentry_dsn: str = ""
    cache_backend: str = "diskcache"
    cache_dir: str = ".cache"
    redis_url: str = ""
    auth_whitelist: tuple[int, ...] = ()
    default_lang: str = "ru"
    use_webhook: bool = False
    webhook_url: str = ""
    webhook_path: str = "/telegram"
    webhook_port: int = 8080


def _to_int(value, default: int) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _parse_whitelist(raw: str) -> tuple[int, ...]:
    if not raw:
        return ()
    values = []
    for chunk in raw.split(","):
        token = chunk.strip()
        if not token:
            continue
        try:
            values.append(int(token))
        except ValueError:
            continue
    return tuple(values)


def load_settings(config_path: str = "config.yaml") -> Settings:
    base = {}
    cfg_file = Path(config_path)
    if cfg_file.exists() and yaml is not None:
        try:
            parsed = yaml.safe_load(cfg_file.read_text(encoding="utf-8")) or {}
            if isinstance(parsed, dict):
                base = parsed
        except Exception:
            base = {}

    def pick(name: str, default):
        env_name = name.upper()
        if env_name in os.environ:
            return os.getenv(env_name)
        return base.get(name, default)

    request_timeout_sec = _to_int(pick("request_timeout_sec", 45), 45)
    tickers_per_page = _to_int(pick("tickers_per_page", 10), 10)
    market_cache_ttl_sec = _to_int(pick("market_cache_ttl_sec", 90), 90)
    news_cache_ttl_sec = _to_int(pick("news_cache_ttl_sec", 120), 120)
    ai_timeout_sec = _to_int(pick("ai_timeout_sec", 35), 35)
    log_file = str(pick("log_file", "vanguard_bot.log"))
    sentry_dsn = str(pick("sentry_dsn", ""))
    cache_backend = str(pick("cache_backend", "diskcache")).lower()
    cache_dir = str(pick("cache_dir", ".cache"))
    redis_url = str(pick("redis_url", ""))
    default_lang = str(pick("default_lang", "ru")).lower()
    use_webhook = str(pick("use_webhook", "false")).lower() in {"1", "true", "yes", "on"}
    webhook_url = str(pick("webhook_url", ""))
    webhook_path = str(pick("webhook_path", "/telegram"))
    webhook_port = _to_int(pick("webhook_port", 8080), 8080)
    auth_whitelist = _parse_whitelist(str(pick("auth_whitelist", "")))

    return Settings(
        request_timeout_sec=request_timeout_sec,
        tickers_per_page=tickers_per_page,
        market_cache_ttl_sec=market_cache_ttl_sec,
        news_cache_ttl_sec=news_cache_ttl_sec,
        ai_timeout_sec=ai_timeout_sec,
        log_file=log_file,
        sentry_dsn=sentry_dsn,
        cache_backend=cache_backend,
        cache_dir=cache_dir,
        redis_url=redis_url,
        auth_whitelist=auth_whitelist,
        default_lang=default_lang,
        use_webhook=use_webhook,
        webhook_url=webhook_url,
        webhook_path=webhook_path,
        webhook_port=webhook_port,
    )


SETTINGS = load_settings()
