# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.4.0] - 2026-03-04

### Added
- SaaS API слой (FastAPI) — `api/` — публичный REST-интерфейс для монетизации
- Tier-based rate limiting: `free` (10 req/day) / `pro` (200/day) / `enterprise` (∞)
- `api/models.py`: `ApiKey`, `UsageLog`, `Tier` enum (SQLAlchemy + SQLite/PostgreSQL)
- `api/auth.py`: аутентификация по `X-API-Key`, `require_tier()`, `require_admin()`
- `api/rate_limiter.py`: подсчёт дневных запросов и логирование в `usage_logs`
- `api/routes/analyze.py`: `GET /api/v1/analyze/forecast/{ticker}` (все тиры), `GET /api/v1/analyze/{ticker}` (pro/enterprise)
- `api/routes/keys.py`: CRUD управление ключами (admin)
- `api/routes/webhook.py`: Stripe HMAC-SHA256 webhook для автоматической активации подписок
- `scripts/create_api_key.py`: CLI-инструмент для bootstrap первого admin-ключа
- `docker-compose.prod.yml`: сервис `api` на порту 8090

### Changed
- `api/main.py`: переведён с deprecated `@app.on_event` на `lifespan` context manager
- `requirements.txt`: добавлены `fastapi`, `uvicorn`, `sqlalchemy`, `pydantic-settings`, `stripe`

---

## [1.3.0] - 2026-03-01

### Added
- Runtime Watchdog (`scripts/runtime_watchdog.py`): проверяет webhook и DeepSeek, шлёт алерты в Telegram-чат администратора
- `WATCHDOG_INTERVAL_SEC` и `ADMIN_CHAT_ID` в конфигурацию
- Docker healthcheck через `scripts/healthcheck.py`
- `DEEPSEEK_STRICT` режим: не уходит на fallback-провайдер если явно выбран DeepSeek

### Changed
- Production Docker-образ переведён на `requirements.runtime.txt` (без `torch`, `pytest`)
- Ускорена сборка образа за счёт разделения dev / runtime зависимостей

---

## [1.2.0] - 2026-02-15

### Added
- Мультиязычность (ru/en) через `/lang` команду и `i18n.py`
- Пагинация списка тикеров (`tickers_per_page`)
- `/alert add`, `/alert del`, `/alert list` — алерты на изменение цены
- Backtesting модуль (`backtesting.py`) для оценки точности rule-based модели
- Трекинг прогнозов и команда `/stats`

### Changed
- AI fallback chain: DeepSeek → Groq (Llama-3) → Anthropic Claude
- `AI_TOTAL_BUDGET_SEC` ограничивает суммарное время всей цепочки

---

## [1.1.0] - 2026-01-20

### Added
- Intermarket-корреляции тикера с `SPY` и `BTC-USD`
- Sentiment-анализ новостей (transformers + словарный fallback)
- Кэш-бэкенды: `memory` / `diskcache` / `redis`
- PNG-графики с SMA20/50 и EMA200

### Fixed
- Блокировка повторных инстанций через `/tmp/vanguard_bot.lock`
- Корректная обработка таймаутов yfinance при нестабильном соединении

---

## [1.0.0] - 2026-01-01

### Added
- Первый рабочий релиз
- Технический анализ: SMA, EMA, RSI, ATR, ADX
- Rule-based forecast с quality gate
- Whitelist по Telegram ID
- Polling и Webhook режимы
