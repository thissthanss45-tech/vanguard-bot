"""Tests for the SaaS API layer (api/).

Uses an in-memory SQLite DB and mocks data_provider to avoid live network calls.
"""
from __future__ import annotations

import os
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

# ── env must be set before importing app ────────────────────────────────────
os.environ.setdefault("API_ADMIN_KEY", "test-admin-secret")
os.environ.setdefault("API_DATABASE_URL", "sqlite:///:memory:")

from api.database import Base, get_db  # noqa: E402
from api.main import create_app        # noqa: E402

# ── In-memory test DB ────────────────────────────────────────────────────────
_TEST_ENGINE = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,  # All sessions share one connection → same :memory: DB
)
_TestSession = sessionmaker(autocommit=False, autoflush=False, bind=_TEST_ENGINE)


def _override_get_db():
    db = _TestSession()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture(scope="module")
def client():
    Base.metadata.create_all(bind=_TEST_ENGINE)
    app = create_app()
    app.dependency_overrides[get_db] = _override_get_db
    with TestClient(app, raise_server_exceptions=True) as c:
        yield c
    Base.metadata.drop_all(bind=_TEST_ENGINE)


ADMIN_H = {"X-API-Key": "test-admin-secret"}

# ── Helpers ──────────────────────────────────────────────────────────────────

def _create_key(client, tier: str = "free", label: str = "test") -> str:
    r = client.post(
        "/api/v1/keys",
        json={"label": label, "owner_email": f"{label}@test.com", "tier": tier},
        headers=ADMIN_H,
    )
    assert r.status_code == 201, r.text
    return r.json()["key"]


# ════════════════════════════════════════════════════════════════════════════
# Health
# ════════════════════════════════════════════════════════════════════════════

class TestHealth:
    def test_health_root(self, client):
        r = client.get("/health")
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"
        assert data["version"] == "1.4.0"
        assert "timestamp" in data

    def test_health_versioned(self, client):
        r = client.get("/api/v1/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_pricing(self, client):
        r = client.get("/pricing")
        assert r.status_code == 200
        tiers = r.json()["tiers"]
        assert set(tiers.keys()) == {"free", "pro", "enterprise"}


# ════════════════════════════════════════════════════════════════════════════
# Authentication
# ════════════════════════════════════════════════════════════════════════════

class TestAuth:
    def test_missing_key_returns_401(self, client):
        r = client.get("/api/v1/analyze/forecast/AAPL")
        assert r.status_code == 401

    def test_invalid_key_returns_401(self, client):
        r = client.get(
            "/api/v1/analyze/forecast/AAPL",
            headers={"X-API-Key": "vgd_invalid_key_xyz"},
        )
        assert r.status_code == 401

    def test_key_via_query_param(self, client):
        """Ключ можно передать через ?api_key=."""
        r = client.get("/api/v1/keys", params={"api_key": "test-admin-secret"})
        assert r.status_code == 200


# ════════════════════════════════════════════════════════════════════════════
# Key management
# ════════════════════════════════════════════════════════════════════════════

class TestKeyManagement:
    def test_create_key_as_admin(self, client):
        r = client.post(
            "/api/v1/keys",
            json={"label": "ci-free", "owner_email": "ci@test.com", "tier": "free"},
            headers=ADMIN_H,
        )
        assert r.status_code == 201
        data = r.json()
        assert data["key"].startswith("vgd_")
        assert data["tier"] == "free"
        assert data["daily_limit"] == 10

    def test_create_pro_key(self, client):
        r = client.post(
            "/api/v1/keys",
            json={"label": "ci-pro", "owner_email": "pro@test.com", "tier": "pro"},
            headers=ADMIN_H,
        )
        assert r.status_code == 201
        assert r.json()["daily_limit"] == 200

    def test_create_enterprise_key(self, client):
        r = client.post(
            "/api/v1/keys",
            json={"label": "ci-ent", "owner_email": "ent@test.com", "tier": "enterprise"},
            headers=ADMIN_H,
        )
        assert r.status_code == 201
        assert r.json()["daily_limit"] == 999_999

    def test_create_key_non_admin_forbidden(self, client):
        free_key = _create_key(client, tier="free", label="plain-free")
        r = client.post(
            "/api/v1/keys",
            json={"label": "x", "owner_email": "x@test.com", "tier": "free"},
            headers={"X-API-Key": free_key},
        )
        assert r.status_code == 403

    def test_list_keys_as_admin(self, client):
        r = client.get("/api/v1/keys", headers=ADMIN_H)
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    def test_list_keys_non_admin_forbidden(self, client):
        key = _create_key(client, tier="free", label="list-check")
        r = client.get("/api/v1/keys", headers={"X-API-Key": key})
        assert r.status_code == 403

    def test_get_me(self, client):
        key = _create_key(client, tier="pro", label="me-check")
        r = client.get("/api/v1/keys/me", headers={"X-API-Key": key})
        assert r.status_code == 200
        data = r.json()
        assert data["tier"] == "pro"
        assert data["daily_limit"] == 200
        assert "key_masked" in data

    def test_get_me_usage(self, client):
        key = _create_key(client, tier="free", label="usage-check")
        r = client.get("/api/v1/keys/me/usage", headers={"X-API-Key": key})
        assert r.status_code == 200
        data = r.json()
        assert "requests_today" in data
        assert "remaining_today" in data

    def test_deactivate_key(self, client):
        key = _create_key(client, tier="free", label="to-delete")
        # получить id ключа
        me = client.get("/api/v1/keys/me", headers={"X-API-Key": key})
        key_id = me.json()["id"]

        r = client.delete(f"/api/v1/keys/{key_id}", headers=ADMIN_H)
        assert r.status_code == 204

        # теперь ключ не должен работать
        r2 = client.get("/api/v1/keys/me", headers={"X-API-Key": key})
        assert r2.status_code == 401

    def test_invalid_tier_rejected(self, client):
        r = client.post(
            "/api/v1/keys",
            json={"label": "bad", "owner_email": "x@x.com", "tier": "platinum"},
            headers=ADMIN_H,
        )
        assert r.status_code == 422


# ════════════════════════════════════════════════════════════════════════════
# Forecast endpoint (mocked data_provider)
# ════════════════════════════════════════════════════════════════════════════

_MOCK_MARKET = {
    "ticker": "AAPL",
    "instrument_name": "Apple Inc.",
    "current_price": 170.0,
    "change_pct_1d": 0.5,
    "sma_20": 168.0, "sma_50": 165.0, "ema_200": 160.0,
    "rsi_14": 55.0, "atr_14": 2.5, "adx_14": 28.0,
    "annualized_volatility_pct": 22.5,
    "data_lag_human": "real-time",
    "error": None,
}

_MOCK_RULE = {
    "bias": "bullish", "bull": 65.0, "bear": 35.0,
    "action": "buy", "confidence": "medium",
    "regime": "trend", "trade_allowed": True, "gate_reason": "",
}


def _patch_dp():
    # get_market_data imported lazily inside _get_forecast_data → patch at source
    return patch("data_provider.get_market_data", return_value=_MOCK_MARKET)


def _patch_rule():
    return patch("data_provider.build_rule_based_forecast", return_value=_MOCK_RULE)


class TestForecastEndpoint:
    def test_forecast_free_key(self, client):
        key = _create_key(client, tier="free", label="forecast-free")
        with _patch_dp(), _patch_rule():
            r = client.get(
                "/api/v1/analyze/forecast/AAPL",
                headers={"X-API-Key": key},
            )
        assert r.status_code == 200
        data = r.json()
        assert data["ticker"] == "AAPL"
        assert data["forecast_bias"] == "bullish"
        assert data["forecast_bull_pct"] == 65.0
        assert "latency_ms" in data

    def test_forecast_without_auth_fails(self, client):
        r = client.get("/api/v1/analyze/forecast/AAPL")
        assert r.status_code == 401

    def test_forecast_unknown_ticker_404(self, client):
        key = _create_key(client, tier="free", label="forecast-404")
        with patch("data_provider.get_market_data", return_value={"error": "not found"}):
            r = client.get(
                "/api/v1/analyze/forecast/ZZZZZZ",
                headers={"X-API-Key": key},
            )
        assert r.status_code == 404

    def test_analyze_requires_pro_tier(self, client):
        free_key = _create_key(client, tier="free", label="ai-free-block")
        with _patch_dp(), _patch_rule():
            r = client.get(
                "/api/v1/analyze/AAPL",
                headers={"X-API-Key": free_key},
            )
        assert r.status_code == 403

    def test_analyze_pro_key(self, client):
        pro_key = _create_key(client, tier="pro", label="ai-pro-ok")
        mock_ai = "AI: bullish momentum, strong technicals."
        with _patch_dp(), _patch_rule(), \
             patch("ai_engine.get_ai_prediction", return_value=mock_ai):
            r = client.get(
                "/api/v1/analyze/AAPL",
                headers={"X-API-Key": pro_key},
            )
        assert r.status_code == 200
        data = r.json()
        assert data["ai_analysis"] == mock_ai
        assert data["ticker"] == "AAPL"


# ════════════════════════════════════════════════════════════════════════════
# Rate limiting
# ════════════════════════════════════════════════════════════════════════════

class TestRateLimiting:
    def test_rate_limit_exceeded_returns_429(self, client):
        """Создаём ключ с free tier и симулируем превышение лимита."""
        key = _create_key(client, tier="free", label="rate-limit-test")

        # Выбираем id ключа
        me = client.get("/api/v1/keys/me", headers={"X-API-Key": key})
        key_id = me.json()["id"]

        # Патчим _requests_today чтобы вернуть уже достигнутый лимит (10)
        with patch("api.rate_limiter._requests_today", return_value=10), \
             _patch_dp(), _patch_rule():
            r = client.get(
                "/api/v1/analyze/forecast/AAPL",
                headers={"X-API-Key": key},
            )
        assert r.status_code == 429
        assert "Daily limit reached" in r.json()["error"]

    def test_enterprise_bypasses_rate_limit(self, client):
        ent_key = _create_key(client, tier="enterprise", label="ent-no-limit")
        with patch("api.rate_limiter._requests_today", return_value=99999), \
             _patch_dp(), _patch_rule():
            r = client.get(
                "/api/v1/analyze/forecast/AAPL",
                headers={"X-API-Key": ent_key},
            )
        assert r.status_code == 200
