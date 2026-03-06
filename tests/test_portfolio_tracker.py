"""tests/test_portfolio_tracker.py — unit tests for portfolio_tracker module."""
import json
import os
import pytest
from portfolio_tracker import (
    trade_add, trade_close, trade_close_with_pnl,
    trade_list, portfolio_summary,
    _load, _save, _portfolio_path,
)


@pytest.fixture(autouse=True)
def isolated_portfolio(tmp_path, monkeypatch):
    """Redirect data dir to tmp_path so tests don't touch real files."""
    import portfolio_tracker as pt
    monkeypatch.setattr(pt, "_DATA_DIR", str(tmp_path))
    yield


USER = 999


# ── trade_add ────────────────────────────────────────────────────────────────

def test_trade_add_long():
    msg = trade_add(USER, "AAPL", "buy", 185.5, 5)
    trades = _load(USER)
    assert len(trades) == 1
    assert trades[0]["ticker"] == "AAPL"
    assert trades[0]["direction"] == "buy"
    assert trades[0]["entry_price"] == 185.5
    assert trades[0]["qty"] == 5
    assert "✅" in msg


def test_trade_add_short():
    msg = trade_add(USER, "BTC-USD", "sell", 45000.0, 0.5)
    trades = _load(USER)
    assert trades[0]["direction"] == "sell"
    assert "ШОРТ" in msg


def test_trade_add_invalid_direction():
    msg = trade_add(USER, "AAPL", "hold", 100, 1)
    assert "❌" in msg
    assert _load(USER) == []


def test_trade_add_zero_price():
    msg = trade_add(USER, "AAPL", "buy", 0, 1)
    assert "❌" in msg


def test_trade_add_negative_qty():
    msg = trade_add(USER, "AAPL", "buy", 100, -1)
    assert "❌" in msg


def test_trade_add_multiple_ids_are_sequential():
    trade_add(USER, "AAPL", "buy", 100, 1)
    trade_add(USER, "MSFT", "sell", 200, 2)
    trades = _load(USER)
    assert trades[0]["id"] == 1
    assert trades[1]["id"] == 2


# ── trade_close ──────────────────────────────────────────────────────────────

def test_trade_close_removes_trade():
    trade_add(USER, "AAPL", "buy", 185.5, 5)
    msg = trade_close(USER, 1)
    assert "✅" in msg
    assert _load(USER) == []


def test_trade_close_not_found():
    msg = trade_close(USER, 99)
    assert "❌" in msg


# ── trade_close_with_pnl ─────────────────────────────────────────────────────

def test_trade_close_with_pnl_long_profit():
    trade_add(USER, "AAPL", "buy", 100.0, 10)
    msg, trade = trade_close_with_pnl(USER, 1, 110.0)
    assert trade is not None
    assert "ПРИБЫЛЬ" in msg
    assert "+100.0" in msg or "+100" in msg
    assert _load(USER) == []


def test_trade_close_with_pnl_long_loss():
    trade_add(USER, "AAPL", "buy", 100.0, 10)
    msg, trade = trade_close_with_pnl(USER, 1, 90.0)
    assert "УБЫТОК" in msg
    assert trade["ticker"] == "AAPL"


def test_trade_close_with_pnl_short_profit():
    trade_add(USER, "BTC-USD", "sell", 50000.0, 1)
    msg, trade = trade_close_with_pnl(USER, 1, 45000.0)
    assert "ПРИБЫЛЬ" in msg
    assert "+5000.0" in msg or "5000" in msg


def test_trade_close_with_pnl_short_loss():
    trade_add(USER, "BTC-USD", "sell", 45000.0, 1)
    msg, trade = trade_close_with_pnl(USER, 1, 50000.0)
    assert "УБЫТОК" in msg


def test_trade_close_with_pnl_not_found():
    msg, trade = trade_close_with_pnl(USER, 99, 100.0)
    assert "❌" in msg
    assert trade is None


def test_trade_close_with_pnl_saves_to_history(tmp_path, monkeypatch):
    import portfolio_tracker as pt
    monkeypatch.setattr(pt, "_DATA_DIR", str(tmp_path))
    trade_add(USER, "AAPL", "buy", 100.0, 5)
    trade_close_with_pnl(USER, 1, 110.0)
    closed_path = os.path.join(str(tmp_path), f"portfolio_closed_{USER}.json")
    assert os.path.exists(closed_path)
    with open(closed_path) as f:
        history = json.load(f)
    assert len(history) == 1
    assert history[0]["close_price"] == 110.0
    assert history[0]["pnl_usd"] == 50.0


# ── trade_list ────────────────────────────────────────────────────────────────

def test_trade_list_empty():
    msg = trade_list(USER)
    assert "Нет открытых" in msg


def test_trade_list_shows_trades():
    trade_add(USER, "AAPL", "buy", 185.5, 5)
    trade_add(USER, "ETH-USD", "sell", 3000.0, 0.1)
    msg = trade_list(USER)
    assert "AAPL" in msg
    assert "ETH-USD" in msg
    assert "ЛОНГ" in msg
    assert "ШОРТ" in msg


# ── portfolio_summary ─────────────────────────────────────────────────────────

def test_portfolio_summary_empty():
    msg = portfolio_summary(USER, {})
    assert "пуст" in msg.lower()


def test_portfolio_summary_shows_pnl():
    trade_add(USER, "AAPL", "buy", 100.0, 10)
    msg = portfolio_summary(USER, {"AAPL": 110.0})
    assert "AAPL" in msg
    # Should show +$100 profit
    assert "100" in msg


def test_portfolio_summary_unknown_price():
    trade_add(USER, "AAPL", "buy", 100.0, 10)
    msg = portfolio_summary(USER, {})  # no price provided
    assert "AAPL" in msg
