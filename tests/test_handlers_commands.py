"""tests/test_handlers_commands.py — tests for commands and handler utilities."""
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class DummyMessage:
    def __init__(self, text=""):
        self.text = text
        self.replies = []
        self.edits = []

    async def reply_text(self, text, **kwargs):
        self.replies.append(text)
        return self

    async def edit_text(self, text, **kwargs):
        self.edits.append(text)

    async def delete(self):
        pass


class DummyUser:
    def __init__(self, user_id=123):
        self.id = user_id


class DummyChat:
    def __init__(self, chat_id=123):
        self.id = chat_id


class DummyUpdate:
    def __init__(self, text="", user_id=123):
        self.message = DummyMessage(text)
        self.effective_message = self.message
        self.effective_user = DummyUser(user_id)
        self.effective_chat = DummyChat(user_id)


class DummyContext:
    def __init__(self, args=None):
        self.user_data = {}
        self.bot_data = {}
        self.error = None
        self.args = args or []
        self.bot = None


# ── bot_globals shared helpers ────────────────────────────────────────────────

def test_normalize_ticker():
    from bot_globals import _normalize_ticker
    assert _normalize_ticker("  aapl  ") == "AAPL"
    assert _normalize_ticker("btc-usd") == "BTC-USD"


def test_is_back():
    from bot_globals import _is_back
    assert _is_back("↩️ Назад") is True
    assert _is_back("что-то другое") is False


def test_cache_get_miss():
    from bot_globals import _cache_get
    result = _cache_get("test_ns", "nonexistent_key_xyz", 60)
    assert result is None


def test_main_markup_returns_keyboard():
    from bot_globals import _main_markup, ReplyKeyboardMarkup
    ctx = DummyContext()
    markup = _main_markup(ctx)
    assert isinstance(markup, ReplyKeyboardMarkup)


def test_inc_stat_and_get_stats():
    from bot_globals import _inc_stat, _get_stats_text
    ctx = DummyContext()
    _inc_stat(ctx, "cmd_test")
    _inc_stat(ctx, "cmd_test")
    _inc_stat(ctx, "cmd_other")
    text = _get_stats_text(ctx)
    assert "cmd_test" in text
    assert "2" in text


def test_get_alerts_defaults_empty():
    from bot_globals import _get_alerts
    ctx = DummyContext()
    alerts = _get_alerts(ctx)
    assert alerts == {}


def test_format_atr_levels_no_data():
    from bot_globals import _format_atr_levels
    result = _format_atr_levels({})
    assert result == ""


def test_format_atr_levels_bullish():
    from bot_globals import _format_atr_levels
    data = {
        "current_price": 100.0,
        "atr_14": 2.0,
        "rule_forecast": {"bias": "Бычий"},
    }
    result = _format_atr_levels(data, "balanced")
    assert "ЛОНГ" in result
    assert "SL" in result
    assert "TP1" in result


def test_format_atr_levels_bearish():
    from bot_globals import _format_atr_levels
    data = {
        "current_price": 100.0,
        "atr_14": 2.0,
        "rule_forecast": {"bias": "Медвежий"},
    }
    result = _format_atr_levels(data, "conservative")
    assert "ШОРТ" in result


# ── start and help_command ────────────────────────────────────────────────────

def test_start_sends_welcome():
    from bot_globals import start, MENU
    update = DummyUpdate("/start")
    context = DummyContext()
    result = asyncio.run(start(update, context))
    assert result == MENU
    assert len(update.message.replies) == 1
    assert "Vanguard" in update.message.replies[0] or "меню" in update.message.replies[0].lower()


def test_help_command_sends_help():
    from bot_globals import help_command, MENU
    update = DummyUpdate("/help")
    context = DummyContext()
    result = asyncio.run(help_command(update, context))
    assert result == MENU
    assert len(update.message.replies) == 1
    assert "/start" in update.message.replies[0]


def test_cancel_goes_to_menu():
    from bot_globals import cancel, MENU
    update = DummyUpdate("/cancel")
    context = DummyContext()
    result = asyncio.run(cancel(update, context))
    assert result == MENU


def test_reset_session_clears_state():
    from bot_globals import reset_session, MENU
    update = DummyUpdate("/reset")
    context = DummyContext()
    context.user_data["popular_category"] = "🔵 Акции"
    result = asyncio.run(reset_session(update, context))
    assert result == MENU
    assert "popular_category" not in context.user_data


# ── commands handlers ─────────────────────────────────────────────────────────

def test_stats_command():
    from handlers.commands import stats_command
    update = DummyUpdate("/stats")
    ctx = DummyContext()
    ctx.bot_data["usage_stats"] = {"analysis_runs": 3}
    asyncio.run(stats_command(update, ctx))
    assert len(update.message.replies) == 1
    assert "3" in update.message.replies[0]


def test_lang_command_sets_language():
    from handlers.commands import lang_command
    update = DummyUpdate("/lang en")
    context = DummyContext(args=["en"])
    # re-read command parts from message
    asyncio.run(lang_command(update, context))
    assert context.user_data.get("lang") == "en"


def test_lang_command_invalid():
    from handlers.commands import lang_command
    update = DummyUpdate("/lang de")
    context = DummyContext(args=["de"])
    asyncio.run(lang_command(update, context))
    # should show usage, not set language
    assert "lang" not in context.user_data


def test_backtest_command_no_args(monkeypatch):
    from handlers.commands import backtest_command
    import handlers.commands as hc
    monkeypatch.setattr(hc, "build_per_ticker_accuracy", lambda min_forecasts=1: "TABLE")
    update = DummyUpdate("/backtest")
    ctx = DummyContext(args=[])
    asyncio.run(backtest_command(update, ctx))
    assert len(update.message.replies) == 1
    assert "TABLE" in update.message.replies[0]


def test_compare_command_missing_args():
    from handlers.commands import compare_command
    update = DummyUpdate("/compare")
    ctx = DummyContext(args=[])
    asyncio.run(compare_command(update, ctx))
    assert len(update.message.replies) == 1
    assert "compare" in update.message.replies[0].lower() or "Использование" in update.message.replies[0]


def test_alert_add_and_list():
    from handlers.commands import alert_command
    ctx = DummyContext()

    add_update = DummyUpdate("/alert add BTC-USD price_above 90000")
    asyncio.run(alert_command(add_update, ctx))
    assert "BTC-USD" in ctx.user_data.get("alerts", {})

    list_update = DummyUpdate("/alert list")
    asyncio.run(alert_command(list_update, ctx))
    assert any("BTC-USD" in r for r in list_update.message.replies)


def test_alert_del():
    from handlers.commands import alert_command
    ctx = DummyContext()
    ctx.user_data["alerts"] = {"AAPL": {"price_above": 200}}

    del_update = DummyUpdate("/alert del AAPL")
    asyncio.run(alert_command(del_update, ctx))
    assert "AAPL" not in ctx.user_data.get("alerts", {})
