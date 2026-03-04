import asyncio

import bot


class DummyMessage:
    def __init__(self, text=""):
        self.text = text
        self.replies = []

    async def reply_text(self, text, reply_markup=None):
        self.replies.append({"text": text, "reply_markup": reply_markup})


class DummyUpdate:
    def __init__(self, text="", user_id=123):
        self.message = DummyMessage(text)
        self.effective_message = self.message
        self.effective_user = type("U", (), {"id": user_id})()


class DummyContext:
    def __init__(self):
        self.user_data = {}
        self.bot_data = {}
        self.error = None


def test_lang_command_sets_language():
    update = DummyUpdate("/lang en")
    context = DummyContext()

    asyncio.run(bot.lang_command(update, context))

    assert context.user_data["lang"] == "en"
    assert any("Язык установлен" in item["text"] for item in update.message.replies)


def test_alert_add_and_list():
    context = DummyContext()

    add_update = DummyUpdate("/alert add BTC-USD 30")
    asyncio.run(bot.alert_command(add_update, context))

    list_update = DummyUpdate("/alert list")
    asyncio.run(bot.alert_command(list_update, context))

    assert "BTC-USD" in context.user_data.get("alerts", {})
    assert any("Текущие алерты" in item["text"] for item in list_update.message.replies)


def test_stats_command_outputs_data():
    context = DummyContext()
    context.bot_data["usage_stats"] = {"analysis_runs": 2, "cmd_start": 1}

    update = DummyUpdate("/stats")
    asyncio.run(bot.stats_command(update, context))

    assert any("Статистика" in item["text"] for item in update.message.replies)
