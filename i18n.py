TEXTS = {
    "ru": {
        "unauthorized": "⛔ Доступ ограничен.",
        "menu_intro": "Vanguard Bot запущен. Выберите модуль:",
        "help": "Команды:\n/start /help /reset /stats /lang\n",
    },
    "en": {
        "unauthorized": "⛔ Access denied.",
        "menu_intro": "Vanguard Bot is running. Choose a module:",
        "help": "Commands:\n/start /help /reset /stats /lang\n",
    },
}


def t(lang: str, key: str, default: str = "") -> str:
    lang_key = (lang or "ru").lower()
    return TEXTS.get(lang_key, TEXTS["ru"]).get(key, default)
