import json
import logging
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

from openai import OpenAI


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("runtime_watchdog")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/telegram").strip() or "/telegram"
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "").strip()
DEEPSEEK_MODEL = os.getenv("DEEPSEEK_MODEL", "deepseek-chat").strip() or "deepseek-chat"
WATCHDOG_INTERVAL_SEC = int(os.getenv("WATCHDOG_INTERVAL_SEC", "180"))
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip()


def _telegram_api(method: str, payload: dict | None = None) -> dict:
    if not TELEGRAM_BOT_TOKEN:
        return {"ok": False, "description": "missing TELEGRAM_BOT_TOKEN"}
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    if payload is None:
        with urllib.request.urlopen(url, timeout=20) as r:
            return json.loads(r.read().decode("utf-8"))

    data = urllib.parse.urlencode(payload).encode()
    req = urllib.request.Request(url, data=data)
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode("utf-8"))


def _expected_webhook() -> str:
    if not WEBHOOK_URL:
        return ""
    return f"{WEBHOOK_URL.rstrip('/')}{WEBHOOK_PATH}"


def check_webhook() -> tuple[bool, str]:
    expected = _expected_webhook()
    if not expected:
        return False, "WEBHOOK_URL is empty"

    try:
        info = _telegram_api("getWebhookInfo")
    except Exception as exc:
        return False, f"getWebhookInfo failed: {exc}"

    if not info.get("ok"):
        return False, f"getWebhookInfo not ok: {info}"

    current = (info.get("result") or {}).get("url", "")
    if current != expected:
        return False, f"webhook mismatch: expected={expected}, current={current}"
    return True, "webhook ok"


def check_deepseek() -> tuple[bool, str]:
    if not DEEPSEEK_API_KEY:
        return False, "DEEPSEEK_API_KEY is empty"

    try:
        client = OpenAI(
            api_key=DEEPSEEK_API_KEY,
            base_url="https://api.deepseek.com",
            timeout=18,
            max_retries=0,
        )
        started = time.monotonic()
        response = client.chat.completions.create(
            model=DEEPSEEK_MODEL,
            messages=[
                {"role": "system", "content": "Отвечай кратко."},
                {"role": "user", "content": "Ответь одним словом: OK"},
            ],
            temperature=0,
            max_tokens=8,
        )
        elapsed = time.monotonic() - started
        text = (response.choices[0].message.content or "").strip()
        if not text:
            return False, "deepseek empty response"
        return True, f"deepseek ok ({elapsed:.2f}s)"
    except Exception as exc:
        return False, f"deepseek failed: {exc}"


def send_admin_alert(text: str):
    if not ADMIN_CHAT_ID:
        return
    try:
        _telegram_api(
            "sendMessage",
            {
                "chat_id": ADMIN_CHAT_ID,
                "text": text,
                "disable_web_page_preview": True,
            },
        )
    except Exception as exc:
        logger.warning("send alert failed: %s", exc)


def main():
    last_state = None
    logger.info("runtime_watchdog started; interval=%ss", WATCHDOG_INTERVAL_SEC)

    while True:
        webhook_ok, webhook_msg = check_webhook()
        deepseek_ok, deepseek_msg = check_deepseek()

        state = "OK" if (webhook_ok and deepseek_ok) else "FAIL"
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        msg = f"[{now}] state={state}; webhook={webhook_msg}; deepseek={deepseek_msg}"

        if state == "OK":
            logger.info(msg)
        else:
            logger.error(msg)

        if state != last_state:
            send_admin_alert(f"Vanguard watchdog: {msg}")
            last_state = state

        time.sleep(max(30, WATCHDOG_INTERVAL_SEC))


if __name__ == "__main__":
    main()
