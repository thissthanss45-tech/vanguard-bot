import os
import sys
from pathlib import Path


def _require_env(name: str) -> bool:
    return bool(os.getenv(name, "").strip())


def _writable(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        test_file = path / ".write_test"
        test_file.write_text("ok", encoding="utf-8")
        test_file.unlink(missing_ok=True)
        return True
    except Exception:
        return False


def main() -> int:
    if not _require_env("TELEGRAM_BOT_TOKEN"):
        print("missing TELEGRAM_BOT_TOKEN")
        return 1

    lock_exists = Path("/tmp/vanguard_bot.lock").exists()
    cache_ok = _writable(Path(".cache"))

    if not lock_exists:
        print("lock file not found")
        return 1

    if not cache_ok:
        print("cache directory not writable")
        return 1

    print("ok")
    return 0


if __name__ == "__main__":
    sys.exit(main())
