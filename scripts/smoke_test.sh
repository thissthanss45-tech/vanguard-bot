#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[smoke] py_compile"
python3 -m py_compile bot.py ai_engine.py data_provider.py news_provider.py

echo "[smoke] pytest"
pytest -q

echo "[smoke] bot command handlers"
pytest -q tests/test_bot_smoke.py

echo "[smoke] done"
