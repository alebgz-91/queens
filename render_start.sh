set -euo pipefail

# download demo DB
mkdir -p ./data
curl -L -o ./data/queens_demo.db



#!/usr/bin/env bash
set -Eeuo pipefail

echo "[startup] Python: $(python -V)"
echo "[startup] CWD: $PWD"
pip show queens || true

# Local dirs
mkdir -p ./data ./exports

# Demo DB location (adjust to your release asset URL if different)
DB_PATH="./data/queens_demo.db"
DB_URL="https://github.com/alebgz-91/queens/releases/download/demo-db-2025-08/queens_demo_2025_8.db"

if [ ! -f "$DB_PATH" ]; then
  echo "[startup] Downloading demo DB..."
  curl -fL -o "$DB_PATH" "$DB_URL"
fi

echo "[startup] Configuring queens paths..."
queens config --db-path "$DB_PATH"

echo "[startup] Launching API..."
exec queens serve --host 0.0.0.0 --port "${PORT:-8000}" --log-level debug
