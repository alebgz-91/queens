set -euo pipefail

# download demo DB
mkdir -p ./data
curl -L -o ./data/queens_demo.db "https://github.com/alebgz-91/queens/releases/download/demo-db-2025-08/queens_demo_2025_8.db"

# redirect queens to the demo DB
queens config --db-path "${pwd}/data/queens_demo.db"

# launch API service
queens serve --host 0.0.0.0 --port ${PORT:-8000}