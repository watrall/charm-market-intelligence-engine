#!/bin/sh
set -eu

cmd="${1:-dashboard}"
shift || true

if [ "$cmd" = "dashboard" ]; then
  exec streamlit run dashboard/app.py --server.headless true --server.address 0.0.0.0 --server.port "${PORT:-8501}"
fi

if [ "$cmd" = "pipeline" ]; then
  exec python -u scripts/pipeline.py "$@"
fi

echo "Unknown command: $cmd (expected 'dashboard' or 'pipeline')" >&2
exit 1

