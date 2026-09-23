#!/usr/bin/env bash
set -euo pipefail

export PATH="/usr/local/go/bin:${PATH}"

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
modules=(
  go-cache-aside
  go-kafka
  go-otlp-ingest
  go-zookeeper
  gdoc-grid
)

for mod in "${modules[@]}"; do
  echo "==> ${mod}"
  (
    cd "${repo_root}/${mod}"
    go mod download
    go build ./...
  )
done

echo "Install complete."
