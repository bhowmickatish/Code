#!/usr/bin/env bash
set -euo pipefail
export PATH="/usr/local/go/bin:${PATH}"
DOCKER="/workspace/.cursor/cloud-agent-docker.sh"

free_port() {
  local port=$1
  if command -v fuser >/dev/null 2>&1; then
    fuser -k "${port}/tcp" 2>/dev/null || true
  fi
  sleep 1
}

echo "=== go-zookeeper e2e ==="
free_port 8080
cd /workspace/go-zookeeper
$DOCKER compose up -d
sleep 6
go run . >/tmp/go-zookeeper.log 2>&1 &
APP_PID=$!
trap 'kill $APP_PID 2>/dev/null || true' EXIT
for _ in $(seq 1 20); do curl -sf http://localhost:8080/health >/dev/null && break; sleep 1; done
echo "health:" $(curl -s http://localhost:8080/health)
echo "api/users status:" $(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/api/users)
kill $APP_PID 2>/dev/null || true
trap - EXIT
free_port 8080
free_port 4317

echo
echo "=== go-otlp-ingest e2e ==="
cd /workspace/go-otlp-ingest
$DOCKER compose up -d
sleep 5
free_port 8080
free_port 4317
go run ./cmd/ingest >/tmp/go-otlp-ingest.log 2>&1 &
INGEST_PID=$!
trap 'kill $INGEST_PID 2>/dev/null || true' EXIT
for _ in $(seq 1 20); do curl -sf http://localhost:8080/health >/dev/null && break; sleep 1; done
echo "health:" $(curl -s http://localhost:8080/health)
go run ./cmd/client
sleep 2
echo "gauge rows:" $($DOCKER compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM otel.otel_metrics_gauge")
kill $INGEST_PID 2>/dev/null || true

echo
echo "=== all module tests (quick) ==="
for mod in go-cache-aside go-kafka go-otlp-ingest go-zookeeper gdoc-grid; do
  echo "-- $mod --"
  (cd "/workspace/$mod" && go test ./... 2>&1 | tail -3)
done

echo
echo "E2E_OK"
