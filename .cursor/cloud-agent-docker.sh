#!/usr/bin/env bash
# Wrapper so compose works whether or not the session has docker group membership.
set -euo pipefail

if docker info >/dev/null 2>&1; then
  exec docker "$@"
fi
if sg docker -c "docker info" >/dev/null 2>&1; then
  exec sg docker -c "docker $(printf '%q ' "$@")"
fi
exec sudo docker "$@"
