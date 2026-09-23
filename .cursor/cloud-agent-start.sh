#!/usr/bin/env bash
set -euo pipefail

export PATH="/usr/local/go/bin:${PATH}"

ensure_docker_access() {
  if docker info >/dev/null 2>&1; then
    return 0
  fi
  if getent group docker >/dev/null 2>&1; then
    sudo usermod -aG docker "$(whoami)" 2>/dev/null || true
  fi
  if sg docker -c 'docker info' >/dev/null 2>&1; then
    return 0
  fi
  if [[ -S /var/run/docker.sock ]]; then
    sudo chmod 666 /var/run/docker.sock 2>/dev/null || true
  fi
  docker info >/dev/null 2>&1
}

start_dockerd() {
  if docker info >/dev/null 2>&1; then
    echo "Docker daemon already running."
    return 0
  fi

  if ! pgrep -x dockerd >/dev/null 2>&1; then
    sudo dockerd --iptables=false --storage-driver=vfs >/tmp/dockerd.log 2>&1 &
  fi

  for _ in $(seq 1 60); do
    ensure_docker_access && return 0
    sleep 1
  done

  echo "Docker daemon failed to become ready." >&2
  tail -30 /tmp/dockerd.log >&2 || true
  return 1
}

start_dockerd
echo "Docker is ready."
