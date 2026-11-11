#!/usr/bin/env bash
# [NGƯỜI A] Demo lỗi: tắt đột ngột một peer giữa lúc đang có người tải.
# Dùng: ./scripts/fault_kill_peer.sh peer1 [số giây chờ trước khi kill]
set -euo pipefail
PEER=${1:?tên service, ví dụ peer1}
DELAY=${2:-3}
COMPOSE="docker compose -f deploy/docker-compose.yml --env-file .env"
sleep "$DELAY"
echo "[$(date -u +%FT%T)] KILL $PEER"
$COMPOSE kill -s SIGKILL "$PEER"
