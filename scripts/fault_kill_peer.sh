#!/usr/bin/env bash
# Gây lỗi khi chạy bằng Docker: tắt đột ngột một peer giữa lúc đang có người tải.
# Dùng: ./scripts/fault_kill_peer.sh peer1 [số giây chờ trước khi kill]
# (Chạy không cần Docker: python scripts/demo.py failover)
set -euo pipefail
PEER=${1:?tên service, ví dụ peer1}
DELAY=${2:-3}
sleep "$DELAY"
echo "[$(date -u +%FT%T)] KILL $PEER"
docker compose -f deploy/docker-compose.yml --env-file .env kill -s SIGKILL "$PEER"
