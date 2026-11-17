#!/usr/bin/env bash
# Chạy MỘT node (tracker hoặc peer) trên máy này khi triển khai nhiều máy trong LAN (Linux/macOS).
# Bản tương đương của deploy/run_node.ps1. Hướng dẫn đầy đủ: deploy/README.md
#
#   ./deploy/run_node.sh tracker
#   ./deploy/run_node.sh seed  <TRACKER_IP> <PEER_ID> <PORT> <FILE>
#   ./deploy/run_node.sh get   <TRACKER_IP> <PEER_ID> <PORT> <FILE_ID>
#   ./deploy/run_node.sh serve <TRACKER_IP> <PEER_ID> <PORT>
#   ./deploy/run_node.sh list  <TRACKER_IP>
#
# Biến tùy chọn: IP (ép IP của máy này), TRACKER_PORT (mặc định 8000).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="$ROOT/src"
ROLE="${1:?vai trò: tracker | seed | get | serve | list}"
TRACKER_PORT="${TRACKER_PORT:-8000}"

[ -f "$ROOT/.env" ] || { echo "Chưa có $ROOT/.env: chép .env.example thành .env, đặt TRACKER_TOKEN giống nhau trên mọi máy." >&2; exit 1; }

detect_ip() {
    if [ -n "${IP:-}" ]; then echo "$IP"; return; fi
    # IP dùng để đi ra ngoài (Linux), hoặc của card mặc định (macOS)
    ip route get 1.1.1.1 2>/dev/null | awk '{for (i=1;i<=NF;i++) if ($i=="src") {print $(i+1); exit}}' && return
    ipconfig getifaddr "$(route -n get default 2>/dev/null | awk '/interface:/{print $2}')" 2>/dev/null
}
MY_IP="$(detect_ip)"
[ -n "$MY_IP" ] || { echo "Không dò được IP LAN, đặt biến IP=<địa chỉ> rồi chạy lại." >&2; exit 1; }

if [ "$ROLE" = "tracker" ]; then
    echo "Máy này: $MY_IP  ->  các máy khác dùng TRACKER_IP=$MY_IP"
    cd "$SRC"
    TRACKER_HOST=0.0.0.0 TRACKER_PORT="$TRACKER_PORT" exec python3 -m tracker.server
fi

TRACKER_IP="${2:?thiếu TRACKER_IP}"
export TRACKER_URL="http://$TRACKER_IP:$TRACKER_PORT"
echo "Máy này: $MY_IP    Tracker: $TRACKER_URL"

if [ "$ROLE" = "list" ]; then
    cd "$SRC"; PEER_ID="${PEER_ID:-lister}" exec python3 -m peer.main list
fi

export PEER_ID="${3:?thiếu PEER_ID (ví dụ peer-2)}"
export PEER_PORT="${4:?thiếu PORT (ví dụ 9001)}"
export PEER_HOST="$MY_IP" BIND_HOST=0.0.0.0

case "$ROLE" in
    seed)  FILE="$(cd "$(dirname "${5:?thiếu FILE}")" && pwd)/$(basename "$5")"; cd "$SRC"; exec python3 -m peer.main seed "$FILE" ;;
    get)   cd "$SRC"; exec python3 -m peer.main get "${5:?thiếu FILE_ID}" ;;
    serve) cd "$SRC"; exec python3 -m peer.main serve ;;
    *)     echo "vai trò không hợp lệ: $ROLE" >&2; exit 1 ;;
esac
