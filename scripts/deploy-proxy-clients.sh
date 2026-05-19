#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_URL="${SERVER_URL:?SERVER_URL is required, example: http://1.2.3.4:8081}"
PROXY_NODE_TOKEN="${PROXY_NODE_TOKEN:?PROXY_NODE_TOKEN is required}"
VPS_FILE="${VPS_FILE:-$ROOT_DIR/vps.txt}"
BIN="${BIN:-$ROOT_DIR/dist/proxy-client-linux-amd64}"
LISTEN_ADDR="${LISTEN_ADDR:-:9070}"
MAX_CONCURRENCY="${MAX_CONCURRENCY:-100}"
SSH_OPTS="${SSH_OPTS:-}"
SCP_OPTS="${SCP_OPTS:-}"
REMOTE_BIN="/usr/local/bin/api-fucker-proxy"
SERVICE_NAME="api-fucker-proxy"

if [[ ! -f "$BIN" ]]; then
  echo "binary not found: $BIN" >&2
  echo "run: ./scripts/build-proxy-client.sh" >&2
  exit 1
fi

if [[ ! -f "$VPS_FILE" ]]; then
  echo "vps file not found: $VPS_FILE" >&2
  exit 1
fi

while IFS= read -r host; do
  [[ -z "$host" || "$host" =~ ^[[:space:]]*# ]] && continue
  host="${host%%#*}"
  host="$(echo "$host" | xargs)"
  [[ -z "$host" ]] && continue

  echo "==> deploying to $host"
  # shellcheck disable=SC2086
  scp $SCP_OPTS "$BIN" "$host:/tmp/api-fucker-proxy"

  # shellcheck disable=SC2086
  ssh $SSH_OPTS "$host" \
    "SERVER_URL='$SERVER_URL' PROXY_NODE_TOKEN='$PROXY_NODE_TOKEN' LISTEN_ADDR='$LISTEN_ADDR' MAX_CONCURRENCY='$MAX_CONCURRENCY' REMOTE_BIN='$REMOTE_BIN' SERVICE_NAME='$SERVICE_NAME' bash -s" <<'REMOTE'
set -euo pipefail

install -m 0755 /tmp/api-fucker-proxy "$REMOTE_BIN"

mkdir -p /etc/api-fucker-proxy
cat > /etc/api-fucker-proxy/proxy.env <<EOF
SERVER_URL=${SERVER_URL}
PROXY_NODE_TOKEN=${PROXY_NODE_TOKEN}
LISTEN_ADDR=${LISTEN_ADDR}
MAX_CONCURRENCY=${MAX_CONCURRENCY}
EOF
chmod 600 /etc/api-fucker-proxy/proxy.env

cat > "/etc/systemd/system/${SERVICE_NAME}.service" <<EOF
[Unit]
Description=api-fucker VPS Proxy Client
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
EnvironmentFile=/etc/api-fucker-proxy/proxy.env
Environment=NODE_ID_FILE=/var/lib/api-fucker-proxy/node-id
ExecStart=${REMOTE_BIN}
Restart=always
RestartSec=3
User=root
StateDirectory=api-fucker-proxy

NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=true
ReadWritePaths=/var/lib/api-fucker-proxy

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"

if command -v ufw >/dev/null 2>&1; then
  ufw allow 9070/tcp >/dev/null || true
fi

systemctl --no-pager --full status "$SERVICE_NAME" | sed -n '1,12p'
REMOTE

  echo "==> done $host"
done < "$VPS_FILE"
