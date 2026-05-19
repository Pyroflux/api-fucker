#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VPS_FILE="${VPS_FILE:-$ROOT_DIR/vps.txt}"
SSH_OPTS="${SSH_OPTS:-}"
SERVICE_NAME="${SERVICE_NAME:-api-fucker-proxy}"

if [[ ! -f "$VPS_FILE" ]]; then
  echo "vps file not found: $VPS_FILE" >&2
  exit 1
fi

while IFS= read -r host; do
  [[ -z "$host" || "$host" =~ ^[[:space:]]*# ]] && continue
  host="${host%%#*}"
  host="$(echo "$host" | xargs)"
  [[ -z "$host" ]] && continue

  echo "==> $host"
  # shellcheck disable=SC2086
  ssh $SSH_OPTS "$host" "systemctl is-active $SERVICE_NAME; journalctl -u $SERVICE_NAME -n 8 --no-pager"
done < "$VPS_FILE"
