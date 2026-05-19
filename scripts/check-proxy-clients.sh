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

checked=0
while IFS= read -r host || [[ -n "$host" ]]; do
  [[ -z "$host" || "$host" =~ ^[[:space:]]*# ]] && continue
  host="${host%%#*}"
  host="$(echo "$host" | xargs)"
  [[ -z "$host" ]] && continue
  checked=$((checked + 1))

  echo "==> $host"
  # shellcheck disable=SC2086
  ssh $SSH_OPTS "$host" "SUDO=''; if [ \"\$(id -u)\" != \"0\" ]; then SUDO='sudo'; fi; \$SUDO systemctl is-active '$SERVICE_NAME'; \$SUDO journalctl -u '$SERVICE_NAME' -n 8 --no-pager"
done < "$VPS_FILE"

if [[ "$checked" -eq 0 ]]; then
  echo "no VPS hosts found in $VPS_FILE" >&2
  exit 1
fi
