#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${OUT_DIR:-$ROOT_DIR/dist}"
GOOS="${GOOS:-linux}"
GOARCH="${GOARCH:-amd64}"
BIN_NAME="${BIN_NAME:-proxy-client-${GOOS}-${GOARCH}}"
GOCACHE="${GOCACHE:-/tmp/api-fucker-go-build}"

mkdir -p "$OUT_DIR"
mkdir -p "$GOCACHE"
export GOCACHE

echo "building $BIN_NAME"
CGO_ENABLED=0 GOOS="$GOOS" GOARCH="$GOARCH" \
  go build -trimpath -ldflags="-s -w" \
  -o "$OUT_DIR/$BIN_NAME" "$ROOT_DIR/cmd/proxy-client"

echo "$OUT_DIR/$BIN_NAME"
