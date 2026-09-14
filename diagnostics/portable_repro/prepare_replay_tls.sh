#!/usr/bin/env bash
set -euo pipefail

OUT="${1:?usage: prepare_replay_tls.sh OUTPUT_DIR}"
mkdir -p "$OUT"
CERT="$OUT/replay-cert.pem"
KEY="$OUT/replay-key.pem"

command -v openssl >/dev/null 2>&1 || { echo "openssl is required for replay TLS" >&2; exit 2; }

# Ephemeral certificate scoped to loopback. Never reuse production provider keys.
openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 1 \
  -keyout "$KEY" \
  -out "$CERT" \
  -subj "/CN=127.0.0.1" \
  -addext "subjectAltName=IP:127.0.0.1,DNS:localhost" \
  >/dev/null 2>&1
chmod 600 "$KEY"
printf '%s\n%s\n' "$CERT" "$KEY"
