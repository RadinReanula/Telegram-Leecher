#!/usr/bin/env bash
# Pull latest code on VPS without losing live Pyrogram session / peer cache files.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> Project: $ROOT"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "ERROR: not a git repository: $ROOT" >&2
  exit 1
fi

BACKUP_DIR="$(mktemp -d)"
trap 'rm -rf "$BACKUP_DIR"' EXIT

for f in sessions/user.session sessions/peer_cache.json; do
  if [[ -f "$f" ]]; then
    cp "$f" "$BACKUP_DIR/$(basename "$f")"
  fi
done

echo "==> Stashing local session file changes (if any)..."
git stash push -m "vps-live-sessions-$(date +%Y%m%d%H%M%S)" -- sessions/user.session sessions/peer_cache.json 2>/dev/null || true

echo "==> Pulling origin/main..."
git pull origin main

echo "==> Restoring live session files from backup..."
[[ -f "$BACKUP_DIR/user.session" ]] && cp "$BACKUP_DIR/user.session" sessions/user.session
[[ -f "$BACKUP_DIR/peer_cache.json" ]] && cp "$BACKUP_DIR/peer_cache.json" sessions/peer_cache.json

echo "==> Clearing __pycache__..."
find . -type d -name __pycache__ -prune -exec rm -rf {} + 2>/dev/null || true

if [[ -f .venv/bin/activate ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
  pip install -r requirements.txt
fi

echo "==> Verifying god mode build..."
python scripts/verify_god_mode.py

echo ""
echo "OK — restart the service to load new code:"
echo "  sudo systemctl restart telegram-leecher"
echo "  grep -i 'God mode active' /var/log/telegram-leecher/app.log | tail -1"
