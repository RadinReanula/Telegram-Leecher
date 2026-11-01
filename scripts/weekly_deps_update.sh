#!/usr/bin/env bash
# Upgrade project dependencies, smoke-test imports, restart on success.
# A failed upgrade reinstalls the previous freeze and leaves the bot running.
set -u

PROJECT_DIR="${PROJECT_DIR:-/home/azureuser/Projects/Telegram-Leecher-Source}"
VENV_PY="${VENV_PY:-$PROJECT_DIR/.venv/bin/python}"
VENV_PIP="${VENV_PIP:-$PROJECT_DIR/.venv/bin/pip}"
LOG_FILE="${LOG_FILE:-/var/log/telegram-leecher/deps-update.log}"
SERVICE_NAME="${SERVICE_NAME:-telegram-leecher}"

log() {
  printf '%s %s\n' "$(date -Is)" "$*" | tee -a "$LOG_FILE"
}

mkdir -p "$(dirname "$LOG_FILE")"

if [[ "${1:-}" == "--dry-run-fail" ]]; then
  log "SIMULATED pip failure — rollback would run; restart skipped"
  exit 1
fi

if [[ ! -x "$VENV_PY" || ! -x "$VENV_PIP" ]]; then
  log "ERROR: venv python/pip not found under $PROJECT_DIR"
  exit 1
fi

FREEZE="$(mktemp)"
log "Starting dependency update in $PROJECT_DIR"
if ! "$VENV_PIP" freeze >"$FREEZE"; then
  log "ERROR: pip freeze failed; restart skipped"
  rm -f "$FREEZE"
  exit 1
fi

if ! "$VENV_PIP" install -r "$PROJECT_DIR/requirements.txt" --upgrade >>"$LOG_FILE" 2>&1; then
  log "ERROR: pip upgrade failed; restoring previous packages"
  "$VENV_PIP" install -r "$FREEZE" >>"$LOG_FILE" 2>&1 || log "ERROR: rollback install failed"
  rm -f "$FREEZE"
  log "Restart skipped"
  exit 1
fi

if ! "$VENV_PY" -c "import pyrogram, aiogram" >>"$LOG_FILE" 2>&1; then
  log "ERROR: import smoke test failed; restoring previous packages"
  "$VENV_PIP" install -r "$FREEZE" >>"$LOG_FILE" 2>&1 || log "ERROR: rollback install failed"
  rm -f "$FREEZE"
  log "Restart skipped"
  exit 1
fi

rm -f "$FREEZE"
log "Smoke import OK. Installed:"
"$VENV_PIP" freeze >>"$LOG_FILE" 2>&1 || true
if command -v systemctl >/dev/null 2>&1; then
  systemctl restart "$SERVICE_NAME"
  log "Restarted $SERVICE_NAME"
else
  log "systemctl not available; restart skipped"
fi
exit 0
