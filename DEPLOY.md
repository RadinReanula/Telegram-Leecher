# VPS deployment (systemd)

Run the bot as a background service that starts on boot, survives SSH disconnect, restarts on failure, and logs to `/var/log/telegram-leecher/app.log`.

**Do not use cron** for this — cron is for scheduled one-shot tasks. This bot is a long-running process; **systemd** is the right tool.

## Prerequisites

- Project cloned at `/home/rdvps/projects/Telegram-Leecher-Source`
- Virtualenv created and dependencies installed (`pip install -r requirements.txt`)
- `.env` and `sessions/user.session` present (private repo clone)
- No other instance of this bot running (PC or second terminal) — avoids `TelegramConflictError`

## One-time setup

### 1. Stop any manual instance

```bash
pkill -f "python -m app.main" || true
```

### 2. Create log directory

```bash
sudo mkdir -p /var/log/telegram-leecher
sudo chown rdvps:rdvps /var/log/telegram-leecher
```

### 3. Install systemd service

From the project root:

```bash
cd ~/projects/Telegram-Leecher-Source
sudo cp deploy/telegram-leecher.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable telegram-leecher
sudo systemctl start telegram-leecher
```

### 4. Verify

```bash
sudo systemctl status telegram-leecher
tail -f /var/log/telegram-leecher/app.log
```

You should see `Run polling for bot @...` without repeating `TelegramConflictError`.

### 5. Install log rotation

```bash
sudo cp deploy/telegram-leecher.logrotate /etc/logrotate.d/telegram-leecher
```

Test config (dry run):

```bash
sudo logrotate -d /etc/logrotate.d/telegram-leecher
```

### 6. Optional reboot test

```bash
sudo reboot
```

After reconnect:

```bash
systemctl is-active telegram-leecher
```

## Day-to-day commands

| Action | Command |
|--------|---------|
| View logs | `tail -f /var/log/telegram-leecher/app.log` |
| Status | `sudo systemctl status telegram-leecher` |
| Restart (e.g. after `.env` change) | `sudo systemctl restart telegram-leecher` |
| Stop | `sudo systemctl stop telegram-leecher` |
| Start | `sudo systemctl start telegram-leecher` |
| Disable autostart on boot | `sudo systemctl disable telegram-leecher` |

### After code or config update

```bash
cd ~/projects/Telegram-Leecher-Source
git pull
sudo systemctl restart telegram-leecher
```

If `requirements.txt` changed:

```bash
source .venv/bin/activate
pip install -r requirements.txt
sudo systemctl restart telegram-leecher
```

## User session (`login.py`)

`login.py` is **interactive** (phone / OTP / 2FA). Run it manually when you need to create or refresh the Pyrogram session — not via systemd:

```bash
cd ~/projects/Telegram-Leecher-Source
source .venv/bin/activate
python login.py
```

Then restart the service:

```bash
sudo systemctl restart telegram-leecher
```

## Custom paths

If your install path or Linux user differs from `rdvps` / `/home/rdvps/projects/Telegram-Leecher-Source`, edit [`deploy/telegram-leecher.service`](deploy/telegram-leecher.service) before copying it to `/etc/systemd/system/`, then run `sudo systemctl daemon-reload`.

## Troubleshooting

### `TelegramConflictError` / "only one bot instance"

Another process is polling with the same bot token. Stop the bot on your PC and any extra VPS processes:

```bash
pgrep -af "app.main"
sudo systemctl stop telegram-leecher
# stop other instances, then:
sudo systemctl start telegram-leecher
```

### Service fails immediately

```bash
sudo journalctl -u telegram-leecher -n 50 --no-pager
cat /var/log/telegram-leecher/app.log
```

Common causes: missing `.env`, missing `sessions/user.session`, wrong `ExecStart` path to `.venv/bin/python`.

### Logs not appearing

Ensure the log directory exists and is owned by `rdvps`:

```bash
ls -la /var/log/telegram-leecher
```

### Service in restart loop

Check `.env` values and run manually once:

```bash
cd ~/projects/Telegram-Leecher-Source
source .venv/bin/activate
python -m app.main
```

Fix errors, then `sudo systemctl reset-failed telegram-leecher && sudo systemctl start telegram-leecher`.
