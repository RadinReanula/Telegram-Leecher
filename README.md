# Telegram Leecher

[![Repository](https://img.shields.io/badge/GitHub-Telegram--Leecher-181717?logo=github)](https://github.com/RadinReanula/Telegram-Leecher)

Personal Telegram bot that downloads media from message links using a **Bot API UI** (aiogram) and a **user session** (Pyrogram). Works for many public and private `t.me/...` links when your account is already in the chat—including cases where the mobile app hides the save button.

## Features

### Downloads

- Paste **one or many** message links (batch queue with per-link status)
- **Public** channels (`t.me/channel/123`) and **private** supergroups (`t.me/c/.../123`)
- **Albums** on single-link jobs (full media group); batch jobs fetch only the linked message
- **Large files** delivered via user session (DM) when over bot upload limits (~20–50 MB)
- **Video metadata** and thumbnails on upload (duration, preview tile)
- **Byte-level progress** in status messages during download (configurable)

### Queue and control

- **Job queue** with IDs, stages, and throttled live status edits
- **`/status`** — compact list of your jobs
- **`/job <id>`** — full detail for one job
- **`/queue`** — global waiting / running counts
- **`/stop`** — cancel **all your** queued and running jobs without restarting the bot
- **FloodWait auto-retry** (configurable)
- **Skipped** jobs for links with no media (not treated as failures)

### Performance and reliability

- **Private peer cache** (`sessions/peer_cache.json`) — faster repeat `t.me/c/` links
- **Background dialog sync** — bot starts polling immediately; chat list warms in parallel
- **Album pipeline** (optional) — overlap download of next item with upload of previous
- **Bot SSL** — certifi CA bundle; optional `BOT_SSL_VERIFY=false` for broken HTTPS inspection
- **Parallel batch enqueue** — faster status messages when pasting many links

### Deployment

- **Local / dev** — `python -m app.main`
- **VPS / production** — systemd service, logrotate, deploy guide → [DEPLOY.md](DEPLOY.md)

## Architecture

| Component | Role |
|-----------|------|
| **aiogram bot** | Commands, link intake, status messages, delivery under 50 MB |
| **Pyrogram user client** | Downloads media with your authorized account (`no_updates=True`) |
| **Job queue** | Worker pool, per-user limits, cancellation, throttled status updates |

```text
You → Bot (paste links) → Queue → Pyrogram download → Bot or user session upload
                              ↑
                         /stop cancels your jobs
```

## Job lifecycle

Each link becomes a job (e.g. `a1b2c3d4`) with its own status message.

| Status | Meaning |
|--------|---------|
| `queued` | Waiting in line |
| `running` | Resolving chat → downloading → uploading |
| `completed` | File(s) sent |
| `skipped` | No media or invalid link |
| `failed` | Error (chat access, FloodWait exhausted, etc.) |
| `cancelled` | Stopped by `/stop` |

Stages shown while running: `resolving` → `downloading` → `uploading`.

## Requirements

- Python 3.11+ (tested on 3.13)
- Windows, macOS, or Linux (VPS: see [DEPLOY.md](DEPLOY.md))
- Telegram [API_ID / API_HASH](https://my.telegram.org/apps)
- Bot token from [@BotFather](https://t.me/BotFather)
- On Windows: [MSVC Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/) if `tgcrypto` has no prebuilt wheel

## Quick start

### 1. Clone the repository

```bash
git clone https://github.com/RadinReanula/Telegram-Leecher.git
cd Telegram-Leecher
```

### 2. Create Telegram credentials

1. [my.telegram.org/apps](https://my.telegram.org/apps) → **API_ID** and **API_HASH**
2. [@BotFather](https://t.me/BotFather) → `/newbot` → **BOT_TOKEN**
3. [@userinfobot](https://t.me/userinfobot) → your numeric **user id** for `ALLOWED_USER_IDS`

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env — never commit this file
```

### 4. Install and log in

```powershell
# Windows
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python login.py
```

```bash
# macOS / Linux
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python login.py
```

`login.py` creates `sessions/user.session` (one-time phone / OTP / 2FA).

### 5. Run the bot

```bash
python -m app.main
```

Open your bot in Telegram → `/start` → paste message link(s).

### 6. VPS (optional)

For a Linux server that survives reboot and SSH disconnect, follow **[DEPLOY.md](DEPLOY.md)** (`deploy/telegram-leecher.service`, logrotate, `systemctl`).

## Usage

| Link type | Example |
|-----------|---------|
| Public channel | `https://t.me/somechannel/42` |
| Private supergroup/channel | `https://t.me/c/1867392134/42` |

- The **user account** from `login.py` must be in the target chat.
- For private links, copy the URL from Telegram while logged in as that account.

### Commands

| Command | Description |
|---------|-------------|
| `/start` | Introduction and command list |
| `/help` | Short help |
| `/auth` | User session status (Pyrogram login) |
| `/status` | Your jobs summary (newest first) |
| `/stop` | Cancel **all your** queued and running jobs; bot keeps running |
| `/job <id>` | Full details for one job |
| `/queue` | Global queue summary (waiting / running / workers) |
| Paste link(s) | Enqueue download(s); one status message per link |

### `/stop` behavior

- Scoped to **you** only (`ALLOWED_USER_IDS` user), same as `/status`
- **Queued** jobs: removed immediately; status shows cancelled
- **Running** job: worker task is cancelled; may finish the **current file** before stop applies (Pyrogram cannot abort mid-download cleanly)
- **Albums**: stops before the next item when possible
- After `/stop`, paste new links normally — no restart required

### Batch downloads

Send multiple links in one message (spaces or newlines, up to `MAX_LINKS_PER_MESSAGE`):

- Summary: `Queued N download(s).`
- One status message per link (with `(2/10)` batch label when applicable)
- Jobs run sequentially by default (`QUEUE_WORKERS=1`)
- Batch jobs fetch **only the linked message**, not the full album (avoids duplicate files)
- Use `/stop` to abort the whole batch

## Configuration

Copy [.env.example](.env.example) to `.env`.

### Credentials and paths

| Setting | Default | Meaning |
|---------|---------|---------|
| `API_ID` / `API_HASH` | — | From [my.telegram.org](https://my.telegram.org/apps) |
| `BOT_TOKEN` | — | From @BotFather |
| `ALLOWED_USER_IDS` | — | Comma-separated Telegram user IDs allowed to use the bot |
| `SESSION_NAME` | `user` | Pyrogram session file base name |
| `SESSIONS_DIR` | `sessions` | Session and peer cache directory |
| `TMP_DIR` | `tmp` | Temporary download files |

### Bot API and SSL

| Setting | Default | Meaning |
|---------|---------|---------|
| `BOT_MAX_FILE_BYTES` | `52428800` | Hard cap for bot upload (~50 MB) |
| `BOT_UPLOAD_THRESHOLD_BYTES` | `20971520` | Above ~20 MB → user session (fewer timeouts) |
| `BOT_REQUEST_TIMEOUT_SEC` | `600` | Bot API upload timeout (seconds) |
| `BOT_SSL_VERIFY` | `true` | Set `false` only if HTTPS inspection breaks the bot API |

### Job queue

| Setting | Default | Meaning |
|---------|---------|---------|
| `QUEUE_WORKERS` | `1` | Parallel workers (higher = more FloodWait risk) |
| `MAX_QUEUE_SIZE` | `50` | Max jobs waiting globally |
| `MAX_PENDING_PER_USER` | `5` | Max queued + running per user (single-link messages) |
| `MAX_LINKS_PER_MESSAGE` | `25` | Max links per message (batch burst bypasses per-user cap) |
| `JOB_HISTORY_LIMIT` | `200` | Finished jobs kept in memory for `/status` |
| `STATUS_UPDATE_INTERVAL_SEC` | `2` | Min seconds between live progress edits |
| `FLOODWAIT_MAX_RETRIES` | `1` | Auto-retry after Telegram FloodWait (`0` = fail immediately) |
| `JOB_PRUNE_EVERY_N_ENQUEUES` | `10` | How often to prune old jobs from memory |

### Performance

| Setting | Default | Meaning |
|---------|---------|---------|
| `SYNC_DIALOGS_ON_STARTUP` | `true` | Load chat list for private `t.me/c/` resolution |
| `SYNC_DIALOGS_IN_BACKGROUND` | `true` | Start polling before dialog sync finishes |
| `ALBUM_PIPELINE` | `false` | Overlap album download with previous upload |
| `ALBUM_CONCURRENCY` | `1` | Concurrent album downloads (if pipeline enabled) |
| `DOWNLOAD_PROGRESS_ENABLED` | `true` | Byte-level progress in status during download |

**Tips:** Set `ALBUM_PIPELINE=true` for faster multi-photo albums. After first run, private peers stay in `sessions/peer_cache.json`. Try `QUEUE_WORKERS=2` only if you accept more FloodWait risk.

## Project layout

```text
Telegram-Leecher/
├── app/
│   ├── main.py                 # Entry point, startup, polling
│   ├── config.py               # Settings from .env
│   ├── bot/handlers.py         # Commands, links, /stop
│   ├── queue/
│   │   ├── manager.py          # Workers, enqueue, cancel
│   │   ├── models.py           # Job status / stages
│   │   ├── exceptions.py       # JobCancelledError
│   │   └── status_format.py    # /status and status message text
│   ├── downloader/             # Pyrogram download, upload, peer cache
│   ├── parser/                 # t.me link parsing
│   └── network/                # Bot HTTPS session (SSL)
├── deploy/
│   ├── telegram-leecher.service
│   └── telegram-leecher.logrotate
├── DEPLOY.md                   # VPS systemd guide
├── login.py                    # One-time user session auth
├── requirements.txt
├── .env.example
└── README.md
```

## Security

- **Never commit** `.env`, `sessions/`, or `tmp/` to a **public** repo.
- `sessions/user.session` is full account access—treat like a password.
- Set `ALLOWED_USER_IDS` so only you can use the bot.
- Use only on content you are allowed to access; respect copyright and group rules.
- Private deploy clone may include secrets—keep that repository **private**.

## Troubleshooting

| Error / symptom | Fix |
|-----------------|-----|
| No user session | Run `python login.py` |
| Cannot access chat | Join the chat with the login account |
| FloodWait | Wait; increase `FLOODWAIT_MAX_RETRIES` |
| File in DM from user session | Normal for large files (> ~20 MB or bot limit) |
| `CERTIFICATE_VERIFY_FAILED` (bot) | Fix system CA / AV HTTPS scanning; last resort `BOT_SSL_VERIFY=false` |
| Private `t.me/c/...` fails | Open chat in Telegram, restart bot, wait for `Synced N dialog(s)` |
| Batch only shows “Queued N” | Update to latest code (batch status message fix) |
| `/stop` but one file still arrives | Current download may finish before cancel; queued jobs stop immediately |
| `TelegramConflictError` on VPS | Stop other instances (PC or second terminal); one bot token = one poller |
| Bot slow to respond after start | Normal if dialog sync runs in background; private links work after sync |

## License

MIT License — see [LICENSE](LICENSE).

## Author

[RadinReanula](https://github.com/RadinReanula) — [Telegram-Leecher](https://github.com/RadinReanula/Telegram-Leecher)
