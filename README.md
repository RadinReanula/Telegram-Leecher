# Telegram Leecher

[![Repository](https://img.shields.io/badge/GitHub-Telegram--Leecher-181717?logo=github)](https://github.com/RadinReanula/Telegram-Leecher)

Personal Telegram bot that downloads media from message links using a **Bot API UI** (aiogram) and a **user session** (Pyrogram). Works for many public and private `t.me/...` links when your account is already in the chatΓÇöincluding cases where the mobile app hides the save button.

## Features

### Downloads

- Paste **one or many** message links (batch queue with per-link status)
- **Public** channels (`t.me/channel/123`) and **private** supergroups (`t.me/c/.../123`)
- **Albums** expand fully for single-link and batch jobs (duplicate album links in one batch are skipped)
- **Typed media delivery** for common formats (video/audio/image documents) on bot and user-session paths ΓÇö no ffmpeg
- **Large files** delivered via user session (DM) when over bot upload limits (~20ΓÇô50 MB)
- **Video metadata** and thumbnails on upload (duration, preview tile)
- **Byte-level progress** in status messages during download (configurable)

### Queue and control

- **Job queue** with IDs, stages, and throttled live status edits
- **`/status`** ΓÇö compact list of your jobs
- **`/job <id>`** ΓÇö full detail for one job
- **`/queue`** ΓÇö global waiting / running counts
- **`/stop`** ΓÇö cancel **all your** queued and running jobs (including god mode) without restarting the bot
- **`/god up|down`** ΓÇö crawl a chat by message ID and download media sequentially
- **FloodWait auto-retry** (configurable; god mode backs off and continues)
- **Skipped** jobs for links with no media (not treated as failures)

### Performance and reliability

- **Private peer cache** (`sessions/peer_cache.json`) ΓÇö faster repeat `t.me/c/` links
- **Background dialog sync** ΓÇö bot starts polling immediately; chat list warms in parallel
- **Album pipeline** (optional) ΓÇö overlap download of next item with upload of previous
- **Bot SSL** ΓÇö certifi CA bundle; optional `BOT_SSL_VERIFY=false` for broken HTTPS inspection
- **Parallel batch enqueue** ΓÇö faster status messages when pasting many links

### Deployment

- **Local / dev** ΓÇö `python -m app.main`
- **VPS / production** ΓÇö systemd service, logrotate, deploy guide ΓåÆ [DEPLOY.md](DEPLOY.md)

## Architecture

| Component | Role |
|-----------|------|
| **aiogram bot** | Commands, link intake, status messages, delivery under 50 MB |
| **Pyrogram user client** | Downloads media with your authorized account (`no_updates=True`) |
| **Job queue** | Worker pool, per-user limits, cancellation, throttled status updates |

```text
You ΓåÆ Bot (paste links) ΓåÆ Queue ΓåÆ Pyrogram download ΓåÆ Bot or user session upload
                              Γåæ
                         /stop cancels your jobs
```

## Job lifecycle

Each link becomes a job (e.g. `a1b2c3d4`) with its own status message.

| Status | Meaning |
|--------|---------|
| `queued` | Waiting in line |
| `running` | Resolving chat ΓåÆ downloading ΓåÆ uploading |
| `completed` | File(s) sent |
| `skipped` | No media or invalid link |
| `failed` | Error (chat access, FloodWait exhausted, etc.) |
| `cancelled` | Stopped by `/stop` |

Stages shown while running: `resolving` ΓåÆ `downloading` ΓåÆ `uploading`.

## Requirements

### System

- Python **3.11+** (tested on 3.13)
- Windows, macOS, or Linux (VPS: see [DEPLOY.md](DEPLOY.md))
- Telegram [API_ID / API_HASH](https://my.telegram.org/apps)
- Bot token from [@BotFather](https://t.me/BotFather)
- On Windows: [MSVC Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/) if `tgcrypto` has no prebuilt wheel for your Python version

No separate install for **ffmpeg**, **Redis**, or a database ΓÇö the bot uses only Python packages and Telegram APIs.

### Python packages

Install everything with `pip install -r requirements.txt`. Direct dependencies:

| Package | Role in this project |
|---------|----------------------|
| `pyrogram` | User-session downloads |
| `tgcrypto` | Fast Pyrogram crypto (optional speedup; needs a C compiler if no wheel) |
| `aiogram` | Bot commands, messages, uploads under 50 MB |
| `pydantic` | Settings validation (`app/config.py`) |
| `pydantic-settings` | Load `.env` into settings |
| `python-dotenv` | `.env` file support (used by pydantic-settings) |
| `certifi` | CA bundle for Bot API HTTPS (`BOT_SSL_VERIFY`) |

`aiohttp` / `aiofiles` (aiogram HTTP) and `pyaes` / `pysocks` (Pyrogram) install automatically as transitive deps.

## Quick start

### 1. Clone the repository

```bash
git clone https://github.com/RadinReanula/Telegram-Leecher.git
cd Telegram-Leecher
```

### 2. Create Telegram credentials

1. [my.telegram.org/apps](https://my.telegram.org/apps) ΓåÆ **API_ID** and **API_HASH**
2. [@BotFather](https://t.me/BotFather) ΓåÆ `/newbot` ΓåÆ **BOT_TOKEN**
3. [@userinfobot](https://t.me/userinfobot) ΓåÆ your numeric **user id** for `ALLOWED_USER_IDS`

### 3. Configure environment

```powershell
# Windows
copy .env.example .env
```

```bash
# macOS / Linux
cp .env.example .env
```

Edit `.env` with your credentials ΓÇö never commit this file.

`sessions/` and `tmp/` are created automatically on first `login.py` / bot run (no manual `mkdir`).

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

Open your bot in Telegram ΓåÆ `/start` ΓåÆ paste message link(s).

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
| `/stop` | Cancel **all your** queued and running jobs (including god); bot keeps running |
| `/god up\|down [link]` | Crawl chat media by message ID (see below) |
| `/job <id>` | Full details for one job |
| `/queue` | Global queue summary (waiting / running / workers) |
| Paste link(s) | Enqueue download(s); one status message per link |

### Media formats

Telegram classifies media when it is sent. This bot re-uploads using typed methods when possible:

| Kind | Examples | Behavior |
|------|----------|----------|
| Video | `.mp4`, `.mov`, `.mkv`, `.avi`, `.webm` | `send_video` when Telegram marks as video or `video/*` / known suffix document |
| Audio | `.mp3`, `.wav`, `.aac`, `.flac` | `send_audio` (including audio documents) |
| Image | `.jpg`, `.png`, `.webp` | `send_photo` (including image documents) |
| GIF | `.gif` / animation | `send_animation` |

No remux/transcode (no ffmpeg). If a typed send is rejected, the bot falls back once to `send_document`. Large files still use the user-session path with the same typed routing.

### `/stop` behavior

- Scoped to **you** only (`ALLOWED_USER_IDS` user), same as `/status`
- **Queued** jobs: removed immediately; status shows cancelled
- **Running** job: worker task is cancelled; may finish the **current file** before stop applies (Pyrogram cannot abort mid-download cleanly)
- **Albums / god mode**: stops before the next item when possible
- Clears a pending `/god up` / `/god down` session waiting for a link
- After `/stop`, paste new links normally ΓÇö no restart required

### God mode (`/god`)

Crawl many messages in one chat without pasting every link. Message IDs generally increase as new messages are posted.

| Command | Behavior |
|---------|----------|
| `/god up <link>` | Start at that message ID and walk **toward newer** IDs |
| `/god down <link>` | Start at that message ID and walk **toward older** IDs (down to `1`) |
| `/god up` or `/god down` | Set direction; paste **one** link in the next message (5 min TTL) |
| `/god` | Show usage |

- One composite job (not hundreds of queue entries); `/status` and `/job` show crawl counters
- Skips text-only messages; treats deleted/missing IDs as misses (does not fail the whole run)
- Expands albums once; later IDs in the same album are skipped
- `/god up` stops after `GOD_MAX_CONSECUTIVE_MISS` consecutive misses (end of chat)
- Delays between IDs (`GOD_DELAY_SEC`) and handles FloodWait inside the crawl
- `/stop` cancels the crawl ASAP
- Keep `QUEUE_WORKERS=1` while using god mode

### Batch downloads

Send multiple links in one message (spaces or newlines, up to `MAX_LINKS_PER_MESSAGE`):

- Summary: `Queued N download(s).`
- One status message per link (with `(2/10)` batch label when applicable)
- Jobs run sequentially by default (`QUEUE_WORKERS=1`)
- **Albums expand fully** for each link; if several links point at the **same** album, only the first downloads it (others skip as duplicate)
- Use `/stop` to abort the whole batch

## Configuration

Copy [.env.example](.env.example) to `.env`.

### Credentials and paths

| Setting | Default | Meaning |
|---------|---------|---------|
| `API_ID` / `API_HASH` | ΓÇö | From [my.telegram.org](https://my.telegram.org/apps) |
| `BOT_TOKEN` | ΓÇö | From @BotFather |
| `ALLOWED_USER_IDS` | ΓÇö | Comma-separated Telegram user IDs allowed to use the bot |
| `SESSION_NAME` | `user` | Pyrogram session file base name |
| `SESSIONS_DIR` | `sessions` | Session and peer cache directory |
| `TMP_DIR` | `tmp` | Temporary download files |

### Bot API and SSL

| Setting | Default | Meaning |
|---------|---------|---------|
| `BOT_MAX_FILE_BYTES` | `52428800` | Hard cap for bot upload (~50 MB) |
| `BOT_UPLOAD_THRESHOLD_BYTES` | `20971520` | Above ~20 MB ΓåÆ user session (fewer timeouts) |
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

### God mode

| Setting | Default | Meaning |
|---------|---------|---------|
| `GOD_DELAY_SEC` | `2.0` | Sleep between message IDs (anti-FloodWait) |
| `GOD_FLOODWAIT_EXTRA_SEC` | `5` | Extra seconds after a FloodWait sleep |
| `GOD_MAX_CONSECUTIVE_MISS` | `25` | Stop `/god up` after this many consecutive missing IDs |
| `GOD_MAX_MESSAGES` | `5000` | Hard safety cap per god run |
| `GOD_SKIP_ALREADY_SEEN_GROUPS` | `true` | Skip album members already expanded in the crawl |

**Tips:** Set `ALBUM_PIPELINE=true` for faster multi-photo albums. After first run, private peers stay in `sessions/peer_cache.json`. Try `QUEUE_WORKERS=2` only if you accept more FloodWait risk. Prefer `QUEUE_WORKERS=1` during god mode crawls.

## Project layout

```text
Telegram-Leecher/
Γö£ΓöÇΓöÇ app/
Γöé   Γö£ΓöÇΓöÇ main.py                 # Entry point, startup, polling
Γöé   Γö£ΓöÇΓöÇ config.py               # Settings from .env
Γöé   Γö£ΓöÇΓöÇ bot/handlers.py         # Commands, links, /god, /stop
Γöé   Γö£ΓöÇΓöÇ queue/
Γöé   Γöé   Γö£ΓöÇΓöÇ manager.py          # Workers, enqueue, cancel, god branch
Γöé   Γöé   Γö£ΓöÇΓöÇ models.py           # Job status / stages / god fields
Γöé   Γöé   Γö£ΓöÇΓöÇ exceptions.py       # JobCancelledError
Γöé   Γöé   ΓööΓöÇΓöÇ status_format.py    # /status and status message text
Γöé   Γö£ΓöÇΓöÇ downloader/             # Pyrogram download, upload, peer cache, god crawl
Γöé   Γö£ΓöÇΓöÇ parser/                 # t.me link parsing
Γöé   ΓööΓöÇΓöÇ network/                # Bot HTTPS session (SSL)
Γö£ΓöÇΓöÇ deploy/
Γöé   Γö£ΓöÇΓöÇ telegram-leecher.service
Γöé   ΓööΓöÇΓöÇ telegram-leecher.logrotate
Γö£ΓöÇΓöÇ DEPLOY.md                   # VPS systemd guide
Γö£ΓöÇΓöÇ login.py                    # One-time user session auth
Γö£ΓöÇΓöÇ requirements.txt
Γö£ΓöÇΓöÇ .env.example
ΓööΓöÇΓöÇ README.md
```

## Security

- **Never commit** `.env`, `sessions/`, or `tmp/` to a **public** repo.
- `sessions/user.session` is full account accessΓÇötreat like a password.
- Set `ALLOWED_USER_IDS` so only you can use the bot.
- Use only on content you are allowed to access; respect copyright and group rules.
- Private deploy clone may include secretsΓÇökeep that repository **private**.

## Troubleshooting

| Error / symptom | Fix |
|-----------------|-----|
| No user session | Run `python login.py` |
| Cannot access chat | Join the chat with the login account |
| FloodWait | Wait; increase `FLOODWAIT_MAX_RETRIES` |
| File in DM from user session | Normal for large files (> ~20 MB or bot limit) |
| `CERTIFICATE_VERIFY_FAILED` (bot) | Fix system CA / AV HTTPS scanning; last resort `BOT_SSL_VERIFY=false` |
| Private `t.me/c/...` fails | Open chat in Telegram, restart bot, wait for `Synced N dialog(s)` |
| Batch only shows ΓÇ£Queued NΓÇ¥ | Update to latest code (batch status message fix) |
| Batch album only one file | Update to latest ΓÇö albums now expand in batch; duplicates skip |
| `/stop` but one file still arrives | Current download may finish before cancel; queued jobs stop immediately |
| God mode FloodWait / slow | Increase `GOD_DELAY_SEC`; keep `QUEUE_WORKERS=1` |
| God mode never stops on `/god up` | Raise or lower `GOD_MAX_CONSECUTIVE_MISS`; check chat access |
| `TelegramConflictError` on VPS | Stop other instances (PC or second terminal); one bot token = one poller |
| Bot slow to respond after start | Normal if dialog sync runs in background; private links work after sync |

## License

MIT License ΓÇö see [LICENSE](LICENSE).

## Author

[RadinReanula](https://github.com/RadinReanula) ΓÇö [Telegram-Leecher](https://github.com/RadinReanula/Telegram-Leecher)
