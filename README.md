# Telegram Leecher

[![Repository](https://img.shields.io/badge/GitHub-Telegram--Leecher-181717?logo=github)](https://github.com/RadinReanula/Telegram-Leecher)

Personal Telegram bot that downloads media from message links using a **Bot API UI** (aiogram) and a **user session** (Pyrogram). Works for many public and private `t.me/...` links when your account is already in the chat—including cases where the mobile app hides the save button.

## Features

- Paste one or many message links (batch queue with per-link status)
- Public channels (`t.me/channel/123`) and private supergroups (`t.me/c/.../123`)
- Job queue with progress, `/status`, `/job <id>`, `/queue`
- Large files sent via user session (DM) when over bot upload limits
- Video metadata and thumbnails on upload
- Private peer cache (`sessions/peer_cache.json`) for faster `t.me/c/` resolution
- Configurable performance options (background dialog sync, album pipeline, FloodWait retry)

## Architecture

| Component | Role |
|-----------|------|
| **aiogram bot** | Commands, link intake, status messages, delivery under 50 MB |
| **Pyrogram user client** | Downloads media with your authorized account |
| **Job queue** | Ordered workers, throttled status updates |

```text
You → Bot (paste links) → Queue → Pyrogram download → Bot or user session upload
```

## Requirements

- Python 3.11+ (tested on 3.13)
- Windows, macOS, or Linux
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
| `/start` | Introduction |
| `/help` | Short help |
| `/auth` | User session status |
| `/status` | Your jobs summary |
| `/stop` | Cancel all your queued and running download jobs (bot keeps running) |
| `/job <id>` | Full details for one job |
| `/queue` | Global queue summary |
| Paste link(s) | Enqueue download(s) |

### Batch downloads

Send multiple links in one message (spaces or newlines, up to `MAX_LINKS_PER_MESSAGE`):

- Summary: `Queued N download(s).`
- One status message per link
- Jobs run sequentially by default (`QUEUE_WORKERS=1`)
- Batch jobs fetch **only the linked message**, not the full album (avoids duplicates)
- Send `/stop` to cancel all your active jobs without restarting the bot (queued jobs stop immediately; a file currently downloading may finish that file first)

## Configuration

Copy [.env.example](.env.example) to `.env`. Main settings:

| Setting | Default | Meaning |
|---------|---------|---------|
| `ALLOWED_USER_IDS` | — | Comma-separated Telegram user IDs allowed to use the bot |
| `QUEUE_WORKERS` | `1` | Parallel workers (raise only if you accept more FloodWait risk) |
| `MAX_LINKS_PER_MESSAGE` | `25` | Max links per message |
| `BOT_UPLOAD_THRESHOLD_BYTES` | `20971520` | Above ~20 MB, send via user session |
| `BOT_MAX_FILE_BYTES` | `52428800` | Bot API cap (~50 MB) |
| `SYNC_DIALOGS_ON_STARTUP` | `true` | Warm peer cache for private links |
| `SYNC_DIALOGS_IN_BACKGROUND` | `true` | Start polling before dialog sync finishes |
| `FLOODWAIT_MAX_RETRIES` | `1` | Retry after Telegram FloodWait |
| `ALBUM_PIPELINE` | `false` | Overlap album download with previous upload |
| `DOWNLOAD_PROGRESS_ENABLED` | `true` | Byte-level progress in status |
| `BOT_SSL_VERIFY` | `true` | Set `false` only if HTTPS inspection breaks the bot API |

See `.env.example` for the full list.

## Project layout

```text
Telegram-Leecher/
├── app/
│   ├── main.py              # Entry point
│   ├── config.py            # Settings from .env
│   ├── bot/handlers.py      # Telegram bot commands & link intake
│   ├── queue/               # Job queue & status formatting
│   ├── downloader/          # Pyrogram download & delivery
│   ├── parser/              # t.me link parsing
│   └── network/             # Bot HTTPS session (SSL)
├── login.py                 # One-time user session auth
├── requirements.txt
├── .env.example
└── README.md
```

## Security

- **Never commit** `.env`, `sessions/`, or `tmp/` (already in `.gitignore`).
- `sessions/user.session` is full account access—treat like a password.
- Set `ALLOWED_USER_IDS` so only you can use the bot.
- Use only on content you are allowed to access; respect copyright and group rules.

## Troubleshooting

| Error | Fix |
|-------|-----|
| No user session | Run `python login.py` |
| Cannot access chat | Join the chat with the login account |
| FloodWait | Wait; increase `FLOODWAIT_MAX_RETRIES` if needed |
| File in DM from user session | Normal for large files (> ~20 MB or bot limit) |
| `CERTIFICATE_VERIFY_FAILED` (bot) | Fix system CA / AV HTTPS scanning; last resort `BOT_SSL_VERIFY=false` |
| Private `t.me/c/...` fails | Open chat in Telegram, restart bot, wait for dialog sync |
| Batch shows queue only | Restart bot after updates; ensure latest `handlers.py` fix is deployed |

## License

MIT License — see [LICENSE](LICENSE).

## Author

[RadinReanula](https://github.com/RadinReanula) — [Telegram-Leecher](https://github.com/RadinReanula/Telegram-Leecher)
