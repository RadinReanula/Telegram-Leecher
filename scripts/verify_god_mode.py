#!/usr/bin/env python3
"""Verify god-mode code is present and importable (run on VPS after git pull)."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

REQUIRED_SNIPPETS: dict[str, list[str]] = {
    "app/bot/handlers.py": [
        'Command("god"',
        "_handle_god_command",
        'mode="god"',
        '"pause"',
        '"continue"',
    ],
    "app/downloader/service.py": [
        "async def process_god_crawl",
        "_ensure_user_connected",
        "include_sender",
        "_sender_label",
    ],
    "app/queue/models.py": [
        'mode: Literal["link", "god"]',
        "def is_god",
        "PAUSED",
        "COOLDOWN",
    ],
    "app/queue/manager.py": [
        "user_has_active_god_job",
        "pause_god_job",
        "continue_god_job",
        "process_god_crawl",
    ],
    "app/config.py": [
        "god_delay_sec",
        "GOD_DELAY_SEC",
        "god_cooldown_every",
        "GOD_COOLDOWN_EVERY",
    ],
    "app/main.py": [
        "God mode active",
        'command="god"',
    ],
}


def main() -> int:
    errors: list[str] = []

    for rel_path, needles in REQUIRED_SNIPPETS.items():
        path = PROJECT_ROOT / rel_path
        if not path.is_file():
            errors.append(f"Missing file: {rel_path}")
            continue
        text = path.read_text(encoding="utf-8")
        for needle in needles:
            if needle not in text:
                errors.append(f"{rel_path}: missing {needle!r}")

    try:
        from app.config import get_settings
        from app.downloader.service import DownloadService
        from app.queue.models import DownloadJob

        settings = get_settings()
        job = DownloadJob(
            link="https://t.me/example/1",
            requester_id=1,
            bot_chat_id=1,
            status_chat_id=1,
            status_message_id=1,
            mode="god",
            god_direction="down",
            god_start_id=1,
        )
        if not job.is_god:
            errors.append("DownloadJob.is_god is False for mode='god'")
        if not hasattr(DownloadService, "process_god_crawl"):
            errors.append("DownloadService.process_god_crawl missing")
        if not hasattr(DownloadService, "_ensure_user_connected"):
            errors.append("DownloadService._ensure_user_connected missing")
        if settings.god_delay_sec < 0:
            errors.append("Invalid GOD_DELAY_SEC in settings")
        if settings.god_cooldown_every < 0 or settings.god_cooldown_sec < 0:
            errors.append("Invalid GOD_COOLDOWN_* in settings")
        from app.downloader.service import _sender_label, _is_connection_error

        assert _is_connection_error(BrokenPipeError())
        assert not _is_connection_error(ValueError("no media"))
        _ = _sender_label  # import smoke
    except Exception as exc:
        errors.append(f"Import/runtime check failed: {exc}")

    if errors:
        print("GOD MODE VERIFY: FAILED")
        for item in errors:
            print(f"  - {item}")
        return 1

    print("GOD MODE VERIFY: OK")
    print(f"  project: {PROJECT_ROOT}")
    print("  Restart required after pull: sudo systemctl restart telegram-leecher")
    print("  Then grep logs: grep -i 'God mode active' /var/log/telegram-leecher/app.log")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
