"""
One-time (or re-) authorization for the Pyrogram user session.
Run from project root: python login.py
"""
import asyncio
import sys
from pathlib import Path

from pyrogram import Client

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import get_settings  # noqa: E402


async def main() -> None:
    settings = get_settings()
    settings.ensure_dirs()

    print("Telegram user login")
    print(f"Session will be saved under: {settings.sessions_dir}")
    print("You need API_ID and API_HASH from https://my.telegram.org/apps\n")

    async with Client(
        name=settings.session_name,
        api_id=settings.api_id,
        api_hash=settings.api_hash,
        workdir=str(settings.sessions_dir),
    ) as app:
        me = await app.get_me()
        print(f"\nSuccess — logged in as {me.first_name} (id={me.id}).")
        print("You can now run: python -m app.main")


if __name__ == "__main__":
    asyncio.run(main())
