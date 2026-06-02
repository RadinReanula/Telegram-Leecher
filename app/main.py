import asyncio
import contextlib
import logging
import sys
from pathlib import Path

from aiogram import Bot, Dispatcher
from pyrogram import Client

from app.bot.handlers import create_router
from app.config import PROJECT_ROOT, get_settings
from app.downloader.peer_resolver import configure_peer_cache, save_peer_cache, sync_dialog_peers
from app.downloader.service import DownloadService
from app.network.bot_session import create_bot_session
from app.queue.manager import JobQueue

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


def _ensure_project_on_path() -> None:
    root = str(PROJECT_ROOT)
    if root not in sys.path:
        sys.path.insert(0, root)


async def _sync_dialogs_background(client: Client) -> None:
    try:
        await sync_dialog_peers(client)
    except Exception:
        logger.exception("Background dialog sync failed")


async def run() -> None:
    _ensure_project_on_path()
    settings = get_settings()
    settings.ensure_dirs()
    configure_peer_cache(settings.sessions_dir)

    session_file = Path(f"{settings.session_path}.session")
    if not session_file.exists():
        logger.error(
            "No user session at %s.session — run: python login.py",
            settings.session_path,
        )
        sys.exit(1)

    user_client = Client(
        name=settings.session_name,
        api_id=settings.api_id,
        api_hash=settings.api_hash,
        workdir=str(settings.sessions_dir),
        no_updates=True,
    )
    logger.info("Bot SSL verification: %s", settings.bot_ssl_verify)
    bot_session = create_bot_session(
        timeout=float(settings.bot_request_timeout_sec),
        verify_ssl=settings.bot_ssl_verify,
    )
    bot = Bot(token=settings.bot_token, session=bot_session)
    dispatcher = Dispatcher()
    download_service = DownloadService(user_client, bot, settings)
    job_queue = JobQueue(download_service, bot, settings)
    dispatcher.include_router(create_router(settings, download_service, job_queue))

    async with user_client:
        me = await user_client.get_me()
        logger.info("User session started: %s (id=%s)", me.first_name, me.id)

        sync_task: asyncio.Task[None] | None = None
        if settings.sync_dialogs_on_startup:
            if settings.sync_dialogs_in_background:
                logger.info("Peer cache syncing in background (bot starts immediately)…")
                sync_task = asyncio.create_task(_sync_dialogs_background(user_client))
            else:
                logger.info("Syncing dialogs into peer cache (first run may take a minute)…")
                await sync_dialog_peers(user_client)

        await job_queue.start()
        try:
            logger.info("Bot polling started")
            await dispatcher.start_polling(bot)
        finally:
            await job_queue.stop()
            if sync_task is not None and not sync_task.done():
                sync_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await sync_task
            save_peer_cache()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Stopped.")


if __name__ == "__main__":
    main()
