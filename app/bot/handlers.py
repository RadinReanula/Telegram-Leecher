import asyncio
import logging
import re
import time
from typing import Literal

from aiogram import F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import Message

from app.config import Settings
from app.downloader.service import DownloadService
from app.parser.telegram_links import ParsedLink, extract_telegram_links
from app.queue.manager import JobQueue
from app.queue.models import DownloadJob
from app.queue.status_format import format_job_status, format_status_list

logger = logging.getLogger(__name__)

_LINK_HINT = re.compile(r"t(?:elegram)?\.me/", re.IGNORECASE)
_GOD_PENDING_TTL_SEC = 300.0
_GOD_USAGE = (
    "God mode — crawl message IDs in a chat and download media.\n\n"
    "Usage:\n"
    "• /god up <link> — walk toward newer messages\n"
    "• /god down <link> — walk toward older messages (down to 1)\n"
    "• /god up  or  /god down — then paste one link in the next message\n\n"
    "Direction is required. Use /stop to cancel a running god crawl.\n"
    "Keep QUEUE_WORKERS=1 while using god mode to reduce FloodWait risk."
)


def create_router(
    settings: Settings,
    download_service: DownloadService,
    job_queue: JobQueue,
) -> Router:
    router = Router()
    allowed_ids = settings.allowed_user_id_set
    # user_id -> (direction, expires_at)
    god_pending: dict[int, tuple[Literal["up", "down"], float]] = {}

    def is_allowed(user_id: int) -> bool:
        if not allowed_ids:
            return True
        return user_id in allowed_ids

    def clear_god_pending(user_id: int) -> None:
        god_pending.pop(user_id, None)

    def get_god_pending(user_id: int) -> Literal["up", "down"] | None:
        entry = god_pending.get(user_id)
        if not entry:
            return None
        direction, expires_at = entry
        if time.time() > expires_at:
            clear_god_pending(user_id)
            return None
        return direction

    def set_god_pending(user_id: int, direction: Literal["up", "down"]) -> None:
        god_pending[user_id] = (direction, time.time() + _GOD_PENDING_TTL_SEC)

    @router.message(CommandStart())
    async def cmd_start(message: Message) -> None:
        if not is_allowed(message.from_user.id):
            await message.answer("You are not authorized to use this bot.")
            return

        await message.answer(
            "Telegram media downloader (Option B)\n\n"
            "1. Your operator account must already be in the target group/channel.\n"
            "2. Paste one or more message links (newline or space separated).\n"
            "   • https://t.me/channelname/123\n"
            "   • https://t.me/c/1867392134/42 (private)\n\n"
            "Commands:\n"
            "/status — your jobs summary\n"
            "/stop — cancel your queued and running downloads\n"
            "/god up|down [link] — crawl chat media by message ID\n"
            "/job <id> — full details for one job\n"
            "/queue — global queue summary\n"
            "/auth — check user session status\n"
            "/help — short help"
        )

    @router.message(Command("help"))
    async def cmd_help(message: Message) -> None:
        if not is_allowed(message.from_user.id):
            await message.answer("You are not authorized to use this bot.")
            return
        await message.answer(
            "Paste one or more t.me message links in a single message to enqueue downloads. "
            "Each link gets its own status message with progress and timestamps. "
            "Albums expand fully (duplicate album links in one batch are skipped). "
            f"Maximum {settings.max_links_per_message} links per message. "
            "Use /god up|down [link] to crawl many messages in a chat. "
            "Use /job <id> for full job details. "
            "Use /stop to cancel all your active downloads (including god mode)."
        )

    @router.message(Command("auth"))
    async def cmd_auth(message: Message) -> None:
        if not is_allowed(message.from_user.id):
            await message.answer("You are not authorized to use this bot.")
            return
        status = await download_service.session_status()
        await message.answer(status)

    @router.message(Command("status"))
    async def cmd_status(message: Message) -> None:
        if not is_allowed(message.from_user.id):
            await message.answer("You are not authorized to use this bot.")
            return

        jobs = job_queue.jobs_for_user(message.from_user.id, include_finished=True)[:15]
        await message.answer(format_status_list(jobs))

    @router.message(Command("stop"))
    async def cmd_stop(message: Message) -> None:
        if not is_allowed(message.from_user.id):
            await message.answer("You are not authorized to use this bot.")
            return

        clear_god_pending(message.from_user.id)
        cancelled_pending, cancelled_running = await job_queue.cancel_jobs_for_user(
            message.from_user.id
        )
        total = cancelled_pending + cancelled_running
        if total == 0:
            await message.answer("No queued or running jobs to stop.")
            return

        parts: list[str] = []
        if cancelled_pending:
            parts.append(f"{cancelled_pending} queued")
        if cancelled_running:
            parts.append(f"{cancelled_running} running")
        summary = " and ".join(parts)
        await message.answer(
            f"Stopped {summary} job(s).\n"
            "The bot is still running — you can paste new links anytime."
        )

    @router.message(Command("job"))
    async def cmd_job(message: Message) -> None:
        if not is_allowed(message.from_user.id):
            await message.answer("You are not authorized to use this bot.")
            return

        parts = (message.text or "").split(maxsplit=1)
        if len(parts) < 2:
            await message.answer("Usage: /job <job_id>\nExample: /job a1b2c3d4")
            return

        job_id = parts[1].strip().strip("`")
        job = job_queue.get_job(job_id)
        if not job or job.requester_id != message.from_user.id:
            await message.answer(f"Job `{job_id}` not found.", parse_mode="Markdown")
            return

        await message.answer(format_job_status(job))

    @router.message(Command("queue"))
    async def cmd_queue(message: Message) -> None:
        if not is_allowed(message.from_user.id):
            await message.answer("You are not authorized to use this bot.")
            return

        pending, running, total = job_queue.queue_snapshot()
        await message.answer(
            f"Queue: {pending} waiting, {running} running "
            f"({settings.queue_workers} worker(s)). "
            f"{total} jobs tracked in memory."
        )

    @router.message(Command("god"))
    async def cmd_god(message: Message) -> None:
        if not is_allowed(message.from_user.id):
            await message.answer("You are not authorized to use this bot.")
            return

        user_id = message.from_user.id
        if job_queue.user_has_active_god_job(user_id):
            await message.answer("God mode already running — use /stop to cancel it first.")
            return

        parts = (message.text or "").split(maxsplit=2)
        # /god
        if len(parts) == 1:
            await message.answer(_GOD_USAGE)
            return

        direction_raw = parts[1].strip().lower()
        if direction_raw not in {"up", "down"}:
            await message.answer(
                "Direction required: up or down.\n\n" + _GOD_USAGE
            )
            return

        direction: Literal["up", "down"] = "up" if direction_raw == "up" else "down"

        # /god up  or  /god down  — wait for next link
        if len(parts) == 2:
            set_god_pending(user_id, direction)
            await message.answer(
                f"God mode ready ({direction}). "
                "Paste exactly one t.me message link in your next message "
                f"(expires in {int(_GOD_PENDING_TTL_SEC // 60)} minutes)."
            )
            return

        # /god up <link>  or  /god down <link>
        link_text = parts[2].strip()
        extraction = extract_telegram_links(link_text)
        if len(extraction.links) != 1:
            await message.answer(
                "Provide exactly one valid t.me message link after the direction.\n\n"
                + _GOD_USAGE
            )
            return

        clear_god_pending(user_id)
        parsed = extraction.parsed[0] if extraction.parsed else None
        await _enqueue_god_job(
            message,
            extraction.links[0],
            direction=direction,
            parsed=parsed,
        )

    def _queued_status_text(
        job: DownloadJob, position: int, running: int, batch_part: str = ""
    ) -> str:
        if position == 1 and running == 0:
            return f"⏳ [{job.id}]{batch_part} Queued — starting soon…"
        return f"⏳ [{job.id}]{batch_part} Queued — position #{position} in line."

    async def _enqueue_god_job(
        message: Message,
        link: str,
        *,
        direction: Literal["up", "down"],
        parsed: ParsedLink | None,
    ) -> None:
        if parsed is None:
            await message.answer("Could not parse that link.")
            return

        status_message = await message.answer(
            f"Queuing god {direction} from msg {parsed.message_id}…"
        )
        job = DownloadJob(
            link=link,
            parsed=parsed,
            requester_id=message.from_user.id,
            bot_chat_id=message.chat.id,
            status_chat_id=message.chat.id,
            status_message_id=status_message.message_id,
            mode="god",
            god_direction=direction,
            god_start_id=parsed.message_id,
            display_name=f"God {direction} · msg {parsed.message_id}",
        )

        try:
            _, position, running = await job_queue.enqueue(job)
            await status_message.edit_text(_queued_status_text(job, position, running))
        except ValueError as exc:
            await status_message.edit_text(str(exc))

    async def _enqueue_single_link(
        message: Message,
        link: str,
        parsed: ParsedLink | None = None,
    ) -> None:
        status_message = await message.answer("Queuing download…")
        job = DownloadJob(
            link=link,
            parsed=parsed,
            requester_id=message.from_user.id,
            bot_chat_id=message.chat.id,
            status_chat_id=message.chat.id,
            status_message_id=status_message.message_id,
        )

        try:
            _, position, running = await job_queue.enqueue(job)
            await status_message.edit_text(_queued_status_text(job, position, running))
        except ValueError as exc:
            await status_message.edit_text(str(exc))

    async def _enqueue_batch_links(
        message: Message,
        links: list[str],
        parsed_links: list[ParsedLink],
        invalid_count: int,
    ) -> None:
        total = len(links)
        summary = f"Queued {total} download(s)."
        if invalid_count:
            summary += f" ({invalid_count} invalid link(s) ignored.)"
        await message.answer(summary)

        async def _queuing_status(index: int):
            return await message.answer(f"Queuing ({index}/{total})…")

        try:
            status_messages = await asyncio.gather(
                *(_queuing_status(index) for index in range(1, total + 1))
            )
        except Exception:
            logger.exception("Failed to create batch status messages")
            await message.answer(
                "Could not create status messages for all links. "
                "Try fewer links per message or send them one at a time."
            )
            return

        for index, (link, status_message) in enumerate(
            zip(links, status_messages, strict=True),
            start=1,
        ):
            parsed = parsed_links[index - 1] if index - 1 < len(parsed_links) else None
            job = DownloadJob(
                link=link,
                parsed=parsed,
                requester_id=message.from_user.id,
                bot_chat_id=message.chat.id,
                status_chat_id=message.chat.id,
                status_message_id=status_message.message_id,
                batch_index=index,
                batch_total=total,
            )

            try:
                _, position, running = await job_queue.enqueue(job, batch_burst=True)
                batch_part = f" ({index}/{total})"
                await status_message.edit_text(
                    _queued_status_text(job, position, running, batch_part)
                )
            except ValueError as exc:
                await status_message.edit_text(str(exc))

    @router.message(F.text)
    async def on_text(message: Message) -> None:
        if not message.from_user or not message.text:
            return
        if not is_allowed(message.from_user.id):
            await message.answer("You are not authorized to use this bot.")
            return

        user_id = message.from_user.id
        pending_direction = get_god_pending(user_id)

        # Two-step god mode: direction already set, waiting for a link
        if pending_direction is not None:
            if not _LINK_HINT.search(message.text):
                await message.answer(
                    f"God mode ({pending_direction}) is waiting for a link. "
                    "Paste one t.me message link, or /stop to cancel."
                )
                return

            extraction = extract_telegram_links(message.text)
            if len(extraction.links) != 1:
                clear_god_pending(user_id)
                await message.answer(
                    "God mode needs exactly one valid link. Pending god session cleared.\n\n"
                    + _GOD_USAGE
                )
                return

            if job_queue.user_has_active_god_job(user_id):
                clear_god_pending(user_id)
                await message.answer("God mode already running — use /stop to cancel it first.")
                return

            clear_god_pending(user_id)
            parsed = extraction.parsed[0] if extraction.parsed else None
            await _enqueue_god_job(
                message,
                extraction.links[0],
                direction=pending_direction,
                parsed=parsed,
            )
            return

        if not _LINK_HINT.search(message.text):
            return

        extraction = extract_telegram_links(message.text)
        if not extraction.links:
            await message.answer("Could not find any valid t.me message links in your message.")
            return

        if len(extraction.links) > settings.max_links_per_message:
            await message.answer(
                f"Too many links ({len(extraction.links)}). "
                f"Maximum is {settings.max_links_per_message} per message."
            )
            return

        if len(extraction.links) == 1:
            parsed = extraction.parsed[0] if extraction.parsed else None
            await _enqueue_single_link(message, extraction.links[0], parsed=parsed)
        else:
            await _enqueue_batch_links(
                message,
                extraction.links,
                extraction.parsed,
                extraction.invalid_count,
            )

    return router
