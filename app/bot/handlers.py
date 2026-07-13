import asyncio
import logging
import re
import time
from dataclasses import dataclass
from typing import Literal

from aiogram import F, Router
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import Message

from app.config import Settings
from app.downloader.service import DownloadService
from app.parser.telegram_links import ParsedLink, extract_telegram_links
from app.queue.manager import JobQueue
from app.queue.models import DownloadJob
from app.queue.status_format import format_job_status, format_status_list

logger = logging.getLogger(__name__)

_LINK_HINT = re.compile(r"t(?:elegram)?\.me/", re.IGNORECASE)
_GOD_CMD_PREFIX = re.compile(r"^/god(?:@\w+)?(?:\s|$)", re.IGNORECASE)
_GOD_CMD_ARGS = re.compile(r"^/god(?:@\w+)?\s*(.*)$", re.IGNORECASE | re.DOTALL)
_GOD_PENDING_TTL_SEC = 300.0
_GOD_USAGE = (
    "God mode — crawl message IDs in a chat and download media.\n\n"
    "Usage:\n"
    "• /god up|down [every] [cooldown_sec] [link]\n"
    "• /god up|down [every] [cooldown_sec] — then paste one link\n"
    "• /god pause — soft-pause the active god crawl\n"
    "• /god continue — resume a paused god crawl\n\n"
    "Examples:\n"
    "• /god down https://t.me/c/123/456\n"
    "• /god down 150 180 https://t.me/c/123/456\n"
    "• /god down 100 60  (then paste a link)\n\n"
    "every = successful sends before auto-cooldown (default from .env).\n"
    "cooldown_sec = auto-cooldown length in seconds.\n"
    "Use /stop to cancel a running god crawl.\n"
    "Keep QUEUE_WORKERS=1 while using god mode to reduce FloodWait risk."
)


@dataclass(slots=True)
class _GodPending:
    direction: Literal["up", "down"]
    expires_at: float
    cooldown_every: int | None = None
    cooldown_sec: int | None = None


def create_router(
    settings: Settings,
    download_service: DownloadService,
    job_queue: JobQueue,
) -> Router:
    router = Router()
    allowed_ids = settings.allowed_user_id_set
    god_pending: dict[int, _GodPending] = {}

    def is_allowed(user_id: int) -> bool:
        if not allowed_ids:
            return True
        return user_id in allowed_ids

    def clear_god_pending(user_id: int) -> None:
        god_pending.pop(user_id, None)

    def get_god_pending(user_id: int) -> _GodPending | None:
        entry = god_pending.get(user_id)
        if not entry:
            return None
        if time.time() > entry.expires_at:
            clear_god_pending(user_id)
            return None
        return entry

    def set_god_pending(
        user_id: int,
        direction: Literal["up", "down"],
        *,
        cooldown_every: int | None = None,
        cooldown_sec: int | None = None,
    ) -> None:
        god_pending[user_id] = _GodPending(
            direction=direction,
            expires_at=time.time() + _GOD_PENDING_TTL_SEC,
            cooldown_every=cooldown_every,
            cooldown_sec=cooldown_sec,
        )

    def _cooldown_summary(
        cooldown_every: int | None,
        cooldown_sec: int | None,
    ) -> str:
        every = (
            cooldown_every
            if cooldown_every is not None
            else settings.god_cooldown_every
        )
        cool = (
            cooldown_sec
            if cooldown_sec is not None
            else settings.god_cooldown_sec
        )
        if every <= 0 or cool <= 0:
            return "auto-cooldown disabled"
        return f"auto-cooldown every {every} sends for {cool}s"

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
            "/god up|down|pause|continue — crawl chat media by message ID\n"
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
            "Use /god up|down [every] [cooldown_sec] [link] to crawl many messages. "
            "Use /god pause and /god continue to soft-pause/resume. "
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

    def _parse_god_start_args(
        rest: str,
    ) -> tuple[int | None, int | None, str] | None:
        """Parse optional every/cooldown_sec ints then remainder text.

        Returns (every, cooldown_sec, remainder) or None on invalid ints.
        """
        tokens = rest.split()
        every: int | None = None
        cool: int | None = None
        idx = 0
        while idx < len(tokens) and idx < 2 and tokens[idx].isdigit():
            value = int(tokens[idx])
            if idx == 0:
                every = value
            else:
                cool = value
            idx += 1
        # Reject negative-looking tokens that aren't pure digits (already handled).
        # If a non-link token remains that looks numeric-invalid, fail later via link parse.
        remainder = " ".join(tokens[idx:]).strip()
        return every, cool, remainder

    async def _handle_god_command(message: Message, args_text: str) -> None:
        if not message.from_user:
            return
        if not is_allowed(message.from_user.id):
            await message.answer("You are not authorized to use this bot.")
            return

        user_id = message.from_user.id
        args_text = args_text.strip()
        logger.info("God command from user %s args=%r", user_id, args_text)

        if not args_text:
            await message.answer(_GOD_USAGE)
            return

        tokens = args_text.split(maxsplit=1)
        verb = tokens[0].lower()

        if verb in {"pause", "continue"}:
            if len(tokens) > 1 and tokens[1].strip():
                await message.answer(
                    f"/god {verb} takes no extra arguments.\n\n" + _GOD_USAGE
                )
                return
            if verb == "pause":
                result = job_queue.pause_god_job(user_id)
                if result == "none":
                    await message.answer("No active god crawl to pause.")
                elif result == "already_paused":
                    await message.answer(
                        "God crawl is already paused. "
                        "/god continue to resume, /stop to cancel."
                    )
                else:
                    await message.answer(
                        "God crawl paused. /god continue to resume, /stop to cancel."
                    )
                return

            result = job_queue.continue_god_job(user_id)
            if result == "none":
                await message.answer("No paused god crawl to continue.")
            elif result == "already_running":
                await message.answer("God crawl is already running.")
            else:
                await message.answer("God crawl continuing…")
            return

        if verb not in {"up", "down"}:
            await message.answer(
                "Use /god up|down|pause|continue.\n\n" + _GOD_USAGE
            )
            return

        if job_queue.user_has_active_god_job(user_id):
            await message.answer(
                "God mode already running — use /god pause, /god continue, "
                "or /stop to cancel it first."
            )
            return

        direction: Literal["up", "down"] = "up" if verb == "up" else "down"
        rest = tokens[1].strip() if len(tokens) > 1 else ""
        parsed_args = _parse_god_start_args(rest)
        if parsed_args is None:
            await message.answer("Invalid cooldown numbers.\n\n" + _GOD_USAGE)
            return
        cooldown_every, cooldown_sec, remainder = parsed_args

        if not remainder:
            set_god_pending(
                user_id,
                direction,
                cooldown_every=cooldown_every,
                cooldown_sec=cooldown_sec,
            )
            await message.answer(
                f"God mode ready ({direction}, {_cooldown_summary(cooldown_every, cooldown_sec)}). "
                "Paste exactly one t.me message link in your next message "
                f"(expires in {int(_GOD_PENDING_TTL_SEC // 60)} minutes)."
            )
            return

        extraction = extract_telegram_links(remainder)
        if len(extraction.links) != 1:
            await message.answer(
                "Provide exactly one valid t.me message link after the direction "
                "(and optional every/cooldown_sec).\n\n"
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
            cooldown_every=cooldown_every,
            cooldown_sec=cooldown_sec,
        )

    @router.message(Command("god", ignore_case=True))
    async def cmd_god(message: Message, command: CommandObject) -> None:
        await _handle_god_command(message, command.args or "")

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
        cooldown_every: int | None = None,
        cooldown_sec: int | None = None,
    ) -> None:
        if parsed is None:
            await message.answer("Could not parse that link.")
            return

        status_message = await message.answer(
            f"Queuing god {direction} from msg {parsed.message_id} "
            f"({_cooldown_summary(cooldown_every, cooldown_sec)})…"
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
            god_cooldown_every=cooldown_every,
            god_cooldown_sec=cooldown_sec,
            display_name=f"God {direction} · msg {parsed.message_id}",
        )

        try:
            _, position, running = await job_queue.enqueue(job)
            await status_message.edit_text(_queued_status_text(job, position, running))
            logger.info(
                "God job %s queued at position %s for user %s",
                job.id,
                position,
                job.requester_id,
            )
        except ValueError as exc:
            await status_message.edit_text(str(exc))
            logger.warning("God enqueue failed for user %s: %s", job.requester_id, exc)

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
        text = message.text

        # Fallback when Telegram does not tag /god as a bot_command entity
        if text and _GOD_CMD_PREFIX.match(text):
            match = _GOD_CMD_ARGS.match(text)
            args_text = match.group(1).strip() if match else ""
            await _handle_god_command(message, args_text)
            return

        pending = get_god_pending(user_id)

        # Two-step god mode: direction already set, waiting for a link
        if pending is not None:
            if not _LINK_HINT.search(message.text):
                await message.answer(
                    f"God mode ({pending.direction}, "
                    f"{_cooldown_summary(pending.cooldown_every, pending.cooldown_sec)}) "
                    "is waiting for a link. "
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
                await message.answer(
                    "God mode already running — use /god pause, /god continue, "
                    "or /stop to cancel it first."
                )
                return

            clear_god_pending(user_id)
            parsed = extraction.parsed[0] if extraction.parsed else None
            await _enqueue_god_job(
                message,
                extraction.links[0],
                direction=pending.direction,
                parsed=parsed,
                cooldown_every=pending.cooldown_every,
                cooldown_sec=pending.cooldown_sec,
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
