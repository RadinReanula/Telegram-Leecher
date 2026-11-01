import asyncio
import contextlib
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path

from aiogram import Bot
from aiogram.exceptions import TelegramNetworkError
from aiogram.types import FSInputFile
from pyrogram import Client
from pyrogram.errors import (
    ChannelPrivate,
    FloodWait,
    MessageIdInvalid,
    PeerIdInvalid,
    RPCError,
)
from pyrogram.types import Message as PyrogramMessage

from app.config import Settings
from app.downloader.peer_resolver import resolve_chat_id
from app.downloader.progress_bridge import DownloadProgressBridge
from app.downloader.results import DownloadOutcome, DownloadResult
from app.downloader.video_metadata import (
    build_telegram_video_kwargs,
    download_video_thumbnail,
    extract_video_params,
)
from app.network.bot_api_gate import BotApiGate
from app.parser.telegram_links import ParsedLink, parse_telegram_link
from app.queue.exceptions import JobCancelledError

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[str, int, str | None], None]
GodProgressCallback = Callable[[str, int, str | None, dict[str, int]], None]

_MIN_VIDEO_BYTES = 4096

_AUDIO_MIMES = frozenset(
    {
        "audio/mpeg",
        "audio/mp3",
        "audio/wav",
        "audio/x-wav",
        "audio/aac",
        "audio/flac",
        "audio/x-flac",
        "audio/ogg",
    }
)
_AUDIO_SUFFIXES = frozenset({".mp3", ".wav", ".aac", ".flac"})
_IMAGE_MIMES = frozenset(
    {
        "image/jpeg",
        "image/png",
        "image/gif",
        "image/webp",
    }
)
_IMAGE_SUFFIXES = frozenset({".jpg", ".jpeg", ".png", ".gif", ".webp"})
_VIDEO_SUFFIXES = frozenset({".mov", ".mkv", ".avi", ".webm", ".mp4"})


class DownloadService:
    def __init__(self, user_client: Client, bot: Bot, settings: Settings) -> None:
        self._user = user_client
        self._bot = bot
        self._settings = settings
        # (requester_id, chat_id, media_group_id) — short-lived album dedupe for batch jobs
        self._claimed_media_groups: dict[tuple[int, int, str], float] = {}
        self._bot_api_gate = BotApiGate(
            min_interval_sec=settings.god_bot_min_interval_sec,
            retry_max=settings.god_bot_retry_max,
        )
        # One delivery at a time so batch/album/god order stays stable.
        self._delivery_lock = asyncio.Lock()
        self._bot_user_id: int | None = None

    @property
    def bot_api_gate(self) -> BotApiGate:
        """Shared gate for god-mode Bot API sends and status edits."""
        return self._bot_api_gate

    async def process_link(
        self,
        requester_id: int,
        bot_chat_id: int,
        link: str,
        on_progress: ProgressCallback | None = None,
        *,
        expand_media_group: bool = True,
        parsed: ParsedLink | None = None,
        is_cancelled: Callable[[], bool] | None = None,
    ) -> DownloadResult:
        def report(stage: str, progress: int = 0, display_name: str | None = None) -> None:
            if on_progress:
                on_progress(stage, progress, display_name)

        def check_cancelled() -> None:
            if is_cancelled and is_cancelled():
                raise JobCancelledError

        if parsed is None:
            try:
                parsed = parse_telegram_link(link)
            except ValueError as exc:
                return DownloadResult(DownloadOutcome.SKIPPED, str(exc))

        check_cancelled()
        report("resolving", 0, _display_name_from_parsed(parsed))

        try:
            chat_id = await resolve_chat_id(self._user, parsed)
        except ValueError as exc:
            return DownloadResult(DownloadOutcome.FAILED, str(exc))

        fetch_result = await self._fetch_messages(
            chat_id,
            parsed.message_id,
            expand_media_group=expand_media_group,
            requester_id=requester_id,
        )
        if isinstance(fetch_result, DownloadResult):
            return fetch_result

        check_cancelled()
        messages = fetch_result
        media_messages = [message for message in messages if message.media]
        if not media_messages:
            return DownloadResult(
                DownloadOutcome.SKIPPED,
                "No downloadable media on this message.",
                display_name=_display_name_from_message(messages[0]) if messages else None,
            )

        display_name = _display_name_from_message(media_messages[0])
        delivered = await self._process_media_messages(
            media_messages,
            requester_id=requester_id,
            bot_chat_id=bot_chat_id,
            report=report,
            display_name=display_name,
            is_cancelled=is_cancelled,
            include_sender=True,
        )

        report("uploading", 100, display_name)

        if delivered == 1:
            msg = "Done — 1 file sent."
        else:
            msg = f"Done — {delivered} files sent (album)."
        return DownloadResult(DownloadOutcome.SUCCESS, msg, display_name=display_name)

    async def process_god_crawl(
        self,
        requester_id: int,
        bot_chat_id: int,
        link: str,
        *,
        direction: str,
        start_id: int,
        parsed: ParsedLink | None = None,
        on_progress: GodProgressCallback | None = None,
        is_cancelled: Callable[[], bool] | None = None,
        is_paused: Callable[[], bool] | None = None,
        cooldown_every: int | None = None,
        cooldown_sec: int | None = None,
        end_id: int | None = None,
    ) -> DownloadResult:
        def check_cancelled() -> None:
            if is_cancelled and is_cancelled():
                raise JobCancelledError

        if direction not in {"up", "down"}:
            return DownloadResult(DownloadOutcome.FAILED, "God direction must be up or down.")

        if parsed is None:
            try:
                parsed = parse_telegram_link(link)
            except ValueError as exc:
                return DownloadResult(DownloadOutcome.SKIPPED, str(exc))

        check_cancelled()
        display_name = f"God {direction} · {_display_name_from_parsed(parsed)}"

        def report(stage: str, progress: int, counters: dict[str, int]) -> None:
            if on_progress:
                on_progress(stage, progress, display_name, counters)

        try:
            chat_id = await resolve_chat_id(self._user, parsed)
        except ValueError as exc:
            return DownloadResult(DownloadOutcome.FAILED, str(exc), display_name=display_name)

        scanned = 0
        downloaded = 0
        skipped = 0
        missing = 0
        consecutive_miss = 0
        success_since_cooldown = 0
        seen_groups: set[str] = set()
        max_messages = self._settings.god_max_messages
        max_miss = self._settings.god_max_consecutive_miss
        delay = self._settings.god_delay_sec
        flood_extra = self._settings.god_floodwait_extra_sec
        skip_seen_groups = self._settings.god_skip_already_seen_groups
        every = (
            cooldown_every
            if cooldown_every is not None
            else self._settings.god_cooldown_every
        )
        cool_sec = (
            cooldown_sec
            if cooldown_sec is not None
            else self._settings.god_cooldown_sec
        )
        max_reconnect = self._settings.god_reconnect_max_retries
        max_item_retries = self._settings.god_item_max_retries
        reconnect_failures = 0
        item_attempts: dict[int, int] = {}
        missed_ids: list[int] = []
        visited: set[int] = set()
        bounded = end_id is not None

        def make_counters(
            *,
            current_id: int,
            cooldown_remaining_sec: int = 0,
        ) -> dict[str, int]:
            return {
                "scanned": scanned,
                "downloaded": downloaded,
                "skipped": skipped,
                "missing": missing,
                "current_id": current_id,
                "miss_streak": consecutive_miss,
                "success_since_cooldown": success_since_cooldown,
                "cooldown_remaining_sec": cooldown_remaining_sec,
            }

        async def wait_if_paused(current_id: int, progress: int) -> None:
            if not is_paused or not is_paused():
                return
            report("paused", progress, make_counters(current_id=current_id))
            while is_paused and is_paused():
                check_cancelled()
                await asyncio.sleep(1.0)
            check_cancelled()

        async def cancellable_sleep(
            seconds: float,
            *,
            current_id: int,
            progress: int,
            stage: str = "downloading",
        ) -> None:
            remaining = max(0.0, float(seconds))
            while remaining > 0:
                check_cancelled()
                await wait_if_paused(current_id, progress)
                if stage == "cooldown":
                    report(
                        "cooldown",
                        progress,
                        make_counters(
                            current_id=current_id,
                            cooldown_remaining_sec=int(remaining),
                        ),
                    )
                chunk = min(1.0, remaining)
                await asyncio.sleep(chunk)
                remaining -= chunk

        def stop_on_miss_streak() -> bool:
            return (not bounded) and direction == "up" and consecutive_miss >= max_miss

        def remember_attempt(current: int) -> int:
            item_attempts[current] = item_attempts.get(current, 0) + 1
            return item_attempts[current]

        msg_id = start_id
        while scanned < max_messages:
            check_cancelled()
            progress = min(99, int((scanned / max_messages) * 100)) if max_messages else 0
            await wait_if_paused(msg_id, progress)
            if _god_id_past_end(msg_id, direction, end_id):
                break
            if direction == "down" and msg_id < 1:
                break

            report("downloading", progress, make_counters(current_id=msg_id))

            try:
                message = await self._user.get_messages(chat_id, msg_id)
                reconnect_failures = 0
            except FloodWait as exc:
                logger.info(
                    "God crawl FloodWait %ss at msg %s — sleeping",
                    exc.value,
                    msg_id,
                )
                await cancellable_sleep(
                    exc.value + flood_extra,
                    current_id=msg_id,
                    progress=progress,
                )
                continue
            except (PeerIdInvalid, ChannelPrivate):
                return DownloadResult(
                    DownloadOutcome.FAILED,
                    "Cannot access this chat. Join with your authorized account first.",
                    display_name=display_name,
                )
            except MessageIdInvalid:
                missing += 1
                consecutive_miss += 1
                if msg_id not in visited:
                    scanned += 1
                    visited.add(msg_id)
                if stop_on_miss_streak():
                    break
                msg_id = msg_id + 1 if direction == "up" else msg_id - 1
                await cancellable_sleep(delay, current_id=msg_id, progress=progress)
                continue
            except Exception as exc:
                if not _is_connection_error(exc):
                    if isinstance(exc, RPCError):
                        attempt = remember_attempt(msg_id)
                        logger.warning(
                            "God crawl RPC error at msg %s (attempt %s/%s): %s",
                            msg_id,
                            attempt,
                            max_item_retries,
                            exc,
                        )
                        if attempt < max_item_retries:
                            await cancellable_sleep(delay, current_id=msg_id, progress=progress)
                            continue
                        missed_ids.append(msg_id)
                        item_attempts.pop(msg_id, None)
                        missing += 1
                        consecutive_miss += 1
                        if msg_id not in visited:
                            scanned += 1
                            visited.add(msg_id)
                        if stop_on_miss_streak():
                            break
                        msg_id = msg_id + 1 if direction == "up" else msg_id - 1
                        await cancellable_sleep(delay, current_id=msg_id, progress=progress)
                        continue
                    raise
                reconnect_failures += 1
                logger.warning(
                    "God crawl connection error at msg %s (%s/%s): %s",
                    msg_id,
                    reconnect_failures,
                    max_reconnect,
                    exc,
                )
                if reconnect_failures >= max_reconnect:
                    return DownloadResult(
                        DownloadOutcome.FAILED,
                        (
                            f"User session connection lost after {max_reconnect} reconnect "
                            f"attempts at msg {msg_id}."
                        ),
                        display_name=display_name,
                    )
                await self._ensure_user_connected()
                backoff = min(30, 5 * reconnect_failures)
                await cancellable_sleep(backoff, current_id=msg_id, progress=progress)
                continue

            if msg_id not in visited:
                scanned += 1
                visited.add(msg_id)

            if not message or getattr(message, "empty", False):
                missing += 1
                consecutive_miss += 1
                if stop_on_miss_streak():
                    break
                msg_id = msg_id + 1 if direction == "up" else msg_id - 1
                await cancellable_sleep(delay, current_id=msg_id, progress=progress)
                continue

            consecutive_miss = 0
            group_id = str(message.media_group_id) if message.media_group_id else None

            if group_id and skip_seen_groups and group_id in seen_groups:
                skipped += 1
                msg_id = msg_id + 1 if direction == "up" else msg_id - 1
                await cancellable_sleep(delay, current_id=msg_id, progress=progress)
                continue

            try:
                if message.media_group_id:
                    try:
                        media_messages = await self._user.get_media_group(chat_id, msg_id)
                    except FloodWait as exc:
                        await cancellable_sleep(
                            exc.value + flood_extra,
                            current_id=msg_id,
                            progress=progress,
                        )
                        continue
                    except Exception as exc:
                        if _is_connection_error(exc):
                            raise
                        logger.warning(
                            "God crawl media group fetch failed at msg %s: %s",
                            msg_id,
                            exc,
                        )
                        media_messages = [message] if message.media else []
                    else:
                        if group_id and skip_seen_groups:
                            seen_groups.add(group_id)
                else:
                    media_messages = [message] if message.media else []
            except FloodWait as exc:
                await cancellable_sleep(
                    exc.value + flood_extra,
                    current_id=msg_id,
                    progress=progress,
                )
                continue
            except Exception as exc:
                if not _is_connection_error(exc):
                    raise
                reconnect_failures += 1
                logger.warning(
                    "God crawl connection error fetching media at msg %s (%s/%s): %s",
                    msg_id,
                    reconnect_failures,
                    max_reconnect,
                    exc,
                )
                if reconnect_failures >= max_reconnect:
                    return DownloadResult(
                        DownloadOutcome.FAILED,
                        (
                            f"User session connection lost after {max_reconnect} reconnect "
                            f"attempts at msg {msg_id}."
                        ),
                        display_name=display_name,
                    )
                await self._ensure_user_connected()
                backoff = min(30, 5 * reconnect_failures)
                await cancellable_sleep(backoff, current_id=msg_id, progress=progress)
                continue

            media_messages = [m for m in media_messages if m.media]
            if not media_messages:
                skipped += 1
                msg_id = msg_id + 1 if direction == "up" else msg_id - 1
                await cancellable_sleep(delay, current_id=msg_id, progress=progress)
                continue

            check_cancelled()
            await wait_if_paused(msg_id, progress)
            item_name = _display_name_from_message(media_messages[0])

            def item_report(stage: str, pct: int, _name: str | None = None) -> None:
                report(stage, min(99, progress), make_counters(current_id=msg_id))

            try:
                delivered = await self._process_media_messages(
                    media_messages,
                    requester_id=requester_id,
                    bot_chat_id=bot_chat_id,
                    report=item_report,
                    display_name=item_name,
                    is_cancelled=is_cancelled,
                    include_sender=False,
                    pace_bot_api=True,
                )
                downloaded += delivered
                if delivered == 0:
                    skipped += 1
                else:
                    success_since_cooldown += delivered
                    if every > 0 and success_since_cooldown >= every and cool_sec > 0:
                        logger.info(
                            "God crawl auto-cooldown %ss after %s successful sends",
                            cool_sec,
                            success_since_cooldown,
                        )
                        await cancellable_sleep(
                            cool_sec,
                            current_id=msg_id,
                            progress=progress,
                            stage="cooldown",
                        )
                        success_since_cooldown = 0
                        report(
                            "downloading",
                            progress,
                            make_counters(current_id=msg_id),
                        )
            except FloodWait as exc:
                logger.info("God crawl FloodWait during media %ss", exc.value)
                await cancellable_sleep(
                    exc.value + flood_extra,
                    current_id=msg_id,
                    progress=progress,
                )
                continue
            except JobCancelledError:
                raise
            except Exception as exc:
                if _is_connection_error(exc):
                    reconnect_failures += 1
                    logger.warning(
                        "God crawl connection error processing msg %s (%s/%s): %s",
                        msg_id,
                        reconnect_failures,
                        max_reconnect,
                        exc,
                    )
                    if reconnect_failures >= max_reconnect:
                        return DownloadResult(
                            DownloadOutcome.FAILED,
                            (
                                f"User session connection lost after {max_reconnect} reconnect "
                                f"attempts at msg {msg_id}."
                            ),
                            display_name=display_name,
                        )
                    await self._ensure_user_connected()
                    backoff = min(30, 5 * reconnect_failures)
                    await cancellable_sleep(backoff, current_id=msg_id, progress=progress)
                    continue
                attempt = remember_attempt(msg_id)
                logger.exception(
                    "God crawl failed processing msg %s (attempt %s/%s)",
                    msg_id,
                    attempt,
                    max_item_retries,
                )
                if attempt < max_item_retries:
                    await cancellable_sleep(delay, current_id=msg_id, progress=progress)
                    continue
                missed_ids.append(msg_id)
                item_attempts.pop(msg_id, None)
                skipped += 1

            item_attempts.pop(msg_id, None)
            msg_id = msg_id + 1 if direction == "up" else msg_id - 1
            await cancellable_sleep(delay, current_id=msg_id, progress=progress)

        missed_note = ""
        if missed_ids:
            shown = ", ".join(str(item) for item in missed_ids[:20])
            extra = "" if len(missed_ids) <= 20 else f" (+{len(missed_ids) - 20} more)"
            missed_note = f" Unrecovered ids: {shown}{extra}."
        final = (
            f"God {direction} done — scanned {scanned}, sent {downloaded}, "
            f"skipped {skipped}, missing {missing}.{missed_note}"
        )
        report("uploading", 100, make_counters(current_id=msg_id))
        return DownloadResult(DownloadOutcome.SUCCESS, final, display_name=display_name)

    async def _ensure_user_connected(self) -> None:
        """Best-effort reconnect for a broken Pyrogram user session."""
        try:
            if not self._user.is_connected:
                logger.info("Reconnecting Pyrogram user session…")
                await self._user.connect()
            # Touch the API to verify the socket is alive.
            await self._user.get_me()
            logger.info("Pyrogram user session reconnect OK")
        except Exception:
            logger.exception("Pyrogram user session reconnect failed; retrying connect")
            with contextlib.suppress(Exception):
                if self._user.is_connected:
                    await self._user.disconnect()
            await asyncio.sleep(1.0)
            await self._user.connect()
            await self._user.get_me()
            logger.info("Pyrogram user session reconnect OK after reset")

    async def _process_media_messages(
        self,
        media_messages: list[PyrogramMessage],
        *,
        requester_id: int,
        bot_chat_id: int,
        report: Callable[[str, int, str | None], None],
        display_name: str | None,
        is_cancelled: Callable[[], bool] | None = None,
        include_sender: bool = False,
        pace_bot_api: bool = False,
    ) -> int:
        total = len(media_messages)
        # God mode always sequential — overlapping album uploads burst Bot API sends.
        if total > 1 and self._settings.album_pipeline and not pace_bot_api:
            return await self._process_album_pipelined(
                media_messages,
                requester_id=requester_id,
                bot_chat_id=bot_chat_id,
                report=report,
                display_name=display_name,
                is_cancelled=is_cancelled,
                include_sender=include_sender,
                pace_bot_api=pace_bot_api,
            )
        return await self._process_album_sequential(
            media_messages,
            requester_id=requester_id,
            bot_chat_id=bot_chat_id,
            report=report,
            display_name=display_name,
            is_cancelled=is_cancelled,
            include_sender=include_sender,
            pace_bot_api=pace_bot_api,
        )

    async def _process_album_sequential(
        self,
        media_messages: list[PyrogramMessage],
        *,
        requester_id: int,
        bot_chat_id: int,
        report: Callable[[str, int, str | None], None],
        display_name: str | None,
        is_cancelled: Callable[[], bool] | None = None,
        include_sender: bool = False,
        pace_bot_api: bool = False,
    ) -> int:
        delivered = 0
        total = len(media_messages)
        for index, message in enumerate(media_messages):
            if is_cancelled and is_cancelled():
                raise JobCancelledError
            pct = int((index / total) * 90) if total > 1 else 0
            report("downloading", pct, display_name)
            if await self._process_one_media_item(
                message,
                requester_id=requester_id,
                bot_chat_id=bot_chat_id,
                report=report,
                display_name=display_name,
                include_sender=include_sender,
                pace_bot_api=pace_bot_api,
            ):
                delivered += 1
        return delivered

    async def _process_album_pipelined(
        self,
        media_messages: list[PyrogramMessage],
        *,
        requester_id: int,
        bot_chat_id: int,
        report: Callable[[str, int, str | None], None],
        display_name: str | None,
        is_cancelled: Callable[[], bool] | None = None,
        include_sender: bool = False,
        pace_bot_api: bool = False,
    ) -> int:
        total = len(media_messages)
        delivered = 0
        upload_task: asyncio.Task[bool] | None = None

        for index, message in enumerate(media_messages):
            if is_cancelled and is_cancelled():
                raise JobCancelledError
            pct = int((index / total) * 90)
            report("downloading", pct, display_name)
            path = await self._download_message(
                message,
                on_byte_progress=lambda cur, tot: report(
                    "downloading",
                    5 + int(85 * cur / tot) if tot else pct,
                    display_name,
                ),
            )
            _validate_downloaded_file(path, message)

            if upload_task is not None:
                if await upload_task:
                    delivered += 1

            upload_task = asyncio.create_task(
                self._upload_media_item(
                    message,
                    path,
                    requester_id=requester_id,
                    bot_chat_id=bot_chat_id,
                    report=report,
                    display_name=display_name,
                    include_sender=include_sender,
                    pace_bot_api=pace_bot_api,
                )
            )

        if upload_task is not None and await upload_task:
            delivered += 1
        return delivered

    async def _process_one_media_item(
        self,
        message: PyrogramMessage,
        *,
        requester_id: int,
        bot_chat_id: int,
        report: Callable[[str, int, str | None], None],
        display_name: str | None,
        include_sender: bool = False,
        pace_bot_api: bool = False,
    ) -> bool:
        path = await self._download_message(
            message,
            on_byte_progress=lambda cur, tot: report(
                "downloading",
                5 + int(85 * cur / tot) if tot else 0,
                display_name,
            ),
        )
        _validate_downloaded_file(path, message)
        return await self._upload_media_item(
            message,
            path,
            requester_id=requester_id,
            bot_chat_id=bot_chat_id,
            report=report,
            display_name=display_name,
            include_sender=include_sender,
            pace_bot_api=pace_bot_api,
        )

    async def _upload_media_item(
        self,
        message: PyrogramMessage,
        path: Path,
        *,
        requester_id: int,
        bot_chat_id: int,
        report: Callable[[str, int, str | None], None],
        display_name: str | None,
        include_sender: bool = False,
        pace_bot_api: bool = False,
    ) -> bool:
        thumb_path: Path | None = None
        try:
            report("uploading", 95, display_name)
            if _should_attach_thumbnail(message):
                thumb_path = await download_video_thumbnail(
                    self._user, message, self._settings.tmp_dir
                )
            await self._deliver_file(
                requester_id,
                bot_chat_id,
                path,
                message,
                thumb_path=thumb_path,
                include_sender=include_sender,
                pace_bot_api=pace_bot_api,
            )
            return True
        finally:
            _safe_unlink(path)
            if thumb_path:
                _safe_unlink(thumb_path)

    async def session_status(self) -> str:
        try:
            me = await self._user.get_me()
        except Exception as exc:  # noqa: BLE001
            return f"User session not ready: {exc}"
        name = " ".join(part for part in (me.first_name, me.last_name) if part)
        return f"Logged in as {name} (@{me.username or 'no username'}, id={me.id})."

    async def _fetch_messages(
        self,
        chat_id: int,
        message_id: int,
        *,
        expand_media_group: bool,
        requester_id: int | None = None,
    ) -> list[PyrogramMessage] | DownloadResult:
        try:
            message = await self._user.get_messages(chat_id, message_id)
        except FloodWait:
            raise
        except (PeerIdInvalid, ChannelPrivate):
            return DownloadResult(
                DownloadOutcome.FAILED,
                "Cannot access this chat. Join the group/channel with your authorized account first.",
            )
        except ValueError as exc:
            if "peer id invalid" in str(exc).lower():
                return DownloadResult(
                    DownloadOutcome.FAILED,
                    "Cannot access this chat. Restart the bot (syncs your chat list) and retry.",
                )
            raise
        except MessageIdInvalid:
            return DownloadResult(DownloadOutcome.SKIPPED, "Message not found — check the link.")
        except RPCError as exc:
            return DownloadResult(DownloadOutcome.FAILED, f"Telegram error: {exc}")

        if not message:
            return DownloadResult(DownloadOutcome.SKIPPED, "Message not found.")

        if message.media_group_id:
            if not expand_media_group:
                if message.media:
                    return [message]
                return DownloadResult(
                    DownloadOutcome.SKIPPED,
                    "No media on this message (album caption-only anchor).",
                )

            group_key = str(message.media_group_id)
            if requester_id is not None and not self._claim_media_group(
                requester_id, chat_id, group_key
            ):
                return DownloadResult(
                    DownloadOutcome.SKIPPED,
                    "Album already downloaded in this batch.",
                    display_name=_display_name_from_message(message),
                )

            try:
                return await self._user.get_media_group(chat_id, message_id)
            except MessageIdInvalid:
                self._release_media_group(requester_id, chat_id, group_key)
                return DownloadResult(
                    DownloadOutcome.SKIPPED,
                    "Message not found — check the link.",
                )
            except RPCError as exc:
                self._release_media_group(requester_id, chat_id, group_key)
                return DownloadResult(DownloadOutcome.FAILED, f"Telegram error: {exc}")

        return [message]

    def _claim_media_group(self, requester_id: int, chat_id: int, media_group_id: str) -> bool:
        """Return True if this album is newly claimed; False if already handled."""
        self._prune_claimed_media_groups()
        key = (requester_id, chat_id, media_group_id)
        if key in self._claimed_media_groups:
            return False
        self._claimed_media_groups[key] = time.monotonic()
        return True

    def _release_media_group(
        self,
        requester_id: int | None,
        chat_id: int,
        media_group_id: str,
    ) -> None:
        if requester_id is None:
            return
        self._claimed_media_groups.pop((requester_id, chat_id, media_group_id), None)

    def _prune_claimed_media_groups(self, *, max_age_sec: float = 3600.0) -> None:
        if len(self._claimed_media_groups) < 200:
            return
        now = time.monotonic()
        stale = [k for k, ts in self._claimed_media_groups.items() if now - ts > max_age_sec]
        for key in stale:
            del self._claimed_media_groups[key]
        if len(self._claimed_media_groups) > 500:
            ordered = sorted(self._claimed_media_groups.items(), key=lambda item: item[1])
            for key, _ in ordered[: len(ordered) // 2]:
                del self._claimed_media_groups[key]

    async def _download_message(
        self,
        message: PyrogramMessage,
        *,
        on_byte_progress: Callable[[int, int], None] | None = None,
    ) -> Path:
        suffix = _media_suffix(message)
        target = self._settings.tmp_dir / f"{uuid.uuid4().hex}{suffix}"
        progress_bridge: DownloadProgressBridge | None = None
        progress_cb = None

        if (
            on_byte_progress is not None
            and self._settings.download_progress_enabled
        ):
            loop = asyncio.get_running_loop()
            progress_bridge = DownloadProgressBridge(loop, on_byte_progress)
            progress_bridge.start()
            progress_cb = progress_bridge.callback

        try:
            result = await self._user.download_media(
                message,
                file_name=str(target),
                progress=progress_cb,
            )
        except FloodWait:
            raise
        except RPCError as exc:
            raise ValueError(f"Download failed: {exc}") from exc
        finally:
            if progress_bridge is not None:
                await progress_bridge.stop()

        if not result:
            raise ValueError("Download failed — no file returned.")
        return Path(result)

    async def _deliver_file(
        self,
        requester_id: int,
        bot_chat_id: int,
        path: Path,
        message: PyrogramMessage,
        *,
        thumb_path: Path | None = None,
        include_sender: bool = False,
        pace_bot_api: bool = False,
    ) -> None:
        async with self._delivery_lock:
            await self._deliver_file_locked(
                requester_id,
                bot_chat_id,
                path,
                message,
                thumb_path=thumb_path,
                include_sender=include_sender,
                pace_bot_api=pace_bot_api,
            )

    async def _deliver_file_locked(
        self,
        requester_id: int,
        bot_chat_id: int,
        path: Path,
        message: PyrogramMessage,
        *,
        thumb_path: Path | None = None,
        include_sender: bool = False,
        pace_bot_api: bool = False,
    ) -> None:
        size = path.stat().st_size
        caption = self._build_caption(message, include_sender=include_sender)
        timeout = self._settings.bot_request_timeout_sec

        if size > self._settings.bot_max_file_bytes:
            await self._send_via_user(
                requester_id,
                path,
                message,
                caption,
                reason="exceeds the bot upload limit (~50 MB)",
                thumb_path=thumb_path,
            )
            return

        if size > self._settings.bot_upload_threshold_bytes:
            await self._send_via_user(
                requester_id,
                path,
                message,
                caption,
                reason="is large — using user session for a reliable upload",
                thumb_path=thumb_path,
            )
            return

        try:
            await self._send_via_bot(
                bot_chat_id,
                path,
                message,
                caption,
                request_timeout=timeout,
                thumb_path=thumb_path,
                pace_bot_api=pace_bot_api,
            )
        except (TelegramNetworkError, asyncio.TimeoutError, TimeoutError) as exc:
            logger.warning("Bot upload failed (%s), falling back to user session", exc)
            await self._send_via_user(
                requester_id,
                path,
                message,
                caption,
                reason="bot upload timed out — sent via user session instead",
                thumb_path=thumb_path,
            )

    async def _send_via_bot(
        self,
        bot_chat_id: int,
        path: Path,
        message: PyrogramMessage,
        caption: str | None,
        *,
        request_timeout: int | None = None,
        thumb_path: Path | None = None,
        pace_bot_api: bool = False,
    ) -> None:
        input_file = FSInputFile(path)
        kwargs: dict = {}
        if request_timeout is not None:
            kwargs["request_timeout"] = request_timeout

        async def send_typed() -> None:
            await self._send_via_bot_typed(
                bot_chat_id,
                input_file,
                message,
                caption,
                kwargs=kwargs,
                thumb_path=thumb_path,
            )

        async def send_document() -> None:
            await self._bot.send_document(
                bot_chat_id, input_file, caption=caption, **kwargs
            )

        try:
            if pace_bot_api:
                await self._bot_api_gate.run(send_typed)
            else:
                await send_typed()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Typed bot send failed (%s), falling back to document", exc)
            if pace_bot_api:
                await self._bot_api_gate.run(send_document)
            else:
                await send_document()

    async def _send_via_bot_typed(
        self,
        bot_chat_id: int,
        input_file: FSInputFile,
        message: PyrogramMessage,
        caption: str | None,
        *,
        kwargs: dict,
        thumb_path: Path | None = None,
    ) -> None:
        if message.video_note:
            note_kwargs: dict = {}
            if message.video_note.duration:
                note_kwargs["duration"] = message.video_note.duration
            if message.video_note.length:
                note_kwargs["length"] = message.video_note.length
            await self._bot.send_video_note(
                bot_chat_id, input_file, **kwargs, **note_kwargs
            )
            return

        if message.animation or _is_gif_document(message):
            anim_kwargs = build_telegram_video_kwargs(extract_video_params(message))
            if thumb_path:
                anim_kwargs["thumbnail"] = FSInputFile(thumb_path)
            await self._bot.send_animation(
                bot_chat_id, input_file, caption=caption, **kwargs, **anim_kwargs
            )
            return

        if _is_streamable_video(message):
            video_kwargs = build_telegram_video_kwargs(extract_video_params(message))
            if thumb_path:
                video_kwargs["thumbnail"] = FSInputFile(thumb_path)
            await self._bot.send_video(
                bot_chat_id, input_file, caption=caption, **kwargs, **video_kwargs
            )
            return

        if message.voice:
            await self._bot.send_voice(bot_chat_id, input_file, caption=caption, **kwargs)
            return

        if message.audio or _is_audio_document(message):
            await self._bot.send_audio(bot_chat_id, input_file, caption=caption, **kwargs)
            return

        if message.photo or _is_image_document(message):
            await self._bot.send_photo(bot_chat_id, input_file, caption=caption, **kwargs)
            return

        await self._bot.send_document(bot_chat_id, input_file, caption=caption, **kwargs)

    async def _cached_bot_user_id(self) -> int:
        if self._bot_user_id is None:
            me = await self._bot.get_me()
            self._bot_user_id = int(me.id)
        return self._bot_user_id

    async def _user_call_with_retry(self, label: str, factory: Callable[[], Awaitable[object]]):
        """Retry a user-session call on FloodWait or a dropped connection."""
        attempts = self._settings.god_item_max_retries
        last_exc: BaseException | None = None
        for attempt in range(1, attempts + 1):
            try:
                return await factory()
            except FloodWait as exc:
                last_exc = exc
                wait_for = exc.value + self._settings.god_floodwait_extra_sec
                logger.info(
                    "%s FloodWait %ss (attempt %s/%s)",
                    label,
                    exc.value,
                    attempt,
                    attempts,
                )
                await asyncio.sleep(wait_for)
            except Exception as exc:
                if not _is_connection_error(exc) or attempt >= attempts:
                    raise
                last_exc = exc
                logger.warning(
                    "%s connection error (attempt %s/%s): %s",
                    label,
                    attempt,
                    attempts,
                    exc,
                )
                await self._ensure_user_connected()
                await asyncio.sleep(self._settings.god_delay_sec)
        raise RuntimeError(f"{label} failed after {attempts} attempts") from last_exc

    async def _forward_saved_to_bot(self, saved_message_id: int) -> None:
        """Copy a Saved Messages upload into the bot chat, in order."""
        bot_user_id = await self._cached_bot_user_id()

        async def _forward():
            return await self._user.forward_messages(
                bot_user_id,
                "me",
                saved_message_id,
            )

        await self._user_call_with_retry("Forward saved file to bot", _forward)
        logger.info(
            "Forwarded Saved Messages id %s to bot %s",
            saved_message_id,
            bot_user_id,
        )

    async def _send_via_user(
        self,
        requester_id: int,
        path: Path,
        message: PyrogramMessage,
        caption: str | None,
        *,
        reason: str,
        thumb_path: Path | None = None,
    ) -> PyrogramMessage:
        del requester_id  # large files always land in Saved Messages first
        note = f"File {reason}."
        full_caption = f"{note}\n\n{caption}" if caption else note

        async def _save() -> PyrogramMessage:
            try:
                return await self._send_via_user_typed(
                    "me",
                    path,
                    message,
                    full_caption,
                    thumb_path=thumb_path,
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Typed user send failed (%s), falling back to document", exc)
                return await self._user.send_document(
                    "me", document=str(path), caption=full_caption
                )

        saved = await self._user_call_with_retry("Save large file", _save)
        if saved is None or getattr(saved, "id", None) is None:
            raise RuntimeError("Saved Messages upload did not return a message id.")
        await self._forward_saved_to_bot(int(saved.id))
        return saved

    async def _send_via_user_typed(
        self,
        chat_id: int | str,
        path: Path,
        message: PyrogramMessage,
        full_caption: str,
        *,
        thumb_path: Path | None = None,
    ) -> PyrogramMessage:
        if message.video_note:
            note_kwargs: dict = {"caption": full_caption}
            if message.video_note.duration:
                note_kwargs["duration"] = message.video_note.duration
            if message.video_note.length:
                note_kwargs["length"] = message.video_note.length
            return await self._user.send_video_note(chat_id, video_note=str(path), **note_kwargs)

        if message.animation or _is_gif_document(message):
            params = extract_video_params(message)
            anim_kwargs = build_telegram_video_kwargs(params)
            if thumb_path:
                anim_kwargs["thumb"] = str(thumb_path)
            if params and params.file_name:
                anim_kwargs["file_name"] = params.file_name
            return await self._user.send_animation(
                chat_id, animation=str(path), caption=full_caption, **anim_kwargs
            )

        if _is_streamable_video(message):
            params = extract_video_params(message)
            video_kwargs = build_telegram_video_kwargs(params)
            if thumb_path:
                video_kwargs["thumb"] = str(thumb_path)
            if params and params.file_name:
                video_kwargs["file_name"] = params.file_name
            return await self._user.send_video(
                chat_id, video=str(path), caption=full_caption, **video_kwargs
            )

        if message.voice:
            return await self._user.send_voice(chat_id, voice=str(path), caption=full_caption)

        if message.audio or _is_audio_document(message):
            return await self._user.send_audio(chat_id, audio=str(path), caption=full_caption)

        if message.photo or _is_image_document(message):
            return await self._user.send_photo(chat_id, photo=str(path), caption=full_caption)

        return await self._user.send_document(chat_id, document=str(path), caption=full_caption)

    @staticmethod
    def _build_caption(
        message: PyrogramMessage,
        *,
        include_sender: bool = False,
    ) -> str | None:
        parts: list[str] = []
        if message.caption:
            parts.append(message.caption)
        if include_sender:
            sender = _sender_label(message)
            if sender:
                parts.append(f"From: {sender}")
        if message.chat and message.chat.title:
            parts.append(f"Source: {message.chat.title}")
        if not parts:
            return None
        return "\n\n".join(parts)


def _sender_label(message: PyrogramMessage) -> str | None:
    user = message.from_user
    if user is not None:
        if user.username:
            return f"@{user.username}"
        return f"id {user.id}"

    sender_chat = message.sender_chat
    if sender_chat is not None:
        if sender_chat.username:
            return f"@{sender_chat.username}"
        if sender_chat.title:
            return sender_chat.title
        return f"id {sender_chat.id}"
    return None


def _god_id_past_end(msg_id: int, direction: str, end_id: int | None) -> bool:
    if end_id is None:
        return False
    if direction == "up":
        return msg_id > end_id
    return msg_id < end_id


def _is_connection_error(exc: BaseException) -> bool:
    if isinstance(exc, (BrokenPipeError, ConnectionError, ConnectionResetError, TimeoutError)):
        return True
    if isinstance(exc, OSError):
        errno = getattr(exc, "errno", None)
        # 32 Broken pipe, 104 Connection reset, 110 Timed out;
        # Windows: 10053/10054/10060
        if errno in {32, 104, 110, 10053, 10054, 10060}:
            return True
    text = str(exc).lower()
    return any(
        token in text
        for token in (
            "broken pipe",
            "connection reset",
            "connection aborted",
            "not connected",
            "server closed the connection",
            "connection lost",
            "timed out",
        )
    )


def _document_mime(message: PyrogramMessage) -> str | None:
    document = message.document
    if document and document.mime_type:
        return document.mime_type.lower()
    return None


def _document_name(message: PyrogramMessage) -> str | None:
    document = message.document
    if document and document.file_name:
        return document.file_name
    return None


def _document_suffix(message: PyrogramMessage) -> str:
    name = _document_name(message)
    if name:
        return Path(name).suffix.lower()
    return ""


def _is_audio_document(message: PyrogramMessage) -> bool:
    if not message.document:
        return False
    mime = _document_mime(message)
    if mime and (mime in _AUDIO_MIMES or mime.startswith("audio/")):
        return True
    return _document_suffix(message) in _AUDIO_SUFFIXES


def _is_gif_document(message: PyrogramMessage) -> bool:
    if not message.document:
        return False
    mime = _document_mime(message)
    if mime == "image/gif":
        return True
    return _document_suffix(message) == ".gif"


def _is_image_document(message: PyrogramMessage) -> bool:
    if not message.document or message.sticker:
        return False
    if _is_gif_document(message):
        return False
    mime = _document_mime(message)
    if mime and mime in _IMAGE_MIMES:
        return True
    return _document_suffix(message) in _IMAGE_SUFFIXES - {".gif"}


def _is_streamable_video(message: PyrogramMessage) -> bool:
    if message.video:
        return True
    if not message.document:
        return False
    if _is_audio_document(message) or _is_image_document(message) or _is_gif_document(message):
        return False
    mime = _document_mime(message)
    if mime and mime.startswith("video/"):
        return True
    return _document_suffix(message) in _VIDEO_SUFFIXES


def _should_attach_thumbnail(message: PyrogramMessage) -> bool:
    return bool(
        message.video
        or message.animation
        or _is_gif_document(message)
        or _is_streamable_video(message)
    )


def _media_suffix(message: PyrogramMessage) -> str:
    if message.video or message.video_note:
        name = message.video.file_name if message.video and message.video.file_name else None
        if name:
            return Path(name).suffix or ".mp4"
        return ".mp4"
    if message.animation:
        if message.animation.file_name:
            return Path(message.animation.file_name).suffix or ".mp4"
        return ".mp4"
    if message.photo:
        return ".jpg"
    if message.audio:
        return message.audio.file_name and Path(message.audio.file_name).suffix or ".mp3"
    if message.voice:
        return ".ogg"
    if message.document:
        if message.document.file_name:
            return Path(message.document.file_name).suffix or ""
        mime = _document_mime(message)
        if mime and mime.startswith("video/"):
            return ".mp4"
        if mime and (mime in _AUDIO_MIMES or mime.startswith("audio/")):
            return ".mp3"
        if mime and mime in _IMAGE_MIMES:
            if mime == "image/png":
                return ".png"
            if mime == "image/gif":
                return ".gif"
            if mime == "image/webp":
                return ".webp"
            return ".jpg"
    if message.sticker:
        return ".webp"
    return ""


def _safe_unlink(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except PermissionError:
        logger.warning("Could not delete temp file (still in use): %s", path.name)
    except OSError as exc:
        logger.warning("Could not delete temp file %s: %s", path.name, exc)


def _validate_downloaded_file(path: Path, message: PyrogramMessage) -> None:
    if not path.exists():
        raise ValueError("Download failed — file missing.")
    size = path.stat().st_size
    if size == 0:
        raise ValueError("Download failed — empty file.")
    if _should_attach_thumbnail(message) and size < _MIN_VIDEO_BYTES:
        raise ValueError(
            "Download incomplete — video file too small (possible thumbnail only)."
        )


def _display_name_from_parsed(parsed: ParsedLink) -> str:
    if parsed.username:
        return f"@{parsed.username} / msg {parsed.message_id}"
    if parsed.private_internal_id is not None:
        return f"Private chat {parsed.private_internal_id} / msg {parsed.message_id}"
    return f"msg {parsed.message_id}"


def _display_name_from_message(message: PyrogramMessage) -> str:
    if message.chat and message.chat.title:
        title = message.chat.title
        if len(title) > 40:
            title = title[:37] + "..."
        return f"{title} / msg {message.id}"
    if message.chat and message.chat.username:
        return f"@{message.chat.username} / msg {message.id}"
    return f"msg {message.id}"
