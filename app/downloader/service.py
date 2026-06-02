import asyncio
import logging
import uuid
from collections.abc import Callable
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
from app.parser.telegram_links import ParsedLink, parse_telegram_link

logger = logging.getLogger(__name__)

ProgressCallback = Callable[[str, int, str | None], None]

_MIN_VIDEO_BYTES = 4096


class DownloadService:
    def __init__(self, user_client: Client, bot: Bot, settings: Settings) -> None:
        self._user = user_client
        self._bot = bot
        self._settings = settings

    async def process_link(
        self,
        requester_id: int,
        bot_chat_id: int,
        link: str,
        on_progress: ProgressCallback | None = None,
        *,
        expand_media_group: bool = True,
        parsed: ParsedLink | None = None,
    ) -> DownloadResult:
        def report(stage: str, progress: int = 0, display_name: str | None = None) -> None:
            if on_progress:
                on_progress(stage, progress, display_name)

        if parsed is None:
            try:
                parsed = parse_telegram_link(link)
            except ValueError as exc:
                return DownloadResult(DownloadOutcome.SKIPPED, str(exc))

        report("resolving", 0, _display_name_from_parsed(parsed))

        try:
            chat_id = await resolve_chat_id(self._user, parsed)
        except ValueError as exc:
            return DownloadResult(DownloadOutcome.FAILED, str(exc))

        fetch_result = await self._fetch_messages(
            chat_id,
            parsed.message_id,
            expand_media_group=expand_media_group,
        )
        if isinstance(fetch_result, DownloadResult):
            return fetch_result

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
        )

        report("uploading", 100, display_name)

        if delivered == 1:
            msg = "Done — 1 file sent."
        else:
            msg = f"Done — {delivered} files sent (album)."
        return DownloadResult(DownloadOutcome.SUCCESS, msg, display_name=display_name)

    async def _process_media_messages(
        self,
        media_messages: list[PyrogramMessage],
        *,
        requester_id: int,
        bot_chat_id: int,
        report: Callable[[str, int, str | None], None],
        display_name: str | None,
    ) -> int:
        total = len(media_messages)
        if total > 1 and self._settings.album_pipeline:
            return await self._process_album_pipelined(
                media_messages,
                requester_id=requester_id,
                bot_chat_id=bot_chat_id,
                report=report,
                display_name=display_name,
            )
        return await self._process_album_sequential(
            media_messages,
            requester_id=requester_id,
            bot_chat_id=bot_chat_id,
            report=report,
            display_name=display_name,
        )

    async def _process_album_sequential(
        self,
        media_messages: list[PyrogramMessage],
        *,
        requester_id: int,
        bot_chat_id: int,
        report: Callable[[str, int, str | None], None],
        display_name: str | None,
    ) -> int:
        delivered = 0
        total = len(media_messages)
        for index, message in enumerate(media_messages):
            pct = int((index / total) * 90) if total > 1 else 0
            report("downloading", pct, display_name)
            if await self._process_one_media_item(
                message,
                requester_id=requester_id,
                bot_chat_id=bot_chat_id,
                report=report,
                display_name=display_name,
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
    ) -> int:
        total = len(media_messages)
        delivered = 0
        upload_task: asyncio.Task[bool] | None = None

        for index, message in enumerate(media_messages):
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
    ) -> bool:
        thumb_path: Path | None = None
        try:
            report("uploading", 95, display_name)
            if _should_attach_thumbnail(message):
                thumb_path = await download_video_thumbnail(
                    self._user, message, self._settings.tmp_dir
                )
            await self._deliver_file(
                requester_id, bot_chat_id, path, message, thumb_path=thumb_path
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

            if message.media:
                try:
                    return await self._user.get_media_group(chat_id, message_id)
                except MessageIdInvalid:
                    return DownloadResult(
                        DownloadOutcome.SKIPPED,
                        "Message not found — check the link.",
                    )
                except RPCError as exc:
                    return DownloadResult(DownloadOutcome.FAILED, f"Telegram error: {exc}")
            try:
                return await self._user.get_media_group(chat_id, message_id)
            except MessageIdInvalid:
                return DownloadResult(DownloadOutcome.SKIPPED, "Message not found — check the link.")
            except RPCError as exc:
                return DownloadResult(DownloadOutcome.FAILED, f"Telegram error: {exc}")

        return [message]

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
    ) -> None:
        size = path.stat().st_size
        caption = self._build_caption(message)
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
    ) -> None:
        input_file = FSInputFile(path)
        kwargs: dict = {}
        if request_timeout is not None:
            kwargs["request_timeout"] = request_timeout

        if message.video_note:
            note_kwargs: dict = {}
            if message.video_note.duration:
                note_kwargs["duration"] = message.video_note.duration
            if message.video_note.length:
                note_kwargs["length"] = message.video_note.length
            await self._bot.send_video_note(
                bot_chat_id, input_file, **kwargs, **note_kwargs
            )
        elif message.animation:
            anim_kwargs = build_telegram_video_kwargs(extract_video_params(message))
            if thumb_path:
                anim_kwargs["thumbnail"] = FSInputFile(thumb_path)
            await self._bot.send_animation(
                bot_chat_id, input_file, caption=caption, **kwargs, **anim_kwargs
            )
        elif _is_streamable_video(message):
            video_kwargs = build_telegram_video_kwargs(extract_video_params(message))
            if thumb_path:
                video_kwargs["thumbnail"] = FSInputFile(thumb_path)
            await self._bot.send_video(
                bot_chat_id, input_file, caption=caption, **kwargs, **video_kwargs
            )
        elif message.voice:
            await self._bot.send_voice(bot_chat_id, input_file, caption=caption, **kwargs)
        elif message.audio:
            await self._bot.send_audio(bot_chat_id, input_file, caption=caption, **kwargs)
        elif message.photo:
            await self._bot.send_photo(bot_chat_id, input_file, caption=caption, **kwargs)
        else:
            await self._bot.send_document(bot_chat_id, input_file, caption=caption, **kwargs)

    async def _send_via_user(
        self,
        requester_id: int,
        path: Path,
        message: PyrogramMessage,
        caption: str | None,
        *,
        reason: str,
        thumb_path: Path | None = None,
    ) -> None:
        note = f"File {reason}."
        full_caption = f"{note}\n\n{caption}" if caption else note

        if message.video_note:
            note_kwargs: dict = {"caption": full_caption}
            if message.video_note.duration:
                note_kwargs["duration"] = message.video_note.duration
            if message.video_note.length:
                note_kwargs["length"] = message.video_note.length
            await self._user.send_video_note(requester_id, video_note=str(path), **note_kwargs)
            return

        if message.animation:
            params = extract_video_params(message)
            anim_kwargs = build_telegram_video_kwargs(params)
            if thumb_path:
                anim_kwargs["thumb"] = str(thumb_path)
            if params and params.file_name:
                anim_kwargs["file_name"] = params.file_name
            await self._user.send_animation(
                requester_id, animation=str(path), caption=full_caption, **anim_kwargs
            )
            return

        if _is_streamable_video(message):
            params = extract_video_params(message)
            video_kwargs = build_telegram_video_kwargs(params)
            if thumb_path:
                video_kwargs["thumb"] = str(thumb_path)
            if params and params.file_name:
                video_kwargs["file_name"] = params.file_name
            await self._user.send_video(
                requester_id, video=str(path), caption=full_caption, **video_kwargs
            )
            return

        await self._user.send_document(requester_id, document=str(path), caption=full_caption)

    @staticmethod
    def _build_caption(message: PyrogramMessage) -> str | None:
        parts: list[str] = []
        if message.caption:
            parts.append(message.caption)
        if message.chat and message.chat.title:
            parts.append(f"Source: {message.chat.title}")
        if not parts:
            return None
        return "\n\n".join(parts)


def _is_streamable_video(message: PyrogramMessage) -> bool:
    if message.video:
        return True
    document = message.document
    return bool(document and document.mime_type and document.mime_type.startswith("video/"))


def _should_attach_thumbnail(message: PyrogramMessage) -> bool:
    return bool(
        message.video or message.animation or _is_streamable_video(message)
    )


def _media_suffix(message: PyrogramMessage) -> str:
    if message.video or message.video_note:
        return ".mp4"
    if message.animation:
        return ".mp4"
    if message.photo:
        return ".jpg"
    if message.audio:
        return message.audio.file_name and Path(message.audio.file_name).suffix or ".mp3"
    if message.voice:
        return ".ogg"
    if message.document:
        if message.document.mime_type and message.document.mime_type.startswith("video/"):
            if message.document.file_name:
                return Path(message.document.file_name).suffix or ".mp4"
            return ".mp4"
        if message.document.file_name:
            return Path(message.document.file_name).suffix or ""
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
