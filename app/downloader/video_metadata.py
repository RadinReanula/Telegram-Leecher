import logging
import uuid
from dataclasses import dataclass
from pathlib import Path

from pyrogram import Client
from pyrogram.types import Message as PyrogramMessage

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class VideoSendParams:
    duration: int
    width: int
    height: int
    file_name: str | None = None


def extract_video_params(message: PyrogramMessage) -> VideoSendParams | None:
    if message.video:
        video = message.video
        return VideoSendParams(
            duration=int(video.duration or 0),
            width=int(video.width or 0),
            height=int(video.height or 0),
            file_name=video.file_name,
        )
    if message.animation:
        anim = message.animation
        return VideoSendParams(
            duration=int(anim.duration or 0),
            width=int(anim.width or 0),
            height=int(anim.height or 0),
            file_name=anim.file_name,
        )
    if message.video_note:
        note = message.video_note
        length = int(note.length or 0)
        return VideoSendParams(
            duration=int(note.duration or 0),
            width=length,
            height=length,
        )
    document = message.document
    if document and document.mime_type and document.mime_type.startswith("video/"):
        return VideoSendParams(
            duration=0,
            width=0,
            height=0,
            file_name=document.file_name,
        )
    return None


def build_telegram_video_kwargs(params: VideoSendParams | None) -> dict:
    if not params:
        return {"supports_streaming": True}
    kwargs: dict = {"supports_streaming": True}
    if params.duration > 0:
        kwargs["duration"] = params.duration
    if params.width > 0:
        kwargs["width"] = params.width
    if params.height > 0:
        kwargs["height"] = params.height
    return kwargs


async def download_video_thumbnail(
    client: Client,
    message: PyrogramMessage,
    tmp_dir: Path,
) -> Path | None:
    thumbs = None
    if message.video and message.video.thumbs:
        thumbs = message.video.thumbs
    elif message.animation and message.animation.thumbs:
        thumbs = message.animation.thumbs
    elif (
        message.document
        and message.document.mime_type
        and message.document.mime_type.startswith("video/")
        and message.document.thumbs
    ):
        thumbs = message.document.thumbs

    if not thumbs:
        return None

    thumb = max(thumbs, key=lambda item: item.width * item.height)
    target = tmp_dir / f"{uuid.uuid4().hex}_thumb.jpg"
    try:
        result = await client.download_media(thumb, file_name=str(target))
    except Exception as exc:  # noqa: BLE001
        logger.debug("Thumbnail download skipped: %s", exc)
        return None

    if not result:
        return None
    path = Path(result)
    if path.exists() and path.stat().st_size > 0:
        return path
    return None
