import json
import logging
from collections.abc import AsyncIterator
from pathlib import Path

from pyrogram import Client
from pyrogram.errors import ChannelPrivate, FloodWait, MessageIdInvalid, PeerIdInvalid, RPCError
from pyrogram.types import Dialog

from app.parser.telegram_links import ParsedLink

logger = logging.getLogger(__name__)

_peer_cache: dict[int, int] = {}
_cache_file: Path | None = None


def configure_peer_cache(sessions_dir: Path) -> None:
    global _cache_file
    _cache_file = sessions_dir / "peer_cache.json"
    _load_peer_cache()


def _load_peer_cache() -> None:
    global _peer_cache
    if _cache_file is None or not _cache_file.exists():
        return
    try:
        raw = json.loads(_cache_file.read_text(encoding="utf-8"))
        _peer_cache = {int(k): int(v) for k, v in raw.items()}
        logger.info("Loaded %s cached private peer(s)", len(_peer_cache))
    except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
        logger.warning("Could not load peer cache: %s", exc)
        _peer_cache = {}


def save_peer_cache() -> None:
    if _cache_file is None:
        return
    try:
        _cache_file.parent.mkdir(parents=True, exist_ok=True)
        payload = {str(k): v for k, v in _peer_cache.items()}
        _cache_file.write_text(json.dumps(payload), encoding="utf-8")
    except OSError as exc:
        logger.warning("Could not save peer cache: %s", exc)


def cache_private_peer(internal_id: int, chat_id: int) -> None:
    if _peer_cache.get(internal_id) == chat_id:
        return
    _peer_cache[internal_id] = chat_id
    save_peer_cache()


def get_cached_chat_id(internal_id: int) -> int | None:
    return _peer_cache.get(internal_id)


def chat_id_from_internal_id(internal_id: int) -> int:
    return int(f"-100{internal_id}")


def internal_id_from_chat_id(chat_id: int) -> int | None:
    text = str(chat_id)
    if text.startswith("-100"):
        return int(text[4:])
    return None


def chat_matches_internal_id(chat_id: int, internal_id: int) -> bool:
    return chat_id == chat_id_from_internal_id(internal_id)


async def _iter_dialogs(client: Client) -> AsyncIterator[Dialog]:
    """Yield dialogs, skipping entries Pyrogram cannot parse (e.g. reply into private channel)."""
    iterator = client.get_dialogs().__aiter__()
    while True:
        try:
            dialog = await iterator.__anext__()
        except StopAsyncIteration:
            return
        except FloodWait:
            raise
        except (ChannelPrivate, PeerIdInvalid, MessageIdInvalid) as exc:
            logger.warning("Skipping dialog during peer sync: %s", exc)
            continue
        except RPCError as exc:
            logger.warning("Skipping dialog during peer sync (RPC): %s", exc)
            continue
        yield dialog


async def sync_dialog_peers(client: Client) -> int:
    """Populate Pyrogram peer cache and local private-chat map from dialogs."""
    count = 0
    async for dialog in _iter_dialogs(client):
        count += 1
        chat = dialog.chat
        if chat is None:
            continue
        internal_id = internal_id_from_chat_id(chat.id)
        if internal_id is not None:
            cache_private_peer(internal_id, chat.id)
    logger.info("Synced %s dialog(s) into peer cache", count)
    return count


async def find_chat_in_dialogs(client: Client, internal_id: int) -> int | None:
    cached = get_cached_chat_id(internal_id)
    if cached is not None:
        return cached

    target = chat_id_from_internal_id(internal_id)
    async for dialog in _iter_dialogs(client):
        chat = dialog.chat
        if chat is None:
            continue
        if chat.id == target or chat_matches_internal_id(chat.id, internal_id):
            cache_private_peer(internal_id, chat.id)
            return chat.id
    return None


async def resolve_chat_id(client: Client, parsed: ParsedLink) -> int:
    if parsed.username:
        chat = await client.get_chat(parsed.username)
        return chat.id

    assert parsed.private_internal_id is not None
    internal_id = parsed.private_internal_id
    target = chat_id_from_internal_id(internal_id)

    cached = get_cached_chat_id(internal_id)
    if cached is not None:
        return cached

    try:
        await client.get_chat(target)
        cache_private_peer(internal_id, target)
        return target
    except (PeerIdInvalid, ChannelPrivate, ValueError):
        pass
    except FloodWait:
        raise
    except RPCError as exc:
        logger.debug("get_chat(%s) failed: %s", target, exc)

    resolved = await find_chat_in_dialogs(client, internal_id)
    if resolved is not None:
        logger.info("Resolved private chat via dialogs: internal=%s -> %s", internal_id, resolved)
        return resolved

    raise ValueError(
        "Cannot access this chat. Confirm you joined it with the same account as login.py, "
        "open the chat in your Telegram app once, restart the bot, then retry."
    )
