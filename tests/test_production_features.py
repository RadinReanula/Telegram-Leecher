"""Local checks for ordered delivery, god retries, and god range."""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from pyrogram.errors import RPCError

from app.downloader.service import DownloadService
from app.parser.telegram_links import ParsedLink


def _settings(**overrides):
    base = dict(
        god_bot_min_interval_sec=0,
        god_bot_retry_max=1,
        god_delay_sec=0,
        god_floodwait_extra_sec=0,
        god_max_consecutive_miss=25,
        god_max_messages=20,
        god_skip_already_seen_groups=True,
        god_cooldown_every=0,
        god_cooldown_sec=0,
        god_reconnect_max_retries=5,
        god_item_max_retries=3,
        bot_max_file_bytes=1000,
        bot_upload_threshold_bytes=40,
        bot_request_timeout_sec=30,
        download_progress_enabled=False,
        tmp_dir=Path("."),
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _media(message_id: int, group: str | None = None):
    return SimpleNamespace(
        id=message_id,
        media=object(),
        media_group_id=group,
        empty=False,
        chat=None,
        caption=None,
        video_note=None,
        animation=None,
        document=None,
        voice=None,
        audio=None,
        photo=None,
        video=None,
        sticker=None,
    )


class _User:
    def __init__(self, order: list[str]) -> None:
        self.order = order
        self.is_connected = True
        self._next_id = 1

    async def send_document(self, chat_id, document=None, caption=None):
        self.order.append(f"save:{chat_id}:{Path(document).name}")
        message_id = self._next_id
        self._next_id += 1
        return SimpleNamespace(id=message_id)

    async def forward_messages(self, chat_id, from_chat_id, message_ids):
        self.order.append(f"forward:{chat_id}:{from_chat_id}:{message_ids}")
        return []


class _Bot:
    def __init__(self) -> None:
        self.id = 881

    async def get_me(self):
        return SimpleNamespace(id=self.id)


class DeliveryOrderTests(unittest.IsolatedAsyncioTestCase):
    async def test_small_large_small_stays_in_order(self) -> None:
        order: list[str] = []
        user = _User(order)
        bot = _Bot()
        service = DownloadService(user, bot, _settings())  # type: ignore[arg-type]
        with tempfile.TemporaryDirectory() as tmp:
            small = Path(tmp) / "file001.bin"
            large = Path(tmp) / "file002.bin"
            small2 = Path(tmp) / "file003.bin"
            small.write_bytes(b"a" * 10)
            large.write_bytes(b"b" * 50)
            small2.write_bytes(b"c" * 10)
            source = _media(1)

            async def slow_bot(*_args, **_kwargs):
                order.append("bot-start")
                await asyncio.sleep(0.02)
                order.append("bot-end")

            service._send_via_bot = slow_bot  # type: ignore[method-assign]
            first = asyncio.create_task(service._deliver_file(1, 1, small, source))
            await asyncio.sleep(0)
            second = asyncio.create_task(service._deliver_file(1, 1, large, source))
            await asyncio.sleep(0)
            third = asyncio.create_task(service._deliver_file(1, 1, small2, source))
            await asyncio.gather(first, second, third)

        self.assertEqual(
            order,
            [
                "bot-start",
                "bot-end",
                "save:me:file002.bin",
                "forward:881:me:1",
                "bot-start",
                "bot-end",
            ],
        )


class GodCrawlTests(unittest.IsolatedAsyncioTestCase):
    def _service(self, **settings):
        user = SimpleNamespace(is_connected=True)
        bot = SimpleNamespace()
        service = DownloadService(user, bot, _settings(**settings))  # type: ignore[arg-type]
        return service, user

    async def test_rpc_error_retries_same_id(self) -> None:
        service, user = self._service(god_item_max_retries=3)
        fetched: list[int] = []
        failures = {"2": 2}
        processed: list[int] = []

        async def get_chat(_username):
            return SimpleNamespace(id=-1001)

        async def get_messages(_chat_id, msg_id):
            fetched.append(msg_id)
            left = failures.get(str(msg_id), 0)
            if left:
                failures[str(msg_id)] = left - 1
                raise RPCError(value="TEST")
            if msg_id == 2:
                return _media(msg_id)
            return None

        async def process(media_messages, **_kwargs):
            processed.append(media_messages[0].id)
            return 1

        user.get_chat = get_chat
        user.get_messages = get_messages
        service._process_media_messages = process  # type: ignore[method-assign]
        parsed = ParsedLink(message_id=2, username="chan")
        result = await service.process_god_crawl(
            1,
            1,
            "https://t.me/chan/2",
            direction="up",
            start_id=2,
            parsed=parsed,
        )
        self.assertEqual(fetched[:3], [2, 2, 2])
        self.assertEqual(processed, [2])
        self.assertNotIn("Unrecovered", result.message or "")

    async def test_failed_group_fetch_does_not_hide_later_album_items(self) -> None:
        service, user = self._service()
        group_calls: list[int] = []
        processed: list[list[int]] = []

        async def get_chat(_username):
            return SimpleNamespace(id=-1001)

        async def get_messages(_chat_id, msg_id):
            if msg_id in {10, 11}:
                return _media(msg_id, "album")
            return None

        async def get_media_group(_chat_id, msg_id):
            group_calls.append(msg_id)
            if msg_id == 10:
                raise ValueError("group unavailable")
            return [_media(11, "album"), _media(12, "album")]

        async def process(media_messages, **_kwargs):
            processed.append([item.id for item in media_messages])
            return len(media_messages)

        user.get_chat = get_chat
        user.get_messages = get_messages
        user.get_media_group = get_media_group
        service._process_media_messages = process  # type: ignore[method-assign]
        parsed = ParsedLink(message_id=10, username="chan")
        await service.process_god_crawl(
            1,
            1,
            "https://t.me/chan/10",
            direction="up",
            start_id=10,
            end_id=11,
            parsed=parsed,
        )
        self.assertEqual(group_calls, [10, 11])
        self.assertEqual(processed[0], [10])
        self.assertEqual(processed[1], [11, 12])

    async def test_range_stops_at_end_id(self) -> None:
        service, user = self._service()
        fetched: list[int] = []

        async def get_chat(_username):
            return SimpleNamespace(id=-1001)

        async def get_messages(_chat_id, msg_id):
            fetched.append(msg_id)
            if 1 <= msg_id <= 5:
                return _media(msg_id)
            return None

        async def process(media_messages, **_kwargs):
            return 1

        user.get_chat = get_chat
        user.get_messages = get_messages
        service._process_media_messages = process  # type: ignore[method-assign]
        parsed = ParsedLink(message_id=1, username="chan")
        await service.process_god_crawl(
            1,
            1,
            "https://t.me/chan/1",
            direction="up",
            start_id=1,
            end_id=3,
            parsed=parsed,
        )
        self.assertEqual(fetched, [1, 2, 3])


if __name__ == "__main__":
    unittest.main()
