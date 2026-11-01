"""Shared Bot API pacing gate for god-mode private-chat flood control."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import TypeVar

from aiogram.exceptions import TelegramRetryAfter

logger = logging.getLogger(__name__)

T = TypeVar("T")


class BotApiGate:
    """Serialize Bot API calls with a minimum interval and RetryAfter handling.

    Used only for god-mode media sends and status edits in the same private chat.
    """

    def __init__(self, *, min_interval_sec: float, retry_max: int = 2) -> None:
        self._min_interval_sec = max(0.0, float(min_interval_sec))
        self._retry_max = max(0, int(retry_max))
        self._lock = asyncio.Lock()
        self._next_allowed_at = 0.0

    def postpone(self, seconds: float) -> None:
        """Block further gated calls until now + seconds (best-effort, no lock)."""
        if seconds <= 0:
            return
        self._next_allowed_at = max(self._next_allowed_at, time.monotonic() + seconds)

    async def wait_turn(self) -> None:
        """Wait until the minimum interval has elapsed since the last completed call."""
        async with self._lock:
            await self._wait_locked()

    async def run(self, factory: Callable[[], Awaitable[T]]) -> T:
        """Run a Bot API coroutine after waiting for a turn; retry on RetryAfter."""
        async with self._lock:
            attempt = 0
            while True:
                await self._wait_locked()
                try:
                    result = await factory()
                    self._next_allowed_at = time.monotonic() + self._min_interval_sec
                    return result
                except TelegramRetryAfter as exc:
                    wait_for = float(exc.retry_after) + 1.0
                    self._next_allowed_at = time.monotonic() + wait_for
                    if attempt >= self._retry_max:
                        logger.warning(
                            "Bot API RetryAfter exhausted after %s attempt(s); "
                            "retry_after=%ss",
                            attempt + 1,
                            exc.retry_after,
                        )
                        raise
                    attempt += 1
                    logger.warning(
                        "Bot API RetryAfter %ss — sleeping then retry %s/%s",
                        exc.retry_after,
                        attempt,
                        self._retry_max,
                    )
                    await asyncio.sleep(wait_for)

    async def _wait_locked(self) -> None:
        now = time.monotonic()
        delay = self._next_allowed_at - now
        if delay > 0:
            await asyncio.sleep(delay)
