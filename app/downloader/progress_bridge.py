import asyncio
import logging
from collections.abc import Callable

logger = logging.getLogger(__name__)


class DownloadProgressBridge:
    """Thread-safe Pyrogram download progress -> main asyncio loop."""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        on_bytes: Callable[[int, int], None],
    ) -> None:
        self._loop = loop
        self._on_bytes = on_bytes
        self._queue: asyncio.Queue[tuple[int, int] | None] = asyncio.Queue()
        self._consumer_task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._consumer_task = asyncio.create_task(self._consume())

    async def stop(self) -> None:
        await self._queue.put(None)
        if self._consumer_task is not None:
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
            self._consumer_task = None

    def callback(self, current: int, total: int) -> None:
        if total <= 0:
            return
        try:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, (current, total))
        except RuntimeError:
            pass

    async def _consume(self) -> None:
        while True:
            item = await self._queue.get()
            if item is None:
                return
            current, total = item
            try:
                self._on_bytes(current, total)
            except Exception:
                logger.debug("Progress callback error", exc_info=True)
