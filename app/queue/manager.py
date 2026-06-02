import asyncio
import logging
import time
from collections import defaultdict, deque

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError
from pyrogram.errors import FloodWait

from app.config import Settings
from app.downloader.results import DownloadOutcome
from app.downloader.service import DownloadService
from app.queue.models import DownloadJob, JobStage, JobStatus
from app.queue.status_format import format_job_status

logger = logging.getLogger(__name__)


class JobQueue:
    def __init__(
        self,
        download_service: DownloadService,
        bot: Bot,
        settings: Settings,
    ) -> None:
        self._download_service = download_service
        self._bot = bot
        self._settings = settings
        self._jobs: dict[str, DownloadJob] = {}
        self._pending: deque[str] = deque()
        self._queue: asyncio.Queue[str | None] = asyncio.Queue()
        self._workers: list[asyncio.Task[None]] = []
        self._running_ids: set[str] = set()
        self._lock = asyncio.Lock()
        self._last_status_edit: dict[str, float] = {}
        self._pending_refresh: set[str] = set()
        self._pending_by_user: defaultdict[int, int] = defaultdict(int)
        self._running_by_user: defaultdict[int, int] = defaultdict(int)
        self._jobs_by_user: defaultdict[int, list[str]] = defaultdict(list)
        self._enqueue_count = 0

    async def start(self) -> None:
        if self._workers:
            return
        worker_count = self._settings.queue_workers
        self._workers = [
            asyncio.create_task(self._worker_loop(worker_index=i), name=f"job-worker-{i}")
            for i in range(worker_count)
        ]
        logger.info("Job queue started with %s worker(s)", worker_count)

    async def stop(self) -> None:
        for _ in self._workers:
            await self._queue.put(None)
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers.clear()
        logger.info("Job queue stopped")

    async def enqueue(
        self,
        job: DownloadJob,
        *,
        batch_burst: bool = False,
    ) -> tuple[DownloadJob, int, int]:
        async with self._lock:
            if not batch_burst:
                active_for_user = (
                    self._pending_by_user[job.requester_id]
                    + self._running_by_user[job.requester_id]
                )
                if active_for_user >= self._settings.max_pending_per_user:
                    raise ValueError(
                        f"You already have {active_for_user} job(s) queued or running "
                        f"(limit: {self._settings.max_pending_per_user}). "
                        "Wait for them to finish or check /status."
                    )

            if len(self._pending) >= self._settings.max_queue_size:
                raise ValueError(
                    f"Queue is full ({self._settings.max_queue_size} jobs). Try again later."
                )

            running_count = len(self._running_ids)
            self._jobs[job.id] = job
            self._pending.append(job.id)
            self._pending_by_user[job.requester_id] += 1
            self._jobs_by_user[job.requester_id].append(job.id)
            position = len(self._pending)
            await self._queue.put(job.id)

            self._enqueue_count += 1
            if self._enqueue_count % self._settings.job_prune_every_n_enqueues == 0:
                self._prune_old_jobs()

        return job, position, running_count

    def get_job(self, job_id: str) -> DownloadJob | None:
        return self._jobs.get(job_id)

    def jobs_for_user(self, user_id: int, *, include_finished: bool = True) -> list[DownloadJob]:
        job_ids = self._jobs_by_user.get(user_id, [])
        jobs = [self._jobs[jid] for jid in job_ids if jid in self._jobs]
        if not include_finished:
            jobs = [job for job in jobs if not job.is_finished]
        return sorted(jobs, key=lambda job: job.created_at, reverse=True)

    def queue_snapshot(self) -> tuple[int, int, int]:
        pending = len(self._pending)
        running = len(self._running_ids)
        return pending, running, len(self._jobs)

    async def _worker_loop(self, worker_index: int) -> None:
        while True:
            job_id = await self._queue.get()
            try:
                if job_id is None:
                    break
                await self._run_job(job_id)
            finally:
                self._queue.task_done()

    def _expand_media_group_for_job(self, job: DownloadJob) -> bool:
        return job.batch_total is None or job.batch_total <= 1

    async def _run_job(self, job_id: str) -> None:
        job = self._jobs.get(job_id)
        if not job:
            return

        async with self._lock:
            if job_id in self._pending:
                self._pending.remove(job_id)
            self._pending_by_user[job.requester_id] -= 1
            if self._pending_by_user[job.requester_id] <= 0:
                del self._pending_by_user[job.requester_id]
            self._running_ids.add(job_id)
            self._running_by_user[job.requester_id] += 1

        job.status = JobStatus.RUNNING
        job.started_at = time.time()
        job.stage = JobStage.RESOLVING
        job.progress = 0
        await self._refresh_status_message(job, force=True)

        def on_progress(stage: str, progress: int, display_name: str | None) -> None:
            if display_name:
                job.display_name = display_name
            job.progress = progress
            if stage == "resolving":
                job.stage = JobStage.RESOLVING
            elif stage == "downloading":
                job.stage = JobStage.DOWNLOADING
            elif stage == "uploading":
                job.stage = JobStage.UPLOADING
            self._schedule_status_refresh(job)

        flood_retries = 0
        try:
            while True:
                try:
                    result = await self._download_service.process_link(
                        requester_id=job.requester_id,
                        bot_chat_id=job.bot_chat_id,
                        link=job.link,
                        on_progress=on_progress,
                        expand_media_group=self._expand_media_group_for_job(job),
                        parsed=job.parsed,
                    )
                    break
                except FloodWait as exc:
                    if flood_retries >= self._settings.floodwait_max_retries:
                        job.finished_at = time.time()
                        job.status = JobStatus.FAILED
                        job.stage = JobStage.FAILED
                        job.error = (
                            f"Telegram rate limit — try again in {exc.value} seconds."
                        )
                        await self._refresh_status_message(job, force=True)
                        return
                    logger.info(
                        "Job %s hit FloodWait %ss, retry %s/%s",
                        job.id,
                        exc.value,
                        flood_retries + 1,
                        self._settings.floodwait_max_retries,
                    )
                    await asyncio.sleep(exc.value + 1)
                    flood_retries += 1

            job.finished_at = time.time()

            if result.display_name:
                job.display_name = result.display_name

            if result.outcome == DownloadOutcome.SUCCESS:
                job.status = JobStatus.COMPLETED
                job.stage = JobStage.DONE
                job.progress = 100
                job.result = result.message
            elif result.outcome == DownloadOutcome.SKIPPED:
                job.status = JobStatus.SKIPPED
                job.stage = JobStage.SKIPPED
                job.error = result.message
            else:
                job.status = JobStatus.FAILED
                job.stage = JobStage.FAILED
                job.error = result.message

            await self._refresh_status_message(job, force=True)
        except Exception:
            logger.exception("Job %s failed for link %s", job.id, job.link)
            job.status = JobStatus.FAILED
            job.stage = JobStage.FAILED
            job.finished_at = time.time()
            job.error = "Unexpected error while downloading."
            await self._refresh_status_message(job, force=True)
        finally:
            async with self._lock:
                self._running_ids.discard(job_id)
                self._running_by_user[job.requester_id] -= 1
                if self._running_by_user[job.requester_id] <= 0:
                    del self._running_by_user[job.requester_id]
            self._last_status_edit.pop(job.id, None)
            self._pending_refresh.discard(job.id)

    def _schedule_status_refresh(self, job: DownloadJob) -> None:
        if job.id in self._pending_refresh:
            return
        self._pending_refresh.add(job.id)
        asyncio.create_task(self._refresh_status_safe(job))

    async def _refresh_status_safe(self, job: DownloadJob) -> None:
        try:
            await self._refresh_status_message(job)
        finally:
            self._pending_refresh.discard(job.id)

    async def _refresh_status_message(self, job: DownloadJob, *, force: bool = False) -> None:
        now = time.time()
        last = self._last_status_edit.get(job.id, 0)
        if not force and (now - last) < self._settings.status_update_interval_sec:
            if job.stage not in {JobStage.DONE, JobStage.SKIPPED, JobStage.FAILED}:
                return

        prefix = {
            JobStatus.QUEUED: "⏳",
            JobStatus.RUNNING: "▶️",
            JobStatus.COMPLETED: "✅",
            JobStatus.FAILED: "❌",
            JobStatus.SKIPPED: "⏭️",
        }[job.status]
        body = format_job_status(job)
        text = f"{prefix} {body}"

        try:
            await self._bot.edit_message_text(
                text=text,
                chat_id=job.status_chat_id,
                message_id=job.status_message_id,
            )
            self._last_status_edit[job.id] = now
        except TelegramBadRequest as exc:
            if "message is not modified" in str(exc).lower():
                self._last_status_edit[job.id] = now
                return
            logger.warning("Could not edit status message for job %s: %s", job.id, exc)
        except TelegramNetworkError as exc:
            logger.warning("Network error editing status for job %s: %s", job.id, exc)

    def _prune_old_jobs(self) -> None:
        if len(self._jobs) <= self._settings.job_history_limit:
            return
        finished = sorted(
            (job for job in self._jobs.values() if job.is_finished),
            key=lambda job: job.finished_at or 0,
        )
        to_remove = len(self._jobs) - self._settings.job_history_limit
        for job in finished[:to_remove]:
            del self._jobs[job.id]
            user_list = self._jobs_by_user.get(job.requester_id)
            if user_list and job.id in user_list:
                user_list.remove(job.id)
