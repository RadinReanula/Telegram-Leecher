import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Literal

from app.parser.telegram_links import ParsedLink


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class JobStage(str, Enum):
    QUEUED = "queued"
    RESOLVING = "resolving"
    DOWNLOADING = "downloading"
    UPLOADING = "uploading"
    DONE = "done"
    SKIPPED = "skipped"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(slots=True)
class DownloadJob:
    link: str
    requester_id: int
    bot_chat_id: int
    status_chat_id: int
    status_message_id: int
    parsed: ParsedLink | None = None
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    status: JobStatus = JobStatus.QUEUED
    stage: JobStage = JobStage.QUEUED
    progress: int = 0
    display_name: str | None = None
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    result: str | None = None
    error: str | None = None
    batch_index: int | None = None
    batch_total: int | None = None
    mode: Literal["link", "god"] = "link"
    god_direction: Literal["up", "down"] | None = None
    god_start_id: int | None = None
    god_scanned: int = 0
    god_downloaded: int = 0
    god_skipped: int = 0
    god_missing: int = 0
    god_current_id: int | None = None
    god_miss_streak: int = 0

    @property
    def is_finished(self) -> bool:
        return self.status in {
            JobStatus.COMPLETED,
            JobStatus.FAILED,
            JobStatus.SKIPPED,
            JobStatus.CANCELLED,
        }

    @property
    def batch_label(self) -> str | None:
        if self.batch_index is not None and self.batch_total is not None:
            return f"{self.batch_index}/{self.batch_total}"
        return None

    @property
    def is_god(self) -> bool:
        return self.mode == "god"
