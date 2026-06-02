from dataclasses import dataclass
from enum import Enum


class DownloadOutcome(str, Enum):
    SUCCESS = "success"
    SKIPPED = "skipped"
    FAILED = "failed"


@dataclass(slots=True)
class DownloadResult:
    outcome: DownloadOutcome
    message: str
    display_name: str | None = None
