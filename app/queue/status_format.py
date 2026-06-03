from datetime import datetime

from app.queue.models import DownloadJob, JobStage, JobStatus


def _fmt_time(timestamp: float | None) -> str:
    if timestamp is None:
        return "—"
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")


def _progress_bar(progress: int, width: int = 10) -> str:
    filled = int(width * progress / 100)
    return "[" + "#" * filled + "-" * (width - filled) + f"] {progress}%"


def format_job_status(job: DownloadJob, *, compact: bool = False) -> str:
    name = job.display_name or "Pending…"
    batch = f" ({job.batch_label})" if job.batch_label else ""

    if compact:
        icon = {
            JobStatus.QUEUED: "⏳",
            JobStatus.RUNNING: "▶️",
            JobStatus.COMPLETED: "✅",
            JobStatus.FAILED: "❌",
            JobStatus.SKIPPED: "⏭️",
            JobStatus.CANCELLED: "🛑",
        }[job.status]
        detail = job.result or job.error or job.stage.value
        if len(detail) > 40:
            detail = detail[:37] + "..."
        return f"{icon} `{job.id}`{batch} {name} — {detail}"

    lines = [
        f"Job `{job.id}`{batch}",
        f"Name: {name}",
        f"Status: {job.status.value} | Stage: {job.stage.value}",
    ]

    if job.status == JobStatus.RUNNING and job.stage == JobStage.DOWNLOADING:
        lines.append(f"Progress: {_progress_bar(job.progress)}")
    elif job.progress > 0:
        lines.append(f"Progress: {job.progress}%")

    lines.extend(
        [
            f"Created: {_fmt_time(job.created_at)}",
            f"Started: {_fmt_time(job.started_at)}",
            f"Finished: {_fmt_time(job.finished_at)}",
            f"Link: {job.link}",
        ]
    )

    if job.result:
        lines.append(f"Result: {job.result}")
    if job.error:
        lines.append(f"Detail: {job.error}")

    return "\n".join(lines)


def format_status_list(jobs: list[DownloadJob], *, max_chars: int = 4000) -> str:
    if not jobs:
        return "You have no recent jobs."

    header = "Your jobs (newest first):\n"
    parts = [header]
    used = len(header)

    for job in jobs:
        line = format_job_status(job, compact=True) + "\n"
        if used + len(line) > max_chars:
            parts.append(f"…and {len(jobs) - len(parts) + 1} more. Use /job <id> for details.")
            break
        parts.append(line)
        used += len(line)

    return "".join(parts).rstrip()
