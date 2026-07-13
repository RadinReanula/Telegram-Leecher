from functools import cached_property
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    api_id: int = Field(validation_alias="API_ID")
    api_hash: str = Field(validation_alias="API_HASH")
    bot_token: str = Field(validation_alias="BOT_TOKEN")
    allowed_user_ids: list[int] = Field(default_factory=list, validation_alias="ALLOWED_USER_IDS")

    session_name: str = Field(default="user", validation_alias="SESSION_NAME")
    sessions_dir: Path = Field(default=PROJECT_ROOT / "sessions", validation_alias="SESSIONS_DIR")
    tmp_dir: Path = Field(default=PROJECT_ROOT / "tmp", validation_alias="TMP_DIR")
    bot_max_file_bytes: int = Field(default=52_428_800, validation_alias="BOT_MAX_FILE_BYTES")
    # Below the 50 MB bot cap: files above ~40 MB often timeout via Bot API — send via user session instead.
    bot_upload_threshold_bytes: int = Field(
        default=41_943_040,
        validation_alias="BOT_UPLOAD_THRESHOLD_BYTES",
    )
    bot_request_timeout_sec: int = Field(default=600, validation_alias="BOT_REQUEST_TIMEOUT_SEC")
    bot_ssl_verify: bool = Field(default=True, validation_alias="BOT_SSL_VERIFY")

    @field_validator("bot_ssl_verify", mode="before")
    @classmethod
    def parse_bot_ssl_verify(cls, value: object) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"0", "false", "no", "off"}:
                return False
            if normalized in {"1", "true", "yes", "on"}:
                return True
        return bool(value)

    queue_workers: int = Field(default=1, validation_alias="QUEUE_WORKERS")
    max_queue_size: int = Field(default=50, validation_alias="MAX_QUEUE_SIZE")
    max_pending_per_user: int = Field(default=5, validation_alias="MAX_PENDING_PER_USER")
    job_history_limit: int = Field(default=200, validation_alias="JOB_HISTORY_LIMIT")
    sync_dialogs_on_startup: bool = Field(default=True, validation_alias="SYNC_DIALOGS_ON_STARTUP")
    sync_dialogs_in_background: bool = Field(
        default=True,
        validation_alias="SYNC_DIALOGS_IN_BACKGROUND",
    )
    max_links_per_message: int = Field(default=25, validation_alias="MAX_LINKS_PER_MESSAGE")
    status_update_interval_sec: float = Field(default=2.0, validation_alias="STATUS_UPDATE_INTERVAL_SEC")
    floodwait_max_retries: int = Field(default=1, validation_alias="FLOODWAIT_MAX_RETRIES")
    album_pipeline: bool = Field(default=False, validation_alias="ALBUM_PIPELINE")
    album_concurrency: int = Field(default=1, validation_alias="ALBUM_CONCURRENCY")
    job_prune_every_n_enqueues: int = Field(default=10, validation_alias="JOB_PRUNE_EVERY_N_ENQUEUES")
    download_progress_enabled: bool = Field(
        default=True,
        validation_alias="DOWNLOAD_PROGRESS_ENABLED",
    )

    # God mode — sequential message-ID crawl
    god_delay_sec: float = Field(default=2.0, validation_alias="GOD_DELAY_SEC")
    god_floodwait_extra_sec: int = Field(default=5, validation_alias="GOD_FLOODWAIT_EXTRA_SEC")
    god_max_consecutive_miss: int = Field(default=25, validation_alias="GOD_MAX_CONSECUTIVE_MISS")
    god_max_messages: int = Field(default=5000, validation_alias="GOD_MAX_MESSAGES")
    god_skip_already_seen_groups: bool = Field(
        default=True,
        validation_alias="GOD_SKIP_ALREADY_SEEN_GROUPS",
    )
    # Auto-cooldown after N successful media sends (0 = disable)
    god_cooldown_every: int = Field(default=150, validation_alias="GOD_COOLDOWN_EVERY")
    god_cooldown_sec: int = Field(default=180, validation_alias="GOD_COOLDOWN_SEC")
    # Max reconnect attempts before failing a god crawl
    god_reconnect_max_retries: int = Field(default=5, validation_alias="GOD_RECONNECT_MAX_RETRIES")

    @field_validator(
        "sync_dialogs_on_startup",
        "sync_dialogs_in_background",
        "album_pipeline",
        "download_progress_enabled",
        "god_skip_already_seen_groups",
        mode="before",
    )
    @classmethod
    def parse_bool_env(cls, value: object) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    @field_validator(
        "queue_workers",
        "album_concurrency",
        "floodwait_max_retries",
        "god_floodwait_extra_sec",
        "god_max_consecutive_miss",
        "god_max_messages",
        "god_cooldown_every",
        "god_cooldown_sec",
        "god_reconnect_max_retries",
    )
    @classmethod
    def validate_positive_int(cls, value: int) -> int:
        if value < 0:
            raise ValueError("Value must be non-negative.")
        return value

    @field_validator("queue_workers")
    @classmethod
    def validate_queue_workers(cls, value: int) -> int:
        if value < 1:
            raise ValueError("QUEUE_WORKERS must be at least 1.")
        return value

    @field_validator("album_concurrency")
    @classmethod
    def validate_album_concurrency(cls, value: int) -> int:
        if value < 1:
            raise ValueError("ALBUM_CONCURRENCY must be at least 1.")
        return value

    @field_validator("god_max_messages", "god_max_consecutive_miss")
    @classmethod
    def validate_god_positive(cls, value: int) -> int:
        if value < 1:
            raise ValueError("God mode limits must be at least 1.")
        return value

    @field_validator("god_delay_sec")
    @classmethod
    def validate_god_delay(cls, value: float) -> float:
        if value < 0:
            raise ValueError("GOD_DELAY_SEC must be non-negative.")
        return value

    @field_validator("god_reconnect_max_retries")
    @classmethod
    def validate_god_reconnect(cls, value: int) -> int:
        if value < 1:
            raise ValueError("GOD_RECONNECT_MAX_RETRIES must be at least 1.")
        return value

    @cached_property
    def allowed_user_id_set(self) -> frozenset[int]:
        return frozenset(self.allowed_user_ids)

    @field_validator("allowed_user_ids", mode="before")
    @classmethod
    def parse_allowed_ids(cls, value: object) -> list[int]:
        if value is None or value == "":
            return []
        if isinstance(value, list):
            return [int(item) for item in value]
        return [int(part.strip()) for part in str(value).split(",") if part.strip()]

    @property
    def session_path(self) -> Path:
        return self.sessions_dir / self.session_name

    def ensure_dirs(self) -> None:
        self.sessions_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)


def get_settings() -> Settings:
    return Settings()
