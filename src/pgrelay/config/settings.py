"""Runtime settings loaded from PGRELAY environment variables."""

from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from pgrelay.constants import DEFAULT_API_TOKEN, DEFAULT_QUEUE_NAME


class Settings(BaseSettings):
    """Central PgRelay runtime settings."""

    model_config = SettingsConfigDict(env_prefix="PGRELAY_", env_file=".env", extra="ignore")

    env: Literal["dev", "test", "prod"] = "dev"
    database_url: str = Field(min_length=1)

    api_host: str = "0.0.0.0"
    api_port: int = Field(default=8090, ge=1, le=65535)
    api_auth_tokens: str = ""
    api_read_only_auth_tokens: str = ""

    worker_id_prefix: str = Field(default="worker", min_length=3, max_length=64)
    worker_queues: str = DEFAULT_QUEUE_NAME
    worker_concurrency: int = Field(default=8, ge=1, le=256)
    worker_batch_size: int = Field(default=8, ge=1, le=256)
    worker_poll_interval_seconds: float = Field(default=1.0, ge=0.05, le=60.0)
    worker_lease_seconds: int = Field(default=60, ge=5, le=3600)
    worker_shutdown_grace_seconds: int = Field(default=20, ge=1, le=300)

    db_pool_size: int = Field(default=12, ge=1, le=512)
    db_max_overflow: int = Field(default=4, ge=0, le=512)
    db_pool_timeout_seconds: int = Field(default=30, ge=1, le=300)
    db_statement_timeout_ms: int = Field(default=30000, ge=1000, le=300000)
    db_lock_timeout_ms: int = Field(default=5000, ge=100, le=60000)

    retry_base_seconds: int = Field(default=2, ge=1, le=3600)
    retry_max_seconds: int = Field(default=900, ge=1, le=86400)
    retry_jitter_ratio: float = Field(default=0.2, ge=0.0, le=1.0)

    http_default_timeout_seconds: int = Field(default=20, ge=1, le=300)
    http_max_timeout_seconds: int = Field(default=120, ge=1, le=600)
    http_allowed_hosts: str = ""
    block_private_network_targets: bool = True
    http_max_connections: int = Field(default=100, ge=1, le=1000)
    http_max_keepalive_connections: int = Field(default=20, ge=0, le=1000)

    retention_succeeded_days: int = Field(default=7, ge=1, le=365)
    retention_dead_letter_days: int = Field(default=30, ge=1, le=3650)
    purge_batch_size: int = Field(default=1000, ge=1, le=10000)

    max_payload_bytes: int = Field(default=262144, ge=1024, le=10485760)
    max_headers_bytes: int = Field(default=16384, ge=1024, le=262144)
    max_metadata_bytes: int = Field(default=16384, ge=1024, le=262144)
    log_level: str = "INFO"

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: str) -> str:
        """Validate that runtime database URL uses asyncpg."""
        if not value.startswith("postgresql+asyncpg://"):
            raise ValueError("database_url must start with postgresql+asyncpg://")
        return value

    def get_queue_names(self) -> list[str]:
        """Return configured worker queue names."""
        names = []
        parts = self.worker_queues.split(",")
        for part in parts:
            name = part.strip()
            if name:
                names.append(name)
        return names

    def get_allowed_hosts(self) -> set[str]:
        """Return allowed HTTP target hosts."""
        hosts = set()
        parts = self.http_allowed_hosts.split(",")
        for part in parts:
            host = part.strip().lower()
            if host:
                hosts.add(host)
        return hosts

    def get_api_tokens(self) -> set[str]:
        """Return configured API bearer tokens."""
        tokens = set()
        parts = self.api_auth_tokens.split(",")
        for part in parts:
            token = part.strip()
            if token:
                tokens.add(token)
        return tokens

    def get_read_only_api_tokens(self) -> set[str]:
        """Return configured read-only API bearer tokens."""
        tokens = set()
        parts = self.api_read_only_auth_tokens.split(",")
        for part in parts:
            token = part.strip()
            if token:
                tokens.add(token)
        return tokens

    def validate_runtime(self) -> None:
        """Validate settings that depend on runtime environment."""
        tokens = self.get_api_tokens()
        read_only_tokens = self.get_read_only_api_tokens()
        default_token_used = DEFAULT_API_TOKEN in tokens or DEFAULT_API_TOKEN in read_only_tokens
        if self.env == "prod" and not tokens:
            raise ValueError("PGRELAY_API_AUTH_TOKENS is required when PGRELAY_ENV=prod")
        if self.env == "prod" and default_token_used:
            raise ValueError("Default dev API token is forbidden when PGRELAY_ENV=prod")
        if self.env == "prod" and not self.get_allowed_hosts():
            raise ValueError("PGRELAY_HTTP_ALLOWED_HOSTS is required when PGRELAY_ENV=prod")
        minimum_pool_size = self.worker_concurrency + 2
        actual_pool_size = self.db_pool_size + self.db_max_overflow
        if actual_pool_size < minimum_pool_size:
            raise ValueError("db_pool_size + db_max_overflow must be at least worker_concurrency + 2")


def load_settings() -> Settings:
    """Load settings from PGRELAY environment variables."""
    return Settings()  # type: ignore[call-arg]
