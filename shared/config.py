import os
from dataclasses import dataclass

@dataclass(frozen=True)
class Config:
    database_url:       str = os.getenv("DATABASE_URL",       "postgresql://distrq:distrq@postgres:5432/distrq")
    async_database_url: str = os.getenv("ASYNC_DATABASE_URL", "postgresql+asyncpg://distrq:distrq@postgres:5432/distrq")
    redis_url: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
    worker_id:              str = os.getenv("WORKER_ID", "worker-local")
    worker_concurrency:     int = int(os.getenv("WORKER_CONCURRENCY",    "50"))
    heartbeat_interval_sec: int = int(os.getenv("HEARTBEAT_INTERVAL",    "5"))
    visibility_timeout_sec: int = int(os.getenv("VISIBILITY_TIMEOUT",    "30"))
    poll_interval_sec:    float = float(os.getenv("POLL_INTERVAL",       "0.1"))
    watchdog_interval_sec:  int = int(os.getenv("WATCHDOG_INTERVAL",     "10"))
    stale_threshold_sec:    int = int(os.getenv("STALE_THRESHOLD",       "20"))
    api_host: str = os.getenv("API_HOST", "0.0.0.0")
    api_port: int = int(os.getenv("API_PORT", "8000"))
    default_max_retries: int = int(os.getenv("DEFAULT_MAX_RETRIES", "3"))

settings = Config()
