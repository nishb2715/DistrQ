"""
DistrQ — Shared SQLAlchemy Models
PostgreSQL acts as the authoritative state machine for every task lifecycle.
"""

import uuid
import enum
from datetime import datetime

from sqlalchemy import (
    Column, String, Integer, DateTime, Enum as SAEnum,
    Text, Index, CheckConstraint, func
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    pass


class TaskStatus(str, enum.Enum):
    PENDING     = "PENDING"
    PROCESSING  = "PROCESSING"
    COMPLETED   = "COMPLETED"
    FAILED      = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"   # exhausted all retries


class TaskPriority(int, enum.Enum):
    LOW    = 1
    NORMAL = 5
    HIGH   = 10
    URGENT = 20


class Task(Base):
    """
    Central state-machine table.

    Lifecycle:
        PENDING  ──(worker picks up)──►  PROCESSING
        PROCESSING  ──(success)──────►  COMPLETED
        PROCESSING  ──(failure/retry)►  PENDING      (retry_count < max_retries)
        PROCESSING  ──(failure/retry)►  FAILED       (retry_count >= max_retries)
        FAILED      ──(DLQ sweep)────►  DEAD_LETTER

    Fault-tolerance columns:
        worker_id          – which worker instance owns this task right now
        last_heartbeat     – updated every N seconds by the owning worker
        visibility_timeout – absolute UTC deadline; watchdog re-queues if exceeded
        locked_until       – advisory lock to prevent double-pickup under high fan-out
    """

    __tablename__ = "tasks"

    # ── Identity ────────────────────────────────────────────────────────────
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        nullable=False,
    )

    # ── Payload ─────────────────────────────────────────────────────────────
    task_type = Column(String(128), nullable=False, index=True)
    payload   = Column(Text, nullable=False, default="{}")  # JSON blob

    # ── Priority (higher = processed sooner) ────────────────────────────────
    priority = Column(
        Integer,
        nullable=False,
        default=TaskPriority.NORMAL,
        index=True,
    )

    # ── State Machine ────────────────────────────────────────────────────────
    status = Column(
        SAEnum(TaskStatus, name="task_status_enum", create_type=False),
        nullable=False,
        default=TaskStatus.PENDING,
        index=True,
    )

    # ── Retry Bookkeeping ────────────────────────────────────────────────────
    retry_count = Column(Integer, nullable=False, default=0)
    max_retries = Column(Integer, nullable=False, default=3)
    error_msg   = Column(Text, nullable=True)   # last failure reason

    # ── Fault-Tolerance: Worker Ownership ───────────────────────────────────
    worker_id      = Column(String(256), nullable=True, index=True)
    last_heartbeat = Column(DateTime(timezone=True), nullable=True)

    # ── Fault-Tolerance: Visibility Timeout ─────────────────────────────────
    # If status=PROCESSING and now() > visibility_timeout, watchdog re-queues.
    visibility_timeout = Column(DateTime(timezone=True), nullable=True)

    # ── Timestamps ──────────────────────────────────────────────────────────
    created_at   = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at   = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    started_at   = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)

    # ── Constraints ──────────────────────────────────────────────────────────
    __table_args__ = (
        CheckConstraint("retry_count >= 0",             name="chk_retry_nonneg"),
        CheckConstraint("max_retries >= 0",             name="chk_max_retries_nonneg"),
        CheckConstraint("retry_count <= max_retries",   name="chk_retry_lte_max"),
        CheckConstraint("priority > 0",                 name="chk_priority_pos"),
        # Composite index: watchdog query (status + visibility_timeout)
        Index("ix_tasks_watchdog",  "status", "visibility_timeout"),
        # Composite index: priority dequeue (status + priority DESC + created_at)
        Index("ix_tasks_dequeue",   "status", "priority", "created_at"),
    )

    def __repr__(self) -> str:
        return (
            f"<Task id={self.id} type={self.task_type} "
            f"status={self.status} priority={self.priority} "
            f"retries={self.retry_count}/{self.max_retries}>"
        )