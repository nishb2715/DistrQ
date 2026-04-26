"""
DistrQ — Async Worker

Responsibilities:
  1. Poll Redis priority queue for task IDs.
  2. Claim the task in Postgres (PENDING → PROCESSING) with an optimistic lock.
  3. Execute the task (simulated work + real task_type dispatch).
  4. Send heartbeats every HEARTBEAT_INTERVAL seconds.
  5. On success → mark COMPLETED.
  6. On failure → retry (back to PENDING) or mark FAILED / DEAD_LETTER.

Concurrency model:
  • asyncio event loop — single process, N concurrent coroutines (semaphore-bounded).
  • WORKER_CONCURRENCY env var controls fan-out (default 50).
  • Multiple Docker replicas provide horizontal scaling.
"""

import asyncio
import json
import logging
import os
import signal
import socket
import sys
import uuid
from datetime import datetime, timedelta, timezone

import redis.asyncio as aioredis
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

sys.path.insert(0, "/app")

from shared.config import settings
from shared.database import AsyncSessionLocal, create_tables
from shared.models import Task, TaskStatus
from shared.queue import dequeue, get_async_redis, requeue

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("distrq.worker")

# Unique worker identity (hostname + pid makes it unique across Docker replicas)
WORKER_ID = f"{socket.gethostname()}-{os.getpid()}"


# ── Task execution registry ───────────────────────────────────────────────────

async def _execute_task(task_type: str, payload: dict) -> dict:
    """
    Dispatch to the right handler by task_type.
    Extend this registry with real business logic.
    """
    handlers = {
        "compute":  _handle_compute,
        "io_sim":   _handle_io_sim,
        "email":    _handle_email_sim,
        "default":  _handle_default,
    }
    handler = handlers.get(task_type, handlers["default"])
    return await handler(payload)


async def _handle_compute(payload: dict) -> dict:
    """CPU-bound simulation — real sleep to simulate work."""
    duration = payload.get("duration_ms", 50) / 1000
    await asyncio.sleep(duration)
    return {"result": "computed", "duration_ms": payload.get("duration_ms", 50)}


async def _handle_io_sim(payload: dict) -> dict:
    """I/O-bound simulation."""
    await asyncio.sleep(payload.get("latency_ms", 20) / 1000)
    return {"result": "io_complete"}


async def _handle_email_sim(payload: dict) -> dict:
    await asyncio.sleep(0.01)
    return {"result": f"email_sent_to_{payload.get('to', 'unknown')}"}


async def _handle_default(payload: dict) -> dict:
    await asyncio.sleep(0.05)
    return {"result": "ok"}


# ── Core worker logic ─────────────────────────────────────────────────────────

async def claim_task(db: AsyncSession, task_id: uuid.UUID) -> Task | None:
    """
    Atomically transition PENDING → PROCESSING.
    Uses SELECT FOR UPDATE SKIP LOCKED to prevent double-pickup.
    Returns the claimed task or None if already taken.
    """
    now = datetime.now(timezone.utc)
    visibility_deadline = now + timedelta(seconds=settings.visibility_timeout_sec)

    result = await db.execute(
        select(Task)
        .where(Task.id == task_id, Task.status == TaskStatus.PENDING)
        .with_for_update(skip_locked=True)
    )
    task = result.scalar_one_or_none()
    if task is None:
        return None  # already claimed by another worker

    task.status             = TaskStatus.PROCESSING
    task.worker_id          = WORKER_ID
    task.last_heartbeat     = now
    task.visibility_timeout = visibility_deadline
    task.started_at         = now
    await db.commit()
    await db.refresh(task)
    return task


async def send_heartbeat(task_id: uuid.UUID) -> None:
    """Update last_heartbeat and extend visibility_timeout."""
    async with AsyncSessionLocal() as db:
        now = datetime.now(timezone.utc)
        deadline = now + timedelta(seconds=settings.visibility_timeout_sec)
        await db.execute(
            update(Task)
            .where(Task.id == task_id, Task.worker_id == WORKER_ID)
            .values(last_heartbeat=now, visibility_timeout=deadline)
        )
        await db.commit()


async def complete_task(db: AsyncSession, task: Task, result: dict) -> None:
    task.status       = TaskStatus.COMPLETED
    task.completed_at = datetime.now(timezone.utc)
    task.payload      = json.dumps({**json.loads(task.payload), "_result": result})
    await db.commit()


async def fail_task(
    db:    AsyncSession,
    task:  Task,
    error: str,
    redis: aioredis.Redis,
) -> None:
    """
    If retries remain → reset to PENDING + re-enqueue.
    Otherwise → FAILED (then watchdog may move to DEAD_LETTER).
    """
    task.retry_count += 1
    task.error_msg    = error[:2000]  # truncate

    if task.retry_count <= task.max_retries:
        log.warning(
            "Task %s failed (attempt %d/%d) — re-queuing. Error: %s",
            task.id, task.retry_count, task.max_retries, error[:120],
        )
        task.status             = TaskStatus.PENDING
        task.worker_id          = None
        task.visibility_timeout = None
        await db.commit()
        await requeue(redis, task.id, task.priority)
    else:
        log.error(
            "Task %s exhausted retries (%d) — marking FAILED. Error: %s",
            task.id, task.max_retries, error[:120],
        )
        task.status = TaskStatus.FAILED
        await db.commit()


async def process_one(task_id_str: str, redis: aioredis.Redis, sem: asyncio.Semaphore) -> None:
    """Full lifecycle for a single task, bounded by semaphore."""
    async with sem:
        task_id = uuid.UUID(task_id_str)
        heartbeat_task: asyncio.Task | None = None

        async with AsyncSessionLocal() as db:
            task = await claim_task(db, task_id)
            if task is None:
                log.debug("Task %s already claimed — skipping.", task_id)
                return

            log.info("▶ [%s] claimed task %s (type=%s priority=%d)",
                     WORKER_ID, task.id, task.task_type, task.priority)

            payload    = json.loads(task.payload)
            task_type  = task.task_type
            task_id_cp = task.id
            priority   = task.priority

        # ── Heartbeat coroutine (runs in background while task executes) ──
        async def heartbeat_loop():
            while True:
                await asyncio.sleep(settings.heartbeat_interval_sec)
                try:
                    await send_heartbeat(task_id_cp)
                except Exception as exc:
                    log.warning("Heartbeat failed for %s: %s", task_id_cp, exc)

        heartbeat_task = asyncio.create_task(heartbeat_loop())

        try:
            result = await _execute_task(task_type, payload)
            # Re-open session for final state update
            async with AsyncSessionLocal() as db2:
                db2.add(await db2.get(Task, task_id_cp))
                result_row = await db2.get(Task, task_id_cp)
                await complete_task(db2, result_row, result)
            log.info("✅ Task %s COMPLETED", task_id_cp)

        except Exception as exc:
            async with AsyncSessionLocal() as db3:
                row = await db3.get(Task, task_id_cp)
                await fail_task(db3, row, str(exc), redis)

        finally:
            if heartbeat_task:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except asyncio.CancelledError:
                    pass


# ── Main polling loop ─────────────────────────────────────────────────────────

class Worker:
    def __init__(self):
        self._running = True
        self._sem     = asyncio.Semaphore(settings.worker_concurrency)
        self._active: set[asyncio.Task] = set()

    def stop(self, *_):
        log.info("🛑 Shutdown signal received — draining in-flight tasks…")
        self._running = False

    async def run(self) -> None:
        log.info("🚀 Worker %s starting (concurrency=%d)", WORKER_ID, settings.worker_concurrency)
        redis = await get_async_redis()

        try:
            while self._running:
                # Don't poll faster than we can process
                if len(self._active) >= settings.worker_concurrency:
                    await asyncio.sleep(settings.poll_interval_sec)
                    self._active = {t for t in self._active if not t.done()}
                    continue

                task_id_str = await dequeue(redis)
                if task_id_str is None:
                    await asyncio.sleep(settings.poll_interval_sec)
                    continue

                t = asyncio.create_task(process_one(task_id_str, redis, self._sem))
                self._active.add(t)
                t.add_done_callback(self._active.discard)

            # Drain remaining tasks after stop signal
            if self._active:
                log.info("Waiting for %d in-flight tasks to finish…", len(self._active))
                await asyncio.gather(*self._active, return_exceptions=True)

        finally:
            await redis.aclose()
            log.info("👋 Worker %s exited cleanly.", WORKER_ID)


async def main():
    # Ensure schema exists (idempotent)
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, create_tables)

    worker = Worker()
    loop.add_signal_handler(signal.SIGTERM, worker.stop)
    loop.add_signal_handler(signal.SIGINT,  worker.stop)
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())