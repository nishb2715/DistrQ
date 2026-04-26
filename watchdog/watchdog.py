"""
DistrQ — Watchdog Service

The Watchdog is the system's "immune system." It runs as a standalone process
and performs two sweeps on a configurable interval:

  Sweep 1 — Visibility Timeout Recovery
    Finds tasks WHERE status = 'PROCESSING'
                  AND visibility_timeout < NOW()
    → These workers died or hung without sending a heartbeat extension.
    → Reset to PENDING + re-enqueue in Redis.

  Sweep 2 — Stale Heartbeat Recovery (belt-and-suspenders)
    Finds tasks WHERE status = 'PROCESSING'
                  AND last_heartbeat < NOW() - STALE_THRESHOLD
    → Belt-and-suspenders check for workers that are alive but stuck.
    → Same recovery action.

  Sweep 3 — Failed → Dead Letter
    Finds tasks WHERE status = 'FAILED'
                  AND retry_count >= max_retries
    → Move to DEAD_LETTER for human review / alerting.

Zero Task Loss guarantee:
  As long as the watchdog runs, no task stays in a zombie PROCESSING state
  indefinitely. Recovery latency = WATCHDOG_INTERVAL (default 10s).
"""

import asyncio
import logging
import signal
import sys
from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, select, update

sys.path.insert(0, "/app")

from shared.config import settings
from shared.database import AsyncSessionLocal, create_tables
from shared.models import Task, TaskStatus
from shared.queue import get_async_redis, requeue

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] watchdog — %(message)s",
)
log = logging.getLogger("distrq.watchdog")


class Watchdog:
    def __init__(self):
        self._running = True

    def stop(self, *_):
        log.info("Watchdog shutting down…")
        self._running = False

    # ── Sweep 1 & 2 combined: recover zombie tasks ────────────────────────

    async def recover_zombie_tasks(self, redis) -> int:
        """
        Find all PROCESSING tasks that have either:
          a) exceeded visibility_timeout, OR
          b) not sent a heartbeat within STALE_THRESHOLD seconds

        Returns count of recovered tasks.
        """
        now            = datetime.now(timezone.utc)
        stale_cutoff   = now - timedelta(seconds=settings.stale_threshold_sec)
        recovered      = 0

        async with AsyncSessionLocal() as db:
            # Combined query — any PROCESSING task that looks dead
            result = await db.execute(
                select(Task).where(
                    and_(
                        Task.status == TaskStatus.PROCESSING,
                        (
                            (Task.visibility_timeout < now) |
                            (Task.last_heartbeat < stale_cutoff) |
                            (Task.last_heartbeat == None)  # noqa: E711
                        ),
                    )
                ).with_for_update(skip_locked=True)
            )
            zombies = result.scalars().all()

            for task in zombies:
                old_worker = task.worker_id
                task.status             = TaskStatus.PENDING
                task.worker_id          = None
                task.visibility_timeout = None
                task.last_heartbeat     = None

                log.warning(
                    "🧟 Zombie recovered: task=%s worker=%s "
                    "heartbeat=%s visibility_timeout=%s → re-queuing",
                    task.id,
                    old_worker,
                    task.last_heartbeat,
                    task.visibility_timeout,
                )
                await requeue(redis, task.id, task.priority)
                recovered += 1

            if recovered:
                await db.commit()

        return recovered

    # ── Sweep 3: promote FAILED → DEAD_LETTER ────────────────────────────

    async def promote_dead_letters(self) -> int:
        """
        Tasks that have exhausted all retries and are still FAILED
        get promoted to DEAD_LETTER for human inspection.
        """
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(Task).where(
                    and_(
                        Task.status == TaskStatus.FAILED,
                        Task.retry_count >= Task.max_retries,
                    )
                ).with_for_update(skip_locked=True)
            )
            failed_tasks = result.scalars().all()
            count = len(failed_tasks)

            for task in failed_tasks:
                task.status = TaskStatus.DEAD_LETTER
                log.error(
                    "💀 Dead letter: task=%s type=%s retries=%d/%d error=%s",
                    task.id, task.task_type,
                    task.retry_count, task.max_retries,
                    (task.error_msg or "")[:120],
                )

            if count:
                await db.commit()

        return count

    # ── Main loop ─────────────────────────────────────────────────────────

    async def run(self) -> None:
        log.info(
            "🔍 Watchdog started (interval=%ds stale_threshold=%ds)",
            settings.watchdog_interval_sec,
            settings.stale_threshold_sec,
        )
        redis = await get_async_redis()

        try:
            while self._running:
                try:
                    zombies = await self.recover_zombie_tasks(redis)
                    dead    = await self.promote_dead_letters()

                    if zombies or dead:
                        log.info(
                            "Sweep complete — recovered=%d dead_lettered=%d",
                            zombies, dead,
                        )
                    else:
                        log.debug("Sweep complete — all clean.")

                except Exception as exc:
                    log.exception("Watchdog sweep error: %s", exc)

                await asyncio.sleep(settings.watchdog_interval_sec)

        finally:
            await redis.aclose()
            log.info("Watchdog exited cleanly.")


async def main():
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, create_tables)

    watchdog = Watchdog()
    loop.add_signal_handler(signal.SIGTERM, watchdog.stop)
    loop.add_signal_handler(signal.SIGINT,  watchdog.stop)
    await watchdog.run()


if __name__ == "__main__":
    asyncio.run(main())