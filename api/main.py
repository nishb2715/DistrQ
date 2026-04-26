"""
DistrQ — FastAPI Producer Service

Endpoints:
  POST /tasks            – submit a single task
  POST /tasks/bulk       – submit up to 10 000 tasks in one call
  GET  /tasks/{id}       – inspect task state
  GET  /tasks            – list tasks (paginated, filterable)
  GET  /stats            – queue & DB statistics
  GET  /health           – liveness + readiness probe
  GET  /docs             – Swagger UI (auto)
"""

import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional

import redis.asyncio as aioredis
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

# ── Import shared modules (installed as editable or via PYTHONPATH) ──────────
import sys
sys.path.insert(0, "/app")

from shared.config import settings
from shared.database import create_tables, get_db, ping_db
from shared.models import Task, TaskPriority, TaskStatus
from shared.queue import (
    enqueue, enqueue_bulk, get_async_redis,
    get_stats, ping_redis, queue_length,
)


# ── Lifespan ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: ensure tables exist, warm up connection pools
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, create_tables)
    app.state.redis = await get_async_redis()
    print("✅ DistrQ API is ready.")
    yield
    # Shutdown
    await app.state.redis.aclose()
    print("👋 DistrQ API shut down cleanly.")


app = FastAPI(
    title="DistrQ",
    description="Distributed Task Queue with Zero Task Loss guarantee",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Request / Response schemas ───────────────────────────────────────────────

class TaskCreate(BaseModel):
    task_type:   str                  = Field(...,    min_length=1, max_length=128)
    payload:     dict[str, Any]       = Field(default_factory=dict)
    priority:    int                  = Field(default=TaskPriority.NORMAL, ge=1, le=20)
    max_retries: int                  = Field(default=settings.default_max_retries, ge=0, le=10)


class BulkTaskCreate(BaseModel):
    tasks: list[TaskCreate] = Field(..., min_length=1, max_length=10_000)


class TaskResponse(BaseModel):
    id:           uuid.UUID
    task_type:    str
    status:       TaskStatus
    priority:     int
    retry_count:  int
    max_retries:  int
    worker_id:    Optional[str]
    error_msg:    Optional[str]
    created_at:   datetime
    updated_at:   datetime
    started_at:   Optional[datetime]
    completed_at: Optional[datetime]

    model_config = {"from_attributes": True}


class BulkResponse(BaseModel):
    enqueued:   int
    task_ids:   list[uuid.UUID]
    duration_ms: float


# ── Dependency ───────────────────────────────────────────────────────────────

def get_redis(request=None) -> aioredis.Redis:
    return app.state.redis


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.post("/tasks", response_model=TaskResponse, status_code=status.HTTP_201_CREATED)
async def create_task(
    body: TaskCreate,
    db:    AsyncSession  = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """Submit a single task. Persists to Postgres THEN enqueues to Redis."""
    task = Task(
        id=uuid.uuid4(),
        task_type=body.task_type,
        payload=json.dumps(body.payload),
        priority=body.priority,
        max_retries=body.max_retries,
        status=TaskStatus.PENDING,
    )
    db.add(task)
    await db.flush()            # get the id before committing
    await enqueue(redis, task.id, task.priority)
    await db.commit()
    await db.refresh(task)
    return task


@app.post("/tasks/bulk", response_model=BulkResponse, status_code=status.HTTP_201_CREATED)
async def create_tasks_bulk(
    body:  BulkTaskCreate,
    db:    AsyncSession   = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """
    High-throughput bulk ingest — single DB round-trip + single Redis pipeline.
    Tested at 10 000 tasks / call.
    """
    t0 = asyncio.get_event_loop().time()

    task_objects = [
        Task(
            id=uuid.uuid4(),
            task_type=t.task_type,
            payload=json.dumps(t.payload),
            priority=t.priority,
            max_retries=t.max_retries,
            status=TaskStatus.PENDING,
        )
        for t in body.tasks
    ]

    db.add_all(task_objects)
    await db.flush()

    # Single Redis pipeline for all enqueues
    await enqueue_bulk(redis, [(t.id, t.priority) for t in task_objects])
    await db.commit()

    duration_ms = (asyncio.get_event_loop().time() - t0) * 1000
    return BulkResponse(
        enqueued=len(task_objects),
        task_ids=[t.id for t in task_objects],
        duration_ms=round(duration_ms, 2),
    )


@app.get("/tasks/{task_id}", response_model=TaskResponse)
async def get_task(task_id: uuid.UUID, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(Task).where(Task.id == task_id))
    task = result.scalar_one_or_none()
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@app.get("/tasks", response_model=list[TaskResponse])
async def list_tasks(
    status_filter: Optional[TaskStatus] = Query(None, alias="status"),
    limit:  int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db:     AsyncSession = Depends(get_db),
):
    q = select(Task).order_by(Task.created_at.desc()).limit(limit).offset(offset)
    if status_filter:
        q = q.where(Task.status == status_filter)
    result = await db.execute(q)
    return result.scalars().all()


@app.get("/stats")
async def get_system_stats(
    db:    AsyncSession   = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    """Unified stats: DB counts per status + Redis queue depth."""
    # DB aggregation
    rows = await db.execute(
        select(Task.status, func.count(Task.id).label("n"))
        .group_by(Task.status)
    )
    db_counts = {row.status.value: row.n for row in rows}

    # Redis stats
    redis_stats  = await get_stats(redis)
    pending_q    = await queue_length(redis)

    return {
        "db":    db_counts,
        "redis": {**redis_stats, "queue_depth": pending_q},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/health")
async def health(
    db:    AsyncSession   = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    db_ok    = await ping_db()
    redis_ok = await ping_redis(redis)
    healthy  = db_ok and redis_ok
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={
            "status":    "ok" if healthy else "degraded",
            "postgres":  "up" if db_ok    else "down",
            "redis":     "up" if redis_ok else "down",
        },
    )


@app.get("/")
async def root():
    return {"service": "DistrQ", "version": "1.0.0", "docs": "/docs"}