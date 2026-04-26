import json
import os
import time
from typing import Optional
from uuid import UUID

import redis.asyncio as aioredis
import redis as syncredis

REDIS_URL    = os.getenv("REDIS_URL", "redis://redis:6379/0")
QUEUE_KEY    = "distrq:queue"
STATS_KEY    = "distrq:stats"
MAX_PRIORITY = 100

def get_sync_redis():
    return syncredis.from_url(REDIS_URL, decode_responses=True)

async def get_async_redis():
    return aioredis.from_url(REDIS_URL, decode_responses=True)

def _score(priority: int) -> float:
    epoch_us = int(time.time() * 1_000_000)
    return (MAX_PRIORITY - priority) * 1e12 + epoch_us

async def enqueue(redis, task_id, priority: int = 5):
    await redis.zadd(QUEUE_KEY, {str(task_id): _score(priority)})
    await redis.hincrbyfloat(STATS_KEY, "enqueued", 1)

async def enqueue_bulk(redis, tasks: list):
    if not tasks:
        return
    mapping = {str(tid): _score(pri) for tid, pri in tasks}
    pipe = redis.pipeline(transaction=False)
    pipe.zadd(QUEUE_KEY, mapping)
    pipe.hincrbyfloat(STATS_KEY, "enqueued", len(tasks))
    await pipe.execute()

async def dequeue(redis) -> Optional[str]:
    result = await redis.zpopmin(QUEUE_KEY, count=1)
    if not result:
        return None
    task_id_str, _ = result[0]
    await redis.hincrbyfloat(STATS_KEY, "dequeued", 1)
    return task_id_str

async def requeue(redis, task_id, priority: int = 5):
    await redis.zadd(QUEUE_KEY, {str(task_id): _score(priority)})

async def queue_length(redis) -> int:
    return await redis.zcard(QUEUE_KEY)

async def get_stats(redis) -> dict:
    raw = await redis.hgetall(STATS_KEY)
    return {k: int(float(v)) for k, v in raw.items()}

async def ping_redis(redis) -> bool:
    try:
        return await redis.ping()
    except Exception:
        return False
