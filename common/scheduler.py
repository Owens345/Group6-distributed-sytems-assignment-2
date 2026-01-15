"""Simple priority scheduling for telecom-style workloads.

Two traffic classes:
- VOICE: higher priority (lower number)
- DATA: lower priority (higher number)

Demonstration goal:
- Under burst load, VOICE jobs experience lower queueing delay than DATA.
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Any, Callable, Awaitable, Optional

@dataclass
class Job:
    priority: int
    created_at: float
    payload: Any
    fut: asyncio.Future

class PriorityScheduler:
    def __init__(self):
        self._q: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self, worker: Callable[[Any], Awaitable[Any]]):
        if self._running:
            return
        self._running = True

        async def loop():
            while self._running:
                priority, created_at, job = await self._q.get()
                if job.fut.cancelled():
                    continue
                try:
                    res = await worker(job.payload)
                    if not job.fut.done():
                        job.fut.set_result(res)
                except Exception as e:
                    if not job.fut.done():
                        job.fut.set_exception(e)

        self._task = asyncio.create_task(loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()

    async def submit(self, payload: Any, priority: int):
        fut = asyncio.get_event_loop().create_future()
        job = Job(priority=priority, created_at=time.time(), payload=payload, fut=fut)
        await self._q.put((priority, job.created_at, job))
        res = await fut
        q_delay = time.time() - job.created_at
        return res, q_delay
