"""Admin endpoint for the retry scheduler.

Exposes a single ``POST /v1/admin/scheduler/tick`` that runs one pass of
the scheduler synchronously. Useful in tests and for on-demand recovery
without waiting for the next cron tick.
"""

from fastapi import APIRouter

from src.services.scheduler import scheduler

router = APIRouter(prefix="/admin/scheduler", tags=["admin"])


@router.post("/tick")
async def tick_now() -> dict:
    """Run one scheduler pass and return counts of what was processed."""
    summary = await scheduler.tick()
    return {"status": "ok", **summary}
