import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from loguru import logger
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlmodel.ext.asyncio.session import AsyncSession

from src.config.database import settings as database_settings
from src.config.logging import logging_settings
from src.config.scheduler import ensure_scheduler_dirs
from src.config.visit import visit_settings
from src.dependencies import engine
from src.logging_setup import configure_logging
from src.router import router
from src.services.scheduler import scheduler
from src.services.visit_service import VisitService

configure_logging(logging_settings, filename="api.log")


async def _visit_stale_check_loop() -> None:
    """Periodically close stale visits. Runs forever until cancelled."""
    interval = max(visit_settings.stale_check_interval_seconds, 0.5)
    while True:
        try:
            await asyncio.sleep(interval)
            async with AsyncSession(engine) as session:
                closed = await VisitService(session).check_stale_visits()
                if closed:
                    logger.info(f"visit stale check closed {closed} visit(s)")
        except asyncio.CancelledError:
            break
        except Exception as exc:  # pragma: no cover - defensive
            logger.opt(exception=exc).error("visit stale check failed")


@asynccontextmanager
async def lifespan(app: FastAPI):
    database_settings.videos_storage.mkdir(parents=True, exist_ok=True)
    database_settings.failed_videos_folder.mkdir(parents=True, exist_ok=True)
    ensure_scheduler_dirs()

    async with AsyncSession(engine) as session:
        await VisitService(session).recover_open_visits()

    stale_task = asyncio.create_task(_visit_stale_check_loop())
    await scheduler.start()
    logger.info("Agro Tracking API started")
    try:
        yield
    finally:
        stale_task.cancel()
        try:
            await stale_task
        except asyncio.CancelledError:
            pass
        await scheduler.stop()
        logger.info("Agro Tracking API stopped")


app = FastAPI(title="Agro Tracking API", version="0.1.0", lifespan=lifespan)
app.include_router(router)


@app.get("/health", tags=["health"])
async def health():
    """Проверяет API и подключение к PostgreSQL."""
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
    except SQLAlchemyError as e:
        logger.opt(exception=e).error("DB healthcheck failed")
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "db": "unreachable", "detail": str(e)},
        )
    return {"status": "ok", "db": "ok"}


@app.exception_handler(Exception)
async def generic_exception_handler(request, exc):
    logger.opt(exception=exc).error("Unexpected error")
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
