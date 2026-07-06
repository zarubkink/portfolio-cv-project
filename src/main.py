from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from loguru import logger
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.config.database import settings as database_settings
from src.config.logging import logging_settings
from src.config.scheduler import get_scheduler_settings
from src.dependencies import engine
from src.logging_setup import configure_logging
from src.router import router
from src.services.scheduler import scheduler

configure_logging(logging_settings, filename="api.log")


@asynccontextmanager
async def lifespan(app: FastAPI):
    database_settings.videos_storage.mkdir(parents=True, exist_ok=True)
    database_settings.failed_videos_folder.mkdir(parents=True, exist_ok=True)
    get_scheduler_settings()
    await scheduler.start()
    logger.info("Agro Tracking API started")
    try:
        yield
    finally:
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
