from fastapi import APIRouter

from src.router.v1.scheduler import router as scheduler_router
from src.router.v1.station import router as station_router
from src.router.v1.tractor import router as tractor_router
from src.router.v1.video import router as video_router

api_router = APIRouter(prefix="/v1")
api_router.include_router(station_router)
api_router.include_router(tractor_router)
api_router.include_router(video_router)
api_router.include_router(scheduler_router)
