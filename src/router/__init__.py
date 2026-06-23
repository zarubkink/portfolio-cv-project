from fastapi import APIRouter

from src.router.v1 import api_router

router = APIRouter()
router.include_router(api_router)
