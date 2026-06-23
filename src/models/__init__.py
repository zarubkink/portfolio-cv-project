"""Аггрегатор моделей — гарантирует, что SQLModel.metadata соберёт все таблицы."""

from src.models.base import BaseFields
from src.models.event import Event
from src.models.station import Station
from src.models.tractor import Tractor
from src.models.video_file import VideoFile

__all__ = ["BaseFields", "Event", "Station", "Tractor", "VideoFile"]
