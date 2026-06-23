from datetime import datetime
from enum import StrEnum

from sqlmodel import Field, SQLModel


class VideoStatus(StrEnum):
    CREATED = "CREATED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    INVALID = "INVALID"


class VideoFileBase(SQLModel):
    storage_uri: str = Field(..., max_length=1024)
    started_at: datetime
    ended_at: datetime
    fps: float | None = None
    width: int | None = None
    height: int | None = None
    duration_seconds: float | None = None
    status: VideoStatus = VideoStatus.CREATED
    retry_count: int = 0
    error_message: str | None = None
    frames_processed: int = 0
    triggers_fired: int = 0


class VideoFilePublic(SQLModel):
    id: int
    station_id: int | None
    storage_uri: str
    started_at: datetime
    ended_at: datetime
    fps: float | None
    width: int | None
    height: int | None
    duration_seconds: float | None
    status: VideoStatus
    retry_count: int
    error_message: str | None
    frames_processed: int
    triggers_fired: int
    created_at: datetime | None
    updated_at: datetime | None


class VideoFileCreate(SQLModel):
    station_id: int | None = None
    storage_uri: str = Field(..., max_length=1024)
    started_at: datetime
    ended_at: datetime
    fps: float | None = None
    width: int | None = None
    height: int | None = None
    duration_seconds: float | None = None


class VideoFileUpdate(SQLModel):
    status: VideoStatus | None = None
    retry_count: int | None = None
    error_message: str | None = None
    frames_processed: int | None = None
    triggers_fired: int | None = None
    events_found: int | None = None
    fps: float | None = None
    width: int | None = None
    height: int | None = None
    duration_seconds: float | None = None


class VideoFileFilter(SQLModel):
    station_id: int | None = None
    status: VideoStatus | None = None
