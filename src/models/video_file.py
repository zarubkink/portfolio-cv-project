from sqlalchemy import Column, ForeignKey, LargeBinary
from sqlmodel import Field, Index

from src.models.base import BaseFields
from src.schemas.video_file import VideoFileBase


class VideoFile(BaseFields, VideoFileBase, table=True):
    __tablename__ = "video_files"
    __table_args__ = (
        Index(
            "ix_video_files_station_started",
            "station_id",
            "started_at",
        ),
        Index(
            "ix_video_files_status",
            "status",
            postgresql_where=("status IN ('FAILED', 'PROCESSING')"),
        ),
    )

    station_id: int | None = Field(
        default=None,
        sa_column=Column(ForeignKey("stations.id", ondelete="RESTRICT"), nullable=True),
    )

    content_hash: bytes = Field(
        sa_column=Column(
            LargeBinary(length=32),
            nullable=False,
            unique=True,
        ),
        description="SHA-256 хэш видеофайла (32 байта).",
    )

    events_found: int = Field(default=0)
