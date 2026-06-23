from sqlalchemy import Column, Computed, Integer
from sqlalchemy.dialects.postgresql import ARRAY
from sqlmodel import Field, Index

from src.models.base import BaseFields
from src.schemas.tractor import TractorBase


class Tractor(BaseFields, TractorBase, table=True):
    __tablename__ = "tractors"
    __table_args__ = (
        Index(
            "ix_tractors_aruco_ids",
            "aruco_ids",
            postgresql_using="gin",
        ),
        Index(
            "uq_tractors_primary_aruco",
            "primary_aruco_id",
            unique=True,
        ),
    )

    aruco_ids: list[int] = Field(
        default_factory=list,
        sa_column=Column(
            "aruco_ids",
            ARRAY(Integer),
            nullable=False,
            server_default="{}",
        ),
        description="Массив ArUco-маркеров на тракторе (multi-marker).",
    )

    primary_aruco_id: int | None = Field(
        default=None,
        sa_column=Column(
            "primary_aruco_id",
            Integer,
            Computed("aruco_ids[1]", persisted=True),
            nullable=True,
        ),
        description="GENERATED: первый элемент aruco_ids.",
    )
