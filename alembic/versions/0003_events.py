"""events — raw detection stream

Revision ID: 0003_events
Revises: e31b2289c092
Create Date: 2026-06-23 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0003_events"
down_revision: str | None = "e31b2289c092"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the events table.

    We use ``postgresql.ENUM`` (not the generic ``sa.Enum``) because
    the generic flavour re-emits ``CREATE TYPE`` even when the type
    already exists in the same transaction, producing
    ``DuplicateObjectError``. ``postgresql.ENUM`` plus ``create_type=False``
    on the column gives us stable, idempotent DDL.
    """
    event_type = postgresql.ENUM(
        "ENTRY",
        "EXIT",
        "DETECTED",
        name="event_type",
        create_type=False,
    )
    detector_method = postgresql.ENUM(
        "aruco",
        "yolo_aruco",
        "color_class",
        "reid",
        "fallback",
        name="detector_method",
        create_type=False,
    )

    bind = op.get_bind()
    event_type.create(bind, checkfirst=True)
    detector_method.create(bind, checkfirst=True)

    op.create_table(
        "events",
        sa.Column("video_file_id", sa.Integer(), nullable=False),
        sa.Column("tractor_id", sa.Integer(), nullable=True),
        sa.Column("aruco_id", sa.Integer(), nullable=True),
        sa.Column("event_type", event_type, nullable=False, server_default="DETECTED"),
        sa.Column(
            "detector_method", detector_method, nullable=False, server_default="aruco"
        ),
        sa.Column(
            "inside_roi",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("FALSE"),
        ),
        sa.Column("frame_number", sa.Integer(), nullable=False),
        sa.Column("timestamp_in_video", sa.Float(), nullable=False),
        sa.Column(
            "wall_clock_at",
            sa.DateTime(),
            nullable=False,
            server_default=sa.text("CURRENT_TIMESTAMP"),
        ),
        sa.Column("confidence", sa.Float(), nullable=True),
        sa.Column("bbox", sa.JSON(), nullable=True),
        sa.Column("detector_metadata", sa.JSON(), nullable=True),
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column("deleted_at", sa.DateTime(), nullable=True),
        sa.CheckConstraint("frame_number >= 0", name="chk_events_frame_nonneg"),
        sa.CheckConstraint("timestamp_in_video >= 0", name="chk_events_ts_nonneg"),
        sa.ForeignKeyConstraint(
            ["video_file_id"], ["video_files.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["tractor_id"], ["tractors.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_index(
        "ix_events_video_frame",
        "events",
        ["video_file_id", "frame_number"],
        unique=False,
    )
    op.create_index(
        "ix_events_tractor_wall",
        "events",
        ["tractor_id", "wall_clock_at"],
        unique=False,
        postgresql_where="tractor_id IS NOT NULL",
    )
    op.create_index(
        "ix_events_aruco_wall",
        "events",
        ["aruco_id", "wall_clock_at"],
        unique=False,
        postgresql_where="aruco_id IS NOT NULL",
    )
    op.create_index(
        "ix_events_inside_roi_wall",
        "events",
        ["wall_clock_at"],
        unique=False,
        postgresql_where="inside_roi = TRUE",
    )
    op.create_index(
        "ix_events_detector_method",
        "events",
        ["detector_method", "wall_clock_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_events_detector_method", table_name="events")
    op.drop_index("ix_events_inside_roi_wall", table_name="events")
    op.drop_index("ix_events_aruco_wall", table_name="events")
    op.drop_index("ix_events_tractor_wall", table_name="events")
    op.drop_index("ix_events_video_frame", table_name="events")
    op.drop_table("events")
    op.execute("DROP TYPE IF EXISTS detector_method")
    op.execute("DROP TYPE IF EXISTS event_type")
