"""visits — state-machine aggregated rows per (tractor, station)

Revision ID: 0005_visits
Revises: 0003_events
Create Date: 2026-07-06 15:30:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0005_visits"
down_revision: str | None = "0003_events"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the visits table, ENUM type, and indexes.

    See PROJECT_SCAFFOLD_PROMPT.md sections 3.1 and 3.8 for the design
    rationale: a single open visit per (tractor_id, station_id), enforced
    via partial UNIQUE index ``uq_visit_open`` so that CLOSED visits can
    stack up over time while ABSENT is implied by the row's absence.
    """
    visit_state = postgresql.ENUM(
        "ENTERING",
        "PRESENT",
        "LEAVING",
        "CLOSED",
        name="visit_state",
        create_type=False,
    )
    bind = op.get_bind()
    visit_state.create(bind, checkfirst=True)

    op.create_table(
        "visits",
        sa.Column("tractor_id", sa.Integer(), nullable=False),
        sa.Column("station_id", sa.Integer(), nullable=False),
        sa.Column(
            "state",
            visit_state,
            nullable=False,
            server_default="ENTERING",
        ),
        sa.Column("arrived_at", sa.DateTime(), nullable=True),
        sa.Column("departed_at", sa.DateTime(), nullable=True),
        sa.Column("last_seen_at", sa.DateTime(), nullable=True),
        sa.Column("entry_event_id", sa.Integer(), nullable=True),
        sa.Column("exit_event_id", sa.Integer(), nullable=True),
        sa.Column("last_event_id", sa.Integer(), nullable=True),
        sa.Column(
            "entry_seen_seconds",
            sa.Float(),
            nullable=False,
            server_default=sa.text("0"),
        ),
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
        # GENERATED: only meaningful for CLOSED visits; NULL otherwise.
        sa.Column(
            "duration_seconds",
            sa.Float(),
            sa.Computed(
                "CASE WHEN state = 'CLOSED' "
                "AND arrived_at IS NOT NULL AND departed_at IS NOT NULL "
                "THEN EXTRACT(EPOCH FROM (departed_at - arrived_at)) "
                "ELSE NULL END",
                persisted=True,
            ),
            nullable=True,
        ),
        sa.CheckConstraint(
            "arrived_at IS NOT NULL OR state = 'ENTERING'",
            name="chk_visit_arrival",
        ),
        sa.CheckConstraint(
            "(state = 'CLOSED') = (departed_at IS NOT NULL)",
            name="chk_visit_departure",
        ),
        sa.CheckConstraint(
            "departed_at IS NULL OR arrived_at IS NULL OR arrived_at < departed_at",
            name="chk_visit_times",
        ),
        sa.CheckConstraint(
            "entry_seen_seconds >= 0",
            name="chk_visit_entry_seen_nonneg",
        ),
        sa.ForeignKeyConstraint(["tractor_id"], ["tractors.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["station_id"], ["stations.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["entry_event_id"], ["events.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["exit_event_id"], ["events.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["last_event_id"], ["events.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )

    # Hot query: «Где сейчас трактор X?»
    op.create_index(
        "ix_visits_active_tractor",
        "visits",
        ["tractor_id"],
        unique=False,
        postgresql_where=("state IN ('ENTERING', 'PRESENT', 'LEAVING')"),
    )
    # Hot query: «Кто сейчас на станции Y?»
    op.create_index(
        "ix_visits_active_station",
        "visits",
        ["station_id"],
        unique=False,
        postgresql_where=("state IN ('ENTERING', 'PRESENT', 'LEAVING')"),
    )
    # History: «История визитов трактора X»
    op.create_index(
        "ix_visits_tractor_time",
        "visits",
        ["tractor_id", sa.text("arrived_at DESC")],
        unique=False,
        postgresql_where="state = 'CLOSED'",
    )
    # History: «История визитов на станцию Y»
    op.create_index(
        "ix_visits_station_time",
        "visits",
        ["station_id", sa.text("arrived_at DESC")],
        unique=False,
        postgresql_where="state = 'CLOSED'",
    )
    # Partial UNIQUE: only one OPEN visit per (tractor, station).
    op.create_index(
        "uq_visit_open",
        "visits",
        ["tractor_id", "station_id"],
        unique=True,
        postgresql_where="state <> 'CLOSED'",
    )


def downgrade() -> None:
    op.drop_index("uq_visit_open", table_name="visits")
    op.drop_index("ix_visits_station_time", table_name="visits")
    op.drop_index("ix_visits_tractor_time", table_name="visits")
    op.drop_index("ix_visits_active_station", table_name="visits")
    op.drop_index("ix_visits_active_tractor", table_name="visits")
    op.drop_table("visits")
    op.execute("DROP TYPE IF EXISTS visit_state")
