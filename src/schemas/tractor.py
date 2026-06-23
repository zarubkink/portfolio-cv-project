from sqlmodel import Field, SQLModel


class TractorBase(SQLModel):
    """Multi-marker трактор: aruco_ids = список ArUco ID, первый = primary."""

    name: str = Field(..., max_length=255)
    model: str | None = Field(default=None, max_length=255)
    notes: str | None = None
    is_active: bool = Field(default=True)


class TractorPublic(SQLModel):
    id: int
    aruco_ids: list[int]
    primary_aruco_id: int
    name: str
    model: str | None = None
    notes: str | None = None
    is_active: bool
    created_at: str | None = None


class TractorCreate(TractorBase):
    aruco_ids: list[int] = Field(
        ...,
        min_length=1,
        description="Минимум 1 ArUco ID. Первый = primary.",
    )


class TractorUpdate(SQLModel):
    name: str | None = None
    model: str | None = None
    notes: str | None = None
    is_active: bool | None = None
    aruco_ids: list[int] | None = Field(default=None, min_length=1)


class TractorFilter(SQLModel):
    name: str | None = None
    is_active: bool | None = None
