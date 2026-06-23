from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field, SQLModel


class StationBase(SQLModel):
    code: str = Field(..., max_length=32, unique=True, index=True)
    name: str = Field(..., max_length=255)
    location: str | None = Field(default=None)
    video_dir: str = Field(..., max_length=512)
    roi_polygon: list[list[int]] | None = Field(
        default=None,
        sa_type=JSONB,
        description=(
            "ROI-полигон зоны станции в пикселях кадра: [[x1,y1],...,[xN,yN]]. "
            "NULL = вся рамка считается ROI."
        ),
    )
    is_entry_zone: bool = Field(default=True)
    is_active: bool = Field(default=True)


class StationPublic(StationBase):
    id: int


class StationCreate(StationBase):
    pass


class StationUpdate(SQLModel):
    code: str | None = Field(default=None, max_length=32)
    name: str | None = Field(default=None, max_length=255)
    location: str | None = None
    video_dir: str | None = Field(default=None, max_length=512)
    roi_polygon: list[list[int]] | None = Field(default=None, sa_type=JSONB)
    is_entry_zone: bool | None = None
    is_active: bool | None = None


class StationFilter(SQLModel):
    code: str | None = None
    name: str | None = None
    is_active: bool | None = None
