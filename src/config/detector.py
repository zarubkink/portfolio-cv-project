from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DetectorSettings(BaseSettings):
    """Параметры детектора движения и ArUco."""

    model_config = SettingsConfigDict(
        env_file="stack.env", env_file_encoding="utf-8", extra="ignore"
    )

    aruco_dict: str = Field(default="DICT_4X4_50")
    min_aruco_side_pixels: int = Field(default=30)
    confidence_threshold: float = Field(default=0.6)
    marker_size_cm: float = Field(default=25.0)

    velocity_px_threshold: float = Field(default=3.0)
    mog2_mass_min: int = Field(default=1000)

    min_contour_area: int = Field(default=5000)
    mog2_history: int = Field(default=200)
    mog2_var_threshold: int = Field(default=50)


detector_settings = DetectorSettings()
