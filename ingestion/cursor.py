import asyncio
import os
import shutil
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from loguru import logger

from ingestion.config import config
from ingestion.exceptions import StationDirectoryDoesntExist
from src.utils import hash_large_file


@dataclass
class StationVideoFile:
    file_path: Path
    station_code: str
    content_hash: bytes = field(repr=False)
    begin_ts: datetime = field(repr=False)

    @classmethod
    def build(cls, file_path: Path, station_code: str) -> "StationVideoFile":
        return cls(
            file_path=file_path,
            station_code=station_code,
            content_hash=hash_large_file(file_path),
            begin_ts=datetime.strptime(file_path.stem, config.timestamp_format),
        )

    def __repr__(self):
        return (
            f"StationVideoFile(station={self.station_code!r}, "
            f"file={self.file_path.name!r}, hash={self.content_hash.hex()[:16]})"
        )

    @property
    def name(self) -> str:
        return self.file_path.name

    def is_valid(self) -> bool:
        """Имя файла: STATION_XX/YYYYMMDD_HHMMSS.<ext>."""
        if self.file_path.suffix.lower() not in config.allowed_extensions:
            return False
        stem = self.file_path.stem
        try:
            datetime.strptime(stem, config.timestamp_format)
        except ValueError:
            return False
        return True

    @property
    def storage_path(self) -> Path:
        return (
            config.videos_storage
            / f"{self.content_hash.hex()}{self.file_path.suffix.lower()}"
        )

    def copy_to_cold_storage(self) -> Path:
        new_path = Path(shutil.copyfile(self.file_path, self.storage_path))
        os.chmod(new_path, 0o666)
        return new_path

    def unlink(self) -> None:
        self.file_path.unlink(missing_ok=True)


class StationDirectory:
    """Соответствует одной папке STATION_XX/ в stations_root."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.station_code = self.path.name
        if not self.path.exists():
            raise StationDirectoryDoesntExist(self.path)

    async def aget_contiguous_iterator(self) -> AsyncIterator[StationVideoFile]:
        while True:
            found_any = False
            for file_path in sorted(self.path.iterdir()):
                if not (file_path.is_file()):
                    continue
                if file_path.suffix.lower() not in config.allowed_extensions:
                    continue
                stem = file_path.stem
                try:
                    datetime.strptime(stem, config.timestamp_format)
                except ValueError:
                    logger.debug(f"Skip non-timestamped file: {file_path.name}")
                    continue
                found_any = True
                yield StationVideoFile.build(file_path, self.station_code)
            if not found_any:
                await asyncio.sleep(config.cursor_sleep_sec)


def collect_station_directories(
    root: Path, current: set[Path]
) -> tuple[set[Path], set[Path], set[Path]]:
    """Возвращает (new, removed, union) — паттерн sbr."""
    if not root.exists():
        return set(), current, current
    new_dirs = {
        p.resolve()
        for p in root.iterdir()
        if p.is_dir() and p.name.startswith("STATION_")
    }
    cur_resolved = {p.resolve() for p in current}
    return (
        new_dirs - cur_resolved,
        cur_resolved - new_dirs,
        new_dirs,
    )
