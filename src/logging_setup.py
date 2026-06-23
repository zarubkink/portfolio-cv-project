import sys
from pathlib import Path

from loguru import logger

from src.config.logging import LoggingSettings


def configure_logging(
    settings: LoggingSettings, filename: str | Path | None = None
) -> None:
    logger.remove()

    level = settings.log_level.upper()

    logger.add(
        sys.stderr,
        level=level,
        enqueue=True,
        backtrace=True,
        diagnose=False,
    )

    if settings.logs_to_file and filename:
        if not isinstance(filename, Path):
            filename = Path(filename)
        log_path = settings.logs_dir / filename

        log_path.parent.mkdir(parents=True, exist_ok=True)
        logger.add(
            str(log_path),
            level=level,
            rotation="10 MB",
            enqueue=True,
            backtrace=True,
            diagnose=False,
        )
