import asyncio
import signal
from datetime import UTC, datetime, timedelta
from pathlib import Path

import aiohttp
from loguru import logger

from ingestion.config import config
from ingestion.cursor import (
    StationDirectory,
    StationVideoFile,
    collect_station_directories,
)
from ingestion.exceptions import (
    RecognitionApiError,
    StationDirectoryDoesntExist,
    VideoAlreadyExistsError,
)
from src.config.logging import logging_settings
from src.logging_setup import configure_logging

configure_logging(logging_settings, filename="ingestion.log")


def to_utc_iso(dt: datetime) -> str:
    """ISO 8601 в UTC с суффиксом Z."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).isoformat()


async def forward_video_to_api(
    entry: StationVideoFile,
    http_client: aiohttp.ClientSession,
) -> None:
    """POST /v1/videos/handle с путём к cold-storage копии."""
    started = entry.begin_ts
    ended = started + timedelta(seconds=config.default_video_duration_sec)
    params = {
        "filepath": str(entry.storage_path),
        "station_code": entry.station_code,
        "started_at": to_utc_iso(started),
        "ended_at": to_utc_iso(ended),
    }
    url = str(config.api_url)
    try:
        response = await http_client.post(url, data=params)
    except TimeoutError as ex:
        raise RecognitionApiError(
            f"API request timeout for {entry.storage_path}"
        ) from ex
    except aiohttp.ClientError as ex:
        raise RecognitionApiError(
            f"API client error while forwarding {entry.storage_path}"
        ) from ex

    if response.status == 406:
        raise VideoAlreadyExistsError(f"Video already exists for {entry.storage_path}")
    if response.status >= 400:
        raise RecognitionApiError(
            f"API error {response.status} for {entry.storage_path}: {await response.text()}"
        )


async def produce_for_directory(
    semaphore: asyncio.Semaphore,
    directory: StationDirectory,
    http_client: aiohttp.ClientSession,
) -> None:
    """Producer для одной папки STATION_XX/."""
    logger.info(f"Setup producer for {directory.path}")
    try:
        while True:
            try:
                iterator = directory.aget_contiguous_iterator()
                async for item in iterator:
                    logger.debug(f"[{directory.path}] yield {item}")
                    async with semaphore:
                        # 1. Копируем в cold storage ДО отправки, чтобы файл был стабилен.
                        item.copy_to_cold_storage()
                        # 2. Отправляем в API.
                        try:
                            await forward_video_to_api(item, http_client)
                        except VideoAlreadyExistsError as e:
                            logger.info(f"[{directory.path}] duplicate, skipping: {e}")
                        except RecognitionApiError as e:
                            logger.error(f"[{directory.path}] forward failed: {e}")
                            continue
                        # 3. Удаляем исходник только после успешной отправки.
                        item.unlink()
                        logger.info(
                            f"[{directory.path}] forwarded and unlinked: {item.name}"
                        )
            except StationDirectoryDoesntExist:
                logger.warning(
                    f"Station dir vanished: {directory.path}; producer exiting"
                )
                return
            except asyncio.CancelledError:
                logger.info(f"Producer for {directory.path} cancelled")
                raise
            except Exception as e:  # pragma: no cover - defensive
                logger.opt(exception=e).exception(
                    f"Producer for {directory.path} error"
                )
                await asyncio.sleep(config.producer_error_sleep_sec)
    except asyncio.CancelledError:
        raise


async def watch_station_dirs(
    semaphore: asyncio.Semaphore,
    http_client: aiohttp.ClientSession,
    stop_event: asyncio.Event,
) -> None:
    """Следит за появлением/исчезновением папок STATION_XX и запускает producer-ов."""
    active: dict[Path, asyncio.Task] = {}
    current: set[Path] = set()

    while not stop_event.is_set():
        try:
            new, removed, _ = collect_station_directories(config.stations_root, current)
            for d in removed:
                if d in active:
                    active[d].cancel()
                    try:
                        await active[d]
                    except asyncio.CancelledError:
                        pass
                    del active[d]
                    logger.info(f"Stopped producer for {d}")
            for d in new:
                try:
                    sd = StationDirectory(d)
                except StationDirectoryDoesntExist:
                    continue
                active[d] = asyncio.create_task(
                    produce_for_directory(semaphore, sd, http_client)
                )
                logger.info(f"Started producer for {d}")
            current = set(active.keys())
        except Exception as e:  # pragma: no cover - defensive
            logger.opt(exception=e).exception("watch_station_dirs error")

        try:
            await asyncio.wait_for(
                stop_event.wait(), timeout=config.stations_watcher_sleep_sec
            )
        except TimeoutError:
            pass

    # Shutdown
    for task in active.values():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def main() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    semaphore = asyncio.Semaphore(config.num_concurrent_requests)
    timeout = aiohttp.ClientTimeout(total=config.api_request_timeout_sec)
    async with aiohttp.ClientSession(timeout=timeout) as http_client:
        logger.info(
            f"Ingestion started: stations_root={config.stations_root}, "
            f"api={config.api_url}"
        )
        await watch_station_dirs(semaphore, http_client, stop_event)
    logger.info("Ingestion stopped")


if __name__ == "__main__":
    asyncio.run(main())
