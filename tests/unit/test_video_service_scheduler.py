"""Unit tests for src.services.video_service helpers used by the scheduler."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.video_service import VideoService


def _make_video_file(
    *, id: int, storage_uri: str, retry_count: int | None = 0
) -> SimpleNamespace:
    return SimpleNamespace(
        id=id,
        storage_uri=storage_uri,
        retry_count=retry_count,
        status="FAILED",
    )


@pytest.fixture
def session():
    return MagicMock()


@pytest.mark.unit
async def test_mark_unlimited_retry_sets_retry_count_null(session):
    """mark_unlimited_retry should forward ``retry_count=None`` to the repo."""
    repo = MagicMock()
    repo.update_by_id = AsyncMock(return_value=None)
    service = VideoService(session)
    service.repo = repo

    await service.mark_unlimited_retry(42)

    repo.update_by_id.assert_awaited_once_with(42, {"retry_count": None})


@pytest.mark.unit
async def test_increment_retry_count_bumps_by_one(session):
    """increment_retry_count must load the row and bump retry_count by 1."""
    vf = _make_video_file(id=1, storage_uri="/x.mp4", retry_count=4)
    repo = MagicMock()
    repo.get = AsyncMock(return_value=vf)
    repo.update = AsyncMock(return_value=None)
    service = VideoService(session)
    service.repo = repo

    await service.increment_retry_count(1)

    repo.update.assert_awaited_once_with(vf, {"retry_count": 5})


@pytest.mark.unit
async def test_increment_retry_count_returns_none_when_missing(session):
    repo = MagicMock()
    repo.get = AsyncMock(return_value=None)
    service = VideoService(session)
    service.repo = repo

    result = await service.increment_retry_count(999)

    assert result is None


@pytest.mark.unit
async def test_mark_permanently_failed_moves_file_and_updates_status(tmp_path, session):
    """Move the file into failed_videos/, point storage_uri there, flip status."""
    src = tmp_path / "video.mp4"
    src.write_bytes(b"\x00" * 16)
    failed_dir = tmp_path / "failed"
    vf = _make_video_file(id=11, storage_uri=str(src))

    repo = MagicMock()
    repo.get = AsyncMock(side_effect=[vf, vf])  # move_to_failed then mark
    repo.update = AsyncMock(return_value=None)
    repo.update_by_id = AsyncMock(return_value=None)

    service = VideoService(session)
    service.repo = repo

    with patch(
        "src.services.video_service.settings",
        SimpleNamespace(failed_videos_folder=failed_dir),
    ):
        await service.mark_permanently_failed(11)

    expected_dst = failed_dir / "video.mp4.11"
    assert expected_dst.exists()
    assert not src.exists()
    # Two updates total: move_to_failed + mark_permanently_failed
    assert repo.update.await_count == 2
    final_call = repo.update.await_args_list[-1]
    final_kwargs = final_call.args[1]
    assert final_kwargs["status"].value == "INVALID"
    assert final_kwargs["storage_uri"] == str(expected_dst)
    assert "max_retry_attempts" in final_kwargs["error_message"]


@pytest.mark.unit
async def test_mark_permanently_failed_returns_none_when_vanished(tmp_path, session):
    repo = MagicMock()
    repo.get = AsyncMock(return_value=None)
    service = VideoService(session)
    service.repo = repo

    result = await service.mark_permanently_failed(404)

    assert result is None
