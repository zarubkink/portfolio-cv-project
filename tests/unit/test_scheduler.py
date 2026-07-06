"""Unit tests for src.services.scheduler.

The scheduler depends on a real database engine and async session, so we
patch them out with AsyncMock and drive the public methods directly.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services import scheduler as scheduler_module
from src.services.exceptions import VideoProcessError
from src.services.scheduler import VideoRetryScheduler


def _make_video_file(
    *,
    id: int,
    status: str,
    retry_count: int | None,
    updated_at: datetime | None = None,
    storage_uri: str = "/data/videos/test.mp4",
) -> SimpleNamespace:
    return SimpleNamespace(
        id=id,
        status=status,
        retry_count=retry_count,
        storage_uri=storage_uri,
        updated_at=updated_at or datetime.now(UTC),
    )


@pytest.fixture
def reset_scheduler_singleton(monkeypatch):
    """Reset the process-local singleton between tests."""
    if hasattr(scheduler_module.VideoRetryScheduler, "_instance"):
        del scheduler_module.VideoRetryScheduler._instance
    yield
    if hasattr(scheduler_module.VideoRetryScheduler, "_instance"):
        del scheduler_module.VideoRetryScheduler._instance


@pytest.fixture
def fake_session():
    """An AsyncMock that also works as an async context manager."""
    session = MagicMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.commit = AsyncMock(return_value=None)
    session.rollback = AsyncMock(return_value=None)
    return session


@pytest.mark.unit
async def test_mark_stale_videos_returns_count(
    monkeypatch, fake_session, reset_scheduler_singleton
):
    fake_video = _make_video_file(id=7, status="PROCESSING", retry_count=0)

    with (
        patch.object(scheduler_module, "engine", MagicMock()),
        patch.object(scheduler_module, "AsyncSession", return_value=fake_session),
        patch.object(scheduler_module, "VideoService") as video_service_cls,
    ):
        video_service = video_service_cls.return_value
        video_service.repo.get_stale_videos = AsyncMock(side_effect=[[], [fake_video]])
        video_service.update_status = AsyncMock(return_value=None)

        sched = VideoRetryScheduler()
        count = await sched._mark_stale_videos()

    assert count == 1
    video_service.update_status.assert_awaited_once()
    args, _ = video_service.update_status.call_args
    assert args[0] == 7
    assert str(args[1]) == "FAILED"
    fake_session.commit.assert_awaited_once()


@pytest.mark.unit
async def test_mark_stale_videos_empty_returns_zero(
    monkeypatch, fake_session, reset_scheduler_singleton
):
    with (
        patch.object(scheduler_module, "engine", MagicMock()),
        patch.object(scheduler_module, "AsyncSession", return_value=fake_session),
        patch.object(scheduler_module, "VideoService") as video_service_cls,
    ):
        video_service = video_service_cls.return_value
        video_service.repo.get_stale_videos = AsyncMock(return_value=[])

        sched = VideoRetryScheduler()
        count = await sched._mark_stale_videos()

    assert count == 0
    fake_session.commit.assert_not_awaited()


@pytest.mark.unit
async def test_process_failed_videos_dispatches(
    monkeypatch, fake_session, reset_scheduler_singleton
):
    failed_video = _make_video_file(
        id=42,
        status="FAILED",
        retry_count=1,
        storage_uri="/data/videos/x.mp4",
    )

    async def _fake_handle(**kwargs):
        return None

    monkeypatch.setattr(
        scheduler_module,
        "process_video_with_error_handling",
        AsyncMock(side_effect=_fake_handle),
    )

    with (
        patch.object(scheduler_module, "engine", MagicMock()),
        patch.object(scheduler_module, "AsyncSession", return_value=fake_session),
        patch.object(scheduler_module, "VideoService") as video_service_cls,
    ):
        video_service = video_service_cls.return_value
        video_service.repo.get_failed_videos_within_limit = AsyncMock(
            return_value=[failed_video]
        )
        video_service.repo.get_failed_videos_unlimited = AsyncMock(return_value=[])

        sched = VideoRetryScheduler()
        count = await sched._process_failed_videos()

    assert count == 1
    scheduler_module.process_video_with_error_handling.assert_awaited_once()
    kwargs = scheduler_module.process_video_with_error_handling.await_args.kwargs
    assert kwargs["video_id"] == 42
    assert kwargs["storage_uri"] == "/data/videos/x.mp4"
    assert kwargs["is_retry"] is True


@pytest.mark.unit
async def test_process_failed_videos_empty_returns_zero(
    monkeypatch, fake_session, reset_scheduler_singleton
):
    with (
        patch.object(scheduler_module, "engine", MagicMock()),
        patch.object(scheduler_module, "AsyncSession", return_value=fake_session),
        patch.object(scheduler_module, "VideoService") as video_service_cls,
    ):
        video_service = video_service_cls.return_value
        video_service.repo.get_failed_videos_within_limit = AsyncMock(return_value=[])
        video_service.repo.get_failed_videos_unlimited = AsyncMock(return_value=[])

        sched = VideoRetryScheduler()
        count = await sched._process_failed_videos()

    assert count == 0


@pytest.mark.unit
async def test_handle_retry_failure_retriable_marks_unlimited(
    monkeypatch, fake_session, reset_scheduler_singleton
):
    """Errors classified as retriable-without-limit set retry_count to NULL."""
    fake_video = _make_video_file(id=10, status="FAILED", retry_count=0)

    with (
        patch.object(scheduler_module, "engine", MagicMock()),
        patch.object(scheduler_module, "AsyncSession", return_value=fake_session),
        patch.object(scheduler_module, "VideoService") as video_service_cls,
    ):
        video_service = video_service_cls.return_value
        video_service.get = AsyncMock(return_value=fake_video)
        video_service.mark_unlimited_retry = AsyncMock(return_value=None)

        sched = VideoRetryScheduler()
        await sched._handle_retry_failure(
            task_id="t1",
            video_id=10,
            storage_uri="/x.mp4",
            current_retry_count=0,
            is_unlimited=False,
            error=VideoProcessError("decoder hiccup"),
            session=fake_session,
        )

    video_service.mark_unlimited_retry.assert_awaited_once_with(10)
    fake_session.commit.assert_awaited_once()


@pytest.mark.unit
async def test_handle_retry_failure_increments_within_limit(
    monkeypatch, fake_session, reset_scheduler_singleton
):
    """Below the cap, the scheduler only logs — the handler already incremented.

    The post-increment retry_count is read straight off the row, so we
    pass a value that is still strictly below max_retry_attempts.
    """
    fake_video = _make_video_file(id=11, status="FAILED", retry_count=1)

    with (
        patch.object(scheduler_module, "engine", MagicMock()),
        patch.object(scheduler_module, "AsyncSession", return_value=fake_session),
        patch.object(scheduler_module, "VideoService") as video_service_cls,
    ):
        video_service = video_service_cls.return_value
        video_service.get = AsyncMock(return_value=fake_video)
        video_service.increment_retry_count = AsyncMock(return_value=None)
        video_service.mark_permanently_failed = AsyncMock(return_value=None)

        sched = VideoRetryScheduler()
        await sched._handle_retry_failure(
            task_id="t1",
            video_id=11,
            storage_uri="/x.mp4",
            current_retry_count=0,
            is_unlimited=False,
            error=RuntimeError("boom"),
            session=fake_session,
        )

    video_service.mark_permanently_failed.assert_not_awaited()
    video_service.increment_retry_count.assert_not_awaited()
    fake_session.commit.assert_not_awaited()


@pytest.mark.unit
async def test_handle_retry_failure_marks_invalid_at_cap(
    monkeypatch, fake_session, reset_scheduler_singleton
):
    """Once retry_count reaches max, the video moves to INVALID + failed_videos/."""
    fake_video = _make_video_file(id=12, status="FAILED", retry_count=3)

    with (
        patch.object(scheduler_module, "engine", MagicMock()),
        patch.object(scheduler_module, "AsyncSession", return_value=fake_session),
        patch.object(scheduler_module, "VideoService") as video_service_cls,
    ):
        video_service = video_service_cls.return_value
        video_service.get = AsyncMock(return_value=fake_video)
        video_service.mark_permanently_failed = AsyncMock(return_value=None)
        video_service.increment_retry_count = AsyncMock(return_value=None)

        sched = VideoRetryScheduler()
        await sched._handle_retry_failure(
            task_id="t1",
            video_id=12,
            storage_uri="/x.mp4",
            current_retry_count=2,
            is_unlimited=False,
            error=RuntimeError("still broken"),
            session=fake_session,
        )

    video_service.mark_permanently_failed.assert_awaited_once_with(12)
    video_service.increment_retry_count.assert_not_awaited()
    fake_session.commit.assert_awaited_once()


@pytest.mark.unit
async def test_handle_retry_failure_vanished_video(
    monkeypatch, fake_session, reset_scheduler_singleton
):
    """If the row is gone we silently skip."""
    with (
        patch.object(scheduler_module, "engine", MagicMock()),
        patch.object(scheduler_module, "AsyncSession", return_value=fake_session),
        patch.object(scheduler_module, "VideoService") as video_service_cls,
    ):
        video_service = video_service_cls.return_value
        video_service.get = AsyncMock(return_value=None)
        video_service.mark_unlimited_retry = AsyncMock(return_value=None)
        video_service.increment_retry_count = AsyncMock(return_value=None)
        video_service.mark_permanently_failed = AsyncMock(return_value=None)

        sched = VideoRetryScheduler()
        await sched._handle_retry_failure(
            task_id="t1",
            video_id=999,
            storage_uri="/x.mp4",
            current_retry_count=0,
            is_unlimited=False,
            error=RuntimeError("missing"),
            session=fake_session,
        )

    video_service.mark_unlimited_retry.assert_not_awaited()
    video_service.increment_retry_count.assert_not_awaited()
    video_service.mark_permanently_failed.assert_not_awaited()


@pytest.mark.unit
async def test_process_single_retries_via_handler(
    monkeypatch, fake_session, reset_scheduler_singleton
):
    """_process_single must call the shared handler with is_retry=True."""
    monkeypatch.setattr(
        scheduler_module,
        "process_video_with_error_handling",
        AsyncMock(return_value=None),
    )

    with (
        patch.object(scheduler_module, "engine", MagicMock()),
        patch.object(scheduler_module, "AsyncSession", return_value=fake_session),
    ):
        sched = VideoRetryScheduler()
        await sched._process_single(
            video_id=1,
            storage_uri="/x.mp4",
            current_retry_count=0,
            is_unlimited=False,
        )

    scheduler_module.process_video_with_error_handling.assert_awaited_once()
    kwargs = scheduler_module.process_video_with_error_handling.await_args.kwargs
    assert kwargs["is_retry"] is True
    assert kwargs["video_id"] == 1
    assert kwargs["storage_uri"] == "/x.mp4"
    assert isinstance(kwargs["task_id"], str)


@pytest.mark.unit
async def test_process_single_routes_failure_to_handler(
    monkeypatch, fake_session, reset_scheduler_singleton
):
    """A failure inside the handler must trigger _handle_retry_failure."""
    fake_video = _make_video_file(id=2, status="FAILED", retry_count=0)

    monkeypatch.setattr(
        scheduler_module,
        "process_video_with_error_handling",
        AsyncMock(side_effect=RuntimeError("worker died")),
    )

    with (
        patch.object(scheduler_module, "engine", MagicMock()),
        patch.object(scheduler_module, "AsyncSession", return_value=fake_session),
        patch.object(scheduler_module, "VideoService") as video_service_cls,
        patch.object(
            VideoRetryScheduler, "_handle_retry_failure", AsyncMock()
        ) as handler,
    ):
        video_service = video_service_cls.return_value
        video_service.get = AsyncMock(return_value=fake_video)

        sched = VideoRetryScheduler()
        await sched._process_single(
            video_id=2,
            storage_uri="/x.mp4",
            current_retry_count=0,
            is_unlimited=False,
        )

    handler.assert_awaited_once()
    kwargs = handler.await_args.kwargs
    assert kwargs["video_id"] == 2
    assert isinstance(kwargs["error"], RuntimeError)


@pytest.mark.unit
async def test_tick_aggregates_counts(monkeypatch, reset_scheduler_singleton):
    """tick() should return the sum of both passes."""
    sched = VideoRetryScheduler()
    sched._mark_stale_videos = AsyncMock(return_value=3)
    sched._process_failed_videos = AsyncMock(return_value=2)

    summary = await sched.tick()

    assert summary == {"stale_marked": 3, "retried": 2}


@pytest.mark.unit
async def test_start_is_idempotent(monkeypatch, reset_scheduler_singleton):
    """Calling start() twice must not spin up two loops."""

    sched = VideoRetryScheduler()

    async def _noop():
        return None

    created = []

    def _fake_create_task(coro):
        created.append(coro)
        coro.close()
        return None

    sched._run_loop = _noop
    monkeypatch.setattr(scheduler_module.asyncio, "create_task", _fake_create_task)

    with patch.object(scheduler_module.asyncio, "create_task", _fake_create_task):
        await sched.start()
        running_after_first = sched._running
        await sched.start()
        running_after_second = sched._running

    assert running_after_first is True
    assert running_after_second is True
    assert len(created) == 1


@pytest.mark.unit
async def test_stop_cancels_loop(monkeypatch, reset_scheduler_singleton):
    """stop() should set _running=False and cancel the task if present."""
    sched = VideoRetryScheduler()
    sched._running = True

    cancelled = {"value": False}

    class _FakeTask:
        def cancel(self):
            cancelled["value"] = True

        def __await__(self):
            async def _noop():
                return None

            return _noop().__await__()

    sched._task = _FakeTask()

    await sched.stop()

    assert sched._running is False
    assert cancelled["value"] is True


@pytest.mark.unit
def test_is_retriable_without_limit_for_video_process_error():
    from src.services.exceptions import is_retriable_without_limit

    assert is_retriable_without_limit(VideoProcessError("x")) is True
    assert is_retriable_without_limit(RuntimeError("x")) is False
    assert is_retriable_without_limit(ValueError("x")) is False
