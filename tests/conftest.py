"""Shared pytest fixtures for the test suite.

Integration tests run against the live compose stack (db + api +
ingestion containers must be up). The unit suite does not need these
fixtures; it uses mocks.

The fixtures here assume the API is reachable at ``$API_BASE_URL``
(default ``http://localhost:8000``) and PostgreSQL at the URL in
``stack.env``. Adjust ``API_BASE_URL`` and ``DB_URL`` if your stack is
running on a different host or port.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
import pytest_asyncio

from src.dependencies import engine

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")


@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def api_client() -> AsyncIterator[httpx.AsyncClient]:
    """A reusable httpx client pointing at the live API."""
    async with httpx.AsyncClient(base_url=API_BASE_URL, timeout=30.0) as client:
        yield client


@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def api_reachable(api_client: httpx.AsyncClient) -> None:
    """Skip the integration test if the API is not up."""
    try:
        r = await api_client.get("/health", timeout=2.0)
        if r.status_code != 200:
            pytest.skip(f"API healthcheck returned {r.status_code}")
    except (httpx.RequestError, httpx.HTTPError) as exc:
        pytest.skip(f"API not reachable at {API_BASE_URL}: {exc}")


@pytest_asyncio.fixture(loop_scope="session", scope="session")
async def db_engine():
    """A live async engine; skipped if PostgreSQL is unreachable.

    Uses the module-level engine from ``src.dependencies`` — we don't
    create a fresh one because the API containers already share
    settings and we want the test to fail fast on connection issues.
    """
    try:
        async with engine.connect() as conn:
            from sqlalchemy import text

            await conn.execute(text("SELECT 1"))
    except Exception as exc:  # pragma: no cover - skip path
        pytest.skip(f"DB not reachable: {exc}")
    yield engine


@pytest.fixture
def unique_code() -> str:
    """Random code for stations / tractors so re-runs do not collide."""
    return f"IT_{uuid.uuid4().hex[:8].upper()}"


@pytest.fixture
def smoke_video_path(tmp_path: Path, request) -> Path:
    """Render a tiny mp4 with a known ArUco marker for pipeline tests.

    Uses cv2.aruco.generateImageMarker + cv2.VideoWriter. The marker ID
    defaults to 42 (inside DICT_4X4_50); tests can override via
    ``pytest.mark.parametrize("smoke_video_path", [42], indirect=True)``
    or by writing a new fixture that wraps this one. The marker stays
    in the centre of the frame so a default ROI catches every
    detection.
    """
    aruco_id = getattr(request, "param", 42)
    import cv2
    import numpy as np

    path = tmp_path / f"integration_aruco_{aruco_id}.mp4"
    if path.exists():
        path.unlink()

    dictionary = cv2.aruco.getPredefinedDictionary(cv2.aruco.DICT_4X4_50)
    marker = cv2.cvtColor(
        cv2.aruco.generateImageMarker(dictionary, aruco_id, 140),
        cv2.COLOR_GRAY2BGR,
    )
    bg = np.full((240, 320, 3), 64, dtype=np.uint8)
    rng = np.random.default_rng(11)
    writer = cv2.VideoWriter(
        str(path), cv2.VideoWriter_fourcc(*"mp4v"), 10.0, (320, 240)
    )
    for i in range(60):  # 6 seconds @ 10 fps
        frame = cv2.add(bg, rng.integers(0, 40, bg.shape, dtype=np.uint8))
        offset = int(15 * np.sin(2 * np.pi * i / 10))
        x = (320 - 140) // 2 + offset
        y = (240 - 140) // 2 + offset
        frame[y : y + 140, x : x + 140] = marker
        writer.write(frame)
    writer.release()
    return path


def run_command(cmd: list[str]) -> subprocess.CompletedProcess:
    """Helper for running shell commands in fixtures."""
    return subprocess.run(cmd, check=False, capture_output=True, text=True)


def is_docker_available() -> bool:
    return shutil.which("docker") is not None
