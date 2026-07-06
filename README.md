# Agro Tracking

> Video analytics pipeline that tracks tractor visits to farm stations
> using ArUco fiducial markers. FastAPI + PostgreSQL + OpenCV, fully
> async, packaged as a Docker stack.

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688.svg)](https://fastapi.tiangolo.com)
[![SQLModel](https://img.shields.io/badge/SQLModel-0.0.38-orange.svg)](https://sqlmodel.tiangolo.com)
[![PostgreSQL 17](https://img.shields.io/badge/PostgreSQL-17-336791.svg)](https://www.postgresql.org)
[![OpenCV](https://img.shields.io/badge/OpenCV-4.11-5C3EE8.svg)](https://opencv.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## What it does

Farm cameras drop short MP4 clips into per-station watch folders. The
ingestion watcher ships each clip into cold storage and hands it off
to the API. A background worker decodes the frames with a MOG2 motion
gate + ArUco marker decoder, joins marker IDs to tractors, and writes
the raw detection stream to PostgreSQL. A visit aggregator stitches
the detections into visits using a four-state machine
(`ENTERING → PRESENT → LEAVING → CLOSED`) with debounce so a tractor
parked across two video files is logged as a single visit.

The result: per-tractor arrival/departure times, dwell seconds, and a
live "who is at which station right now" feed queryable through REST.

---

## Architecture at a glance

```
         MP4 clip in data/queue/STATION_XX/
                        │
                        ▼
        ┌──────────────────────────────┐
        │  ingestion watcher (aiohttp) │  ← copies to data/videos/<hash>.mp4
        └──────────────────────────────┘
                        │ POST /v1/videos/handle
                        ▼
        ┌──────────────────────────────┐
        │  FastAPI handler (async)     │  ← INSERT video_files (status=CREATED)
        └──────────────────────────────┘
                        │ background task
                        ▼
        ┌──────────────────────────────┐
        │  ProcessPoolExecutor worker  │
        │  ├─ TriggerDetector (MOG2)   │
        │  ├─ ArucoDetector (cv2)      │
        │  ├─ RoiChecker (polygon)     │
        │  └─ ParkedDetector (decision)│
        └──────────────────────────────┘
                        │ list[DetectionEvent]
                        ▼
        ┌──────────────────────────────┐
        │  EventService.create_many()  │  ← INSERT events
        └──────────────────────────────┘
                        │
                        ▼
        ┌──────────────────────────────┐
        │  VisitService                │  ← state-machine aggregation
        │  ENTERING → PRESENT → …      │
        └──────────────────────────────┘
```

---

## Tech stack

- **Python 3.12** — `StrEnum`, PEP 695 generics
- **FastAPI 0.115** + **uvicorn**
- **SQLModel 0.0.38** on SQLAlchemy 2.x with **asyncpg**
- **PostgreSQL 17** (paradedb/pg17 fallback) — JSONB, GENERATED
  columns, ENUMs, GIN + partial indexes
- **Alembic** — migrations (sync driver for ENUM-aware DDL)
- **OpenCV 4.11 (`opencv-contrib-python`)** — MOG2 + ArUco
- **loguru** — structured logging
- **aiohttp** — async HTTP for the ingestion watcher
- **uv** — dependency manager

---

## Project structure

```
agro_prj/
├── compose.yaml              # docker-compose: db + api + ingestion
├── pyproject.toml            # uv-managed deps + ruff/pytest config
├── stack.env / stack.env.example
├── alembic.ini
├── alembic/
│   ├── env.py                # sync psycopg2 for ENUM-aware DDL
│   └── versions/
│       ├── 3112f42d1e35_initial_stations_tractors.py
│       ├── e31b2289c092_video_files.py
│       ├── 0003_events.py
│       └── 0005_visits.py
├── src/
│   ├── main.py               # FastAPI app, /health, lifespan
│   ├── dependencies.py       # async engine + get_async_session
│   ├── utils.py              # hash_large_file (SHA-256)
│   ├── logging_setup.py
│   ├── config/               # pydantic-settings, one module per concern
│   │   ├── database.py
│   │   ├── logging.py
│   │   ├── video.py
│   │   ├── detector.py
│   │   ├── threads.py
│   │   ├── scheduler.py
│   │   └── visit.py
│   ├── models/               # SQLModel table classes
│   ├── schemas/              # Pydantic request/response + NamedTuples
│   ├── repositories/         # generic AsyncRepository[T]
│   ├── services/
│   │   ├── detector.py
│   │   ├── video_processor.py
│   │   ├── video_executor.py
│   │   ├── video_handler.py  # CREATED → PROCESSING → COMPLETED / FAILED
│   │   ├── video_service.py
│   │   ├── event_service.py
│   │   ├── visit_service.py  # state-machine aggregation
│   │   ├── scheduler.py      # retry + stale tick loop
│   │   └── exceptions.py
│   └── router/v1/
│       ├── station.py
│       ├── tractor.py
│       ├── video.py
│       ├── scheduler.py
│       └── status.py
├── ingestion/                # standalone watcher process
│   ├── config.py
│   ├── cursor.py
│   ├── exceptions.py
│   └── main.py
├── scripts/
│   ├── seed_reference.py
│   ├── simulate_station.py
│   ├── generate_test_video.py
│   └── smoke_*.py
└── data/
    ├── queue/STATION_01..35/ # incoming (watcher reads from here)
    ├── videos/               # cold storage, named <hash>.<ext>
    └── failed_videos/        # moved here after retry budget exhausted
```

---

## Quick start

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (`pip install uv`)
- Docker (for the compose stack)

### 1. Install

```bash
git clone https://github.com/<your-handle>/agro_prj.git
cd agro_prj
uv sync
```

### 2. Start the stack

```bash
docker compose --env-file stack.env up -d --build
```

This brings up three services:

- `db` — PostgreSQL 17 (volume-mounted for persistence)
- `api` — FastAPI on `:8000` with the retry scheduler and visit
  stale-check loop running in the lifespan
- `ingestion` — folder watcher that copies queued clips into cold
  storage and POSTs them to the API

### 3. Apply migrations and seed reference data

```bash
uv run alembic upgrade head
uv run python scripts/seed_reference.py
```

### 4. Smoke test

```bash
# Generate a synthetic clip with an ArUco marker
uv run python scripts/generate_test_video.py \
    --aruco-id 1 --duration 4 --output /tmp/test_aruco.mp4

# Drop it into the queue; the watcher picks it up
uv run python scripts/simulate_station.py STATION_01 /tmp/test_aruco.mp4

# After a few seconds, the video is processed and a visit is created
curl -s http://localhost:8000/v1/videos/ | python -m json.tool
curl -s http://localhost:8000/v1/status/tractors | python -m json.tool
```

---

## Database schema

Five tables, all using PostgreSQL-specific features for the hot paths:

| Table | Purpose | Notable columns / constraints |
|---|---|---|
| `stations` | Camera stations on the farm | `code` UNIQUE, `roi_polygon JSONB`, `is_active` |
| `tractors` | Tractors with multi-marker tags | `aruco_ids INTEGER[]`, `primary_aruco_id INTEGER GENERATED ALWAYS AS (aruco_ids[1]) STORED`, GIN on `aruco_ids` |
| `video_files` | One row per ingested clip | `content_hash BYTEA(32)` (SHA-256), `status videostatus`, partial index on non-terminal states |
| `events` | Raw detection stream | `detector_method detector_method`, `inside_roi BOOL`, `detector_metadata JSONB`, partial indexes per access pattern |
| `visits` | Aggregated visits per tractor+station | `state visitstate`, `arrived_at / departed_at / last_seen_at`, `duration_seconds FLOAT GENERATED`, partial UNIQUE `uq_visit_open` for one open visit per pair |

ENUMs:

- `videostatus`: `CREATED / PROCESSING / COMPLETED / FAILED / INVALID`
- `event_type`: `ENTRY / EXIT / DETECTED`
- `detector_method`: `aruco / yolo_aruco / color_class / reid / fallback`
- `visit_state`: `ENTERING / PRESENT / LEAVING / CLOSED`

Migrations live in `alembic/versions/`. `alembic check` confirms
models and migration head are in sync.

---

## Video pipeline

Each frame goes through four composable detectors
(in `src/services/detector.py`). They are stateless across instances
so they can be re-created inside a worker process without
serialization:

1. **`TriggerDetector`** — `cv2.createBackgroundSubtractorMOG2`. Cheap;
   gates the expensive decoder.
2. **`ArucoDetector`** — `cv2.aruco.ArucoDetector` with
   `DICT_4X4_50` (configurable). Drops detections smaller than
   `min_aruco_side_pixels`.
3. **`RoiChecker`** — `cv2.pointPolygonTest`. `roi_polygon=None`
   means the whole frame counts as inside.
4. **`ParkedDetector`** — composite decision:
   `inside_roi AND velocity_px < threshold AND mog2_mass_in_roi > min`.

In-pipeline types are `NamedTuple`s in `src/schemas/detector.py`,
picklable so they cross `ProcessPoolExecutor` boundaries for free.

CPU-bound work runs in a `ProcessPoolExecutor` (`MAX_PROCESS_WORKERS`
env var). Status transitions (`CREATED → PROCESSING → COMPLETED /
FAILED`) and the retry increment live in `video_handler.py`.

---

## Visit aggregation

`VisitService` listens to every event batch written by
`EventService.create_many` and drives the four-state machine:

```
ABSENT ──first in-ROI detection──► ENTERING
                                       │
                                       │ ≥ entry_confirm_seconds
                                       │ accumulated in-ROI seconds
                                       ▼
                                   PRESENT ──last_seen < exit_confirm──► LEAVING
                                                                              │
                                                                              │ another
                                                                              │ detection
                                                                              ▼
                                                                          PRESENT
                                              gap ≥ 2 × exit_confirm ▼
                                                CLOSED (departed_at set,
                                                duration_seconds filled
                                                by GENERATED column)
```

- **`entry_confirm_seconds`** (default 1.0): debounce to filter out
  false-positive single-frame detections.
- **`exit_confirm_seconds`** (default 10.0): how long a tractor can
  disappear from the frame before we start counting it as gone. Two
  windows close the visit.
- **`recovery_grace_multiplier`** (default 3.0): on startup, ENTERING
  visits older than `entry_confirm × multiplier` are deleted as
  false positives that survived a crash.

A background task (`check_stale_visits`) runs every
`stale_check_interval_seconds` and force-closes any open visit whose
`last_seen_at` has gone stale. The partial UNIQUE index
`uq_visit_open` enforces "at most one open visit per (tractor,
station)"; the loser of a race is rolled back and adopts the winner.

---

## Retry scheduler

`VideoRetryScheduler` runs a periodic tick:

1. Any `PROCESSING` row older than `stale_threshold_minutes` is
   flipped to `FAILED` (worker likely crashed).
2. Any `FAILED` row with `retry_count < max_retry_attempts` is
   re-dispatched through `process_video_with_error_handling(is_retry=True)`.
3. After the budget is exhausted, the file is moved to `failed_videos/`
   and the row becomes `INVALID`.

The scheduler is exposed via `POST /v1/admin/scheduler/tick` for
on-demand recovery and debugging.

---

## Configuration

All knobs live in `stack.env` (loaded by `pydantic-settings`).
`stack.env.example` is the version-safe reference checked into git.

```env
DB_URL=postgresql+asyncpg://agro:agro@localhost:5432/agro
VIDEOS_STORAGE=./data/videos
FAILED_VIDEOS_FOLDER=./data/failed_videos
QUEUES_ROOT=./data/queue

MAX_PROCESS_WORKERS=4
ARUCO_DICT=DICT_4X4_50
MIN_ARUCO_SIDE_PIXELS=30
MIN_CONTOUR_AREA=5000
MOG2_HISTORY=200
MOG2_VAR_THRESHOLD=50
VELOCITY_PX_THRESHOLD=3.0
MOG2_MASS_MIN=1000

SCHEDULER_ACTIVATE=true
MAX_CONCURRENT_REQUESTS=8
RETRY_INTERVAL_MINUTES=5
STALE_THRESHOLD_MINUTES=30
MAX_RETRY_ATTEMPTS=3

ENTRY_CONFIRM_SECONDS=1.0
EXIT_CONFIRM_SECONDS=10.0
STALE_CHECK_INTERVAL_SECONDS=5
RECOVERY_GRACE_MULTIPLIER=3.0

LOG_LEVEL=INFO
```

---

## API surface

| Method | Path | Purpose |
|---|---|---|
| `GET`    | `/health` | Liveness + DB ping |
| `POST`   | `/v1/stations` | Create a station |
| `GET`    | `/v1/stations` | List stations |
| `GET`    | `/v1/stations/{id}` | Read one |
| `PATCH`  | `/v1/stations/{id}` | Partial update |
| `DELETE` | `/v1/stations/{id}` | Soft delete |
| `POST`   | `/v1/tractors` | Create (`aruco_ids: int[]`) |
| `GET`    | `/v1/tractors` | List |
| `GET`    | `/v1/tractors/{id}` | Read one |
| `POST`   | `/v1/videos/upload` | Multipart upload, SHA-256 dedup, background processing |
| `POST`   | `/v1/videos/handle` | Server-side path → enqueue for processing |
| `GET`    | `/v1/videos` | List videos |
| `GET`    | `/v1/videos/{id}` | Read one with counters |
| `GET`    | `/v1/status/tractors` | Currently open visits, one row per (tractor, station) |
| `GET`    | `/v1/status/stations` | Active stations with tractors on each |
| `GET`    | `/v1/status/tractor/{id}` | Where is this tractor right now? (`ABSENT` if no open visit) |
| `GET`    | `/v1/status/visits/history` | Closed visits, filterable by tractor/station |
| `POST`   | `/v1/admin/scheduler/tick` | One pass of the retry scheduler on demand |

---

## Development

```bash
uv sync                                 # install deps
uv run ruff check src/ ingestion/ scripts/ tests/   # lint
uv run ruff format src/ ingestion/ scripts/ tests/  # auto-format
uv run alembic check                    # drift detection vs models
uv run pytest tests/unit/               # unit tests
```

The repo's `pyproject.toml` configures `ruff` for strict linting and
`pytest-asyncio` for async tests. Pre-commit runs ruff on every commit.

---

## License

MIT — see [LICENSE](LICENSE).
