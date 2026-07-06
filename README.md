# Agro Tracking

> End-to-end video analytics pipeline that tracks tractor visits to
> farm stations using ArUco fiducial markers — built with FastAPI,
> PostgreSQL, OpenCV, and asyncio.

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688.svg)](https://fastapi.tiangolo.com)
[![SQLModel](https://img.shields.io/badge/SQLModel-0.0.38-orange.svg)](https://sqlmodel.tiangolo.com)
[![PostgreSQL 16+](https://img.shields.io/badge/PostgreSQL-16%2B-336791.svg)](https://www.postgresql.org)
[![OpenCV](https://img.shields.io/badge/OpenCV-4.11-5C3EE8.svg)](https://opencv.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A reference architecture for **video → events → business state** built
in nine incremental stages. Each stage is small, end-to-end verified,
and isolated — the same way a real production codebase would grow
behind feature flags.

---

## What it does

On a farm with **35 stations** and **6 tractors**:

1. IP cameras at each station record short MP4 clips and drop them
   into `data/queue/STATION_XX/YYYYMMDD_HHMMSS.mp4`.
2. An **ingestion watcher** copies the clip into cold storage and
   POSTs it to the FastAPI service.
3. A FastAPI worker **schedules background processing** in a
   `ProcessPoolExecutor` so the event loop stays responsive.
4. The pipeline decodes each frame: a **MOG2 motion trigger** fires
   the expensive ArUco decoder, which reads the marker ID.
5. **Multi-marker** tractors (front, rear, sides) are matched against
   the `tractors.aruco_ids` array via a GIN index — any matching ID
   resolves to the tractor.
6. Detections are tagged with the station's **ROI polygon** and the
   **"parked vs passing-by"** decision (ROI ∩ velocity ≈ 0 ∩ MOG2 mass).
7. **Visit aggregator** stitches detections into visits using a
   state machine: `ENTERING → PRESENT → LEAVING → CLOSED`.
8. Operators see live visit events in a **Server-Sent Events** feed.

---

## Why this project exists

Hiring signal — this codebase demonstrates:

| Area | What's on display |
|---|---|
| **Async architecture** | FastAPI lifespan, async SQLAlchemy sessions, asyncpg, `asyncio.to_thread` for blocking work, `ProcessPoolExecutor` for CPU-bound work. |
| **Domain modelling** | SQLModel tables, Pydantic schemas, generated columns, ENUMs, JSONB, partial + GIN indexes. |
| **Hybrid detection** | A pluggable pipeline (`trigger → decode → ROI → parked-decision`) designed so the decoder can be swapped (YOLO, colour, re-id) without DB migration. |
| **Robust I/O** | Deduplication by SHA-256, atomic rename to cold storage, retry-with-backoff for failed videos, no orphan temp files. |
| **Testing discipline** | End-to-end smoke test per stage; ruff + alembic-check as gate. |
| **Production pragmatism** | Sensible `pydantic-settings` config, loguru logging, lazy-init singletons, ORM `commit` happens in `get_async_session` not in every route. |

---

## Project structure

```
agro_prj/
├── compose.yaml              # docker-compose: service `db` (PostgreSQL)
├── pyproject.toml            # uv-managed deps + ruff/pytest config
├── stack.env / stack.env.example
├── alembic.ini
├── alembic/
│   ├── env.py                # async, filters extension-managed tables (PostGIS, etc.)
│   ├── script.py.mako
│   └── versions/
│       ├── 3112f42d1e35_initial_stations_tractors.py
│       ├── e31b2289c092_video_files.py
│       ├── 0003_events.py
│       └── 0004_events_updated_at.py
├── src/
│   ├── main.py               # FastAPI app, /health, lifespan
│   ├── dependencies.py       # async engine + get_async_session (commits after yield)
│   ├── utils.py              # hash_large_file → 32-byte BYTEA
│   ├── logging_setup.py
│   ├── config/               # pydantic-settings, one module per concern
│   │   ├── database.py       # DB_URL, pool sizes, storage paths
│   │   ├── logging.py
│   │   ├── video.py          # target_width, target_fps
│   │   ├── detector.py       # ArUco dict, MOG2 thresholds, ROI thresholds
│   │   └── threads.py        # max_process_workers
│   ├── models/               # SQLModel table classes
│   ├── schemas/              # Pydantic request/response + internal dataclasses
│   │   ├── detector.py       # TriggerResult, ArucoDetection, ParkedDecision (NamedTuple)
│   │   └── event.py          # DetectorMethod, EventType
│   ├── repositories/         # generic AsyncRepository[T] (PEP 695)
│   ├── services/
│   │   ├── detector.py       # TriggerDetector, ArucoDetector, RoiChecker, ParkedDetector
│   │   ├── video_processor.py# extract_frames, process_video, DetectionEvent
│   │   ├── video_executor.py # ProcessPoolExecutor wrapper
│   │   ├── video_handler.py  # background pipeline (CREATED → PROCESSING → COMPLETED/FAILED)
│   │   ├── event_service.py  # bulk-insert events for one video
│   │   ├── scheduler.py      # VideoRetryScheduler (stale + retry ticks)
│   │   └── exceptions.py     # VideoProcessError, DuplicateVideoError, ArucoDecodeError
│   └── router/v1/
├── ingestion/                # standalone watcher process
│   ├── config.py             # stations_root, api_url, cursor_sleep_sec
│   ├── cursor.py             # StationDirectory, StationVideoFile
│   ├── exceptions.py
│   └── main.py               # producer-per-folder, semaphore, signal handling
├── scripts/
│   ├── seed_reference.py     # 35 stations + 6 tractors
│   ├── simulate_station.py   # drops a test clip into the queue
│   └── generate_test_video.py# cv2.aruco + cv2.VideoWriter synthetic clips
└── data/
    ├── queue/STATION_01..35/ # incoming (watcher reads from here)
    ├── videos/               # cold storage (immutable, named <hash>.<ext>)
    └── failed_videos/        # after retry budget exhausted
```

---

## Tech stack

* **Python 3.12** — modern typing (`StrEnum`, `type` statement, PEP 695 generics)
* **FastAPI 0.115** + **uvicorn** — HTTP API
* **SQLModel 0.0.38** on top of **SQLAlchemy 2.x** with **asyncpg** driver
* **PostgreSQL 17+** (default image: `postgres:17-alpine`) — JSONB, GENERATED columns, ENUMs, GIN + partial indexes
* **Alembic** — async migrations (`env.py` is async-aware)
* **OpenCV 4.11 (`opencv-contrib-python`)** — MOG2 + ArUco
* **loguru** — structured logging
* **aiohttp** — async HTTP for the ingestion watcher
* **uv** — fast dependency manager

---

## Quick start

### Prerequisites

* Python 3.12+
* [uv](https://docs.astral.sh/uv/) (`pip install uv`)
* Docker (only if running PostgreSQL locally via compose)

### 1. Clone and install

```bash
git clone https://github.com/<your-handle>/agro_prj.git
cd agro_prj
uv sync
```

### 2. Start PostgreSQL

```bash
# default — pulls postgres:16-alpine
docker compose up -d db

# Pin a different image tag if 17-alpine is not reachable from your network
POSTGRES_DOCKER_TAG=16-alpine docker compose --env-file stack.env up -d db
```

### 3. Apply migrations and seed reference data

```bash
uv run alembic upgrade head
uv run python scripts/seed_reference.py
```

### 4. Run the API and the ingestion watcher

```bash
# Terminal 1
uv run uvicorn src.main:app --host 0.0.0.0 --port 8000 --reload

# Terminal 2
uv run python -m ingestion.main
```

### 5. Smoke test

```bash
# Drop a synthetic clip into the queue
uv run python scripts/simulate_station.py STATION_01 /tmp/sim_test.mp4

# Or use the included test video generator
uv run python scripts/generate_test_video.py \
    --aruco-id 1 --duration 4 --output /tmp/test_aruco.mp4
uv run python scripts/simulate_station.py STATION_01 /tmp/test_aruco.mp4

# After ~5s, the watcher should have created a VideoFile row
curl -s http://localhost:8000/v1/videos/ | python -m json.tool
```

---

## Database schema

Five tables, all created in stage order:

| Table | Purpose | Notable columns / constraints |
|---|---|---|
| `stations` | Camera stations on the farm | `code` UNIQUE, `roi_polygon JSONB` |
| `tractors` | Tractors with multi-marker tags | `aruco_ids INTEGER[]`, `primary_aruco_id INTEGER GENERATED ALWAYS AS (aruco_ids[1]) STORED`, GIN index on `aruco_ids`, UNIQUE on `primary_aruco_id` |
| `video_files` | One row per ingested clip | `content_hash BYTEA(32) UNIQUE` (SHA-256), `status videostatus`, partial index on `(status)` `WHERE status IN ('FAILED','PROCESSING')` |
| `events` | Raw detection stream | `detector_method detector_method`, `inside_roi BOOL`, `detector_metadata JSONB`, composite `(station_id, started_at)` index |
| `visits` | Aggregated visits per tractor per station | `state visitstate`, `arrived_at / departed_at / last_seen_at`, `duration_seconds FLOAT GENERATED` |

ENUMs:

* `videostatus`: `CREATED / PROCESSING / COMPLETED / FAILED / INVALID`
* `detector_method`: `aruco / yolo_aruco / color_class / reid / fallback`
* `visitstate`: `ENTERING / PRESENT / LEAVING / CLOSED`

> **Design note.** Every "extensibility hook" lives as data, not code —
> `detector_method` lets you swap YOLO for ArUco without a migration;
> `detector_metadata JSONB` carries method-specific metrics; the
> primary ArUco column is a *generated* column from `aruco_ids[1]`,
> so the canonical "identity" can never drift out of sync.

---

## Video pipeline

```
        MP4 clip in data/queue/STATION_XX/
                       │
                       ▼
       ┌─────────────────────────────┐
       │  ingestion watcher (aiohttp)│  ← copies to data/videos/<hash>.mp4
       └─────────────────────────────┘
                       │ POST /v1/videos/handle
                       ▼
       ┌─────────────────────────────┐
       │  FastAPI handler (async)    │  ← INSERT video_files (status=CREATED)
       └─────────────────────────────┘
                       │ background task
                       ▼
       ┌─────────────────────────────┐
       │  ProcessPoolExecutor worker │
       │  ├─ extract_frames()        │  ← cv2.VideoCapture + resize
       │  ├─ TriggerDetector.detect  │  ← MOG2 motion gate
       │  ├─ ArucoDetector.detect    │  ← cv2.aruco marker decode
       │  ├─ RoiChecker.is_inside    │  ← cv2.pointPolygonTest
       │  └─ ParkedDetector.decide   │  ← ROI ∩ velocity ∩ MOG2 mass
       └─────────────────────────────┘
                       │ list[DetectionEvent]
                       ▼
       ┌─────────────────────────────┐
       │  EventService.create()      │  ← INSERT events (bulk)
       └─────────────────────────────┘
                       │
                       ▼
                (Stage 8) VisitAggregator
```

Each frame goes through four small composable detectors (in
`src/services/detector.py`). They are intentionally tiny and
stateless-across-instances so they can be re-created inside a worker
process without serialization.

---

## The four detectors

1. **`TriggerDetector`** — wraps `cv2.createBackgroundSubtractorMOG2`.
   Returns the largest foreground contour's area and bbox. Cheap;
   skipped most frames.
2. **`ArucoDetector`** — wraps `cv2.aruco.ArucoDetector` with
   `DICT_4X4_50` (configurable). Drops detections smaller than
   `min_aruco_side_pixels` (default 30 px).
3. **`RoiChecker`** — point-in-polygon test via
   `cv2.pointPolygonTest`. If `roi_polygon` is `None`, the whole frame
   counts as inside.
4. **`ParkedDetector`** — composite decision:

   ```
   is_parked = inside_roi AND velocity_px < threshold
                          AND mog2_mass_in_roi > min
   ```

   `velocity_px` is the frame-to-frame Euclidean distance of the
   marker's centre.

The schemas for the in-pipeline types are in
`src/schemas/detector.py` and are `NamedTuple`s — picklable, no copy
overhead when crossing process boundaries.

---

## Configuration

Everything is in `stack.env` (loaded by `pydantic-settings`):

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

LOG_LEVEL=DEBUG
```

---

## API surface (current)

| Method | Path | Purpose |
|---|---|---|
| `GET`    | `/health` | Liveness + DB ping |
| `POST`   | `/v1/stations` | Create a station |
| `GET`    | `/v1/stations` | List stations |
| `GET`    | `/v1/stations/{id}` | Read one |
| `PATCH`  | `/v1/stations/{id}` | Partial update |
| `DELETE` | `/v1/stations/{id}` | Soft delete (`deleted_at`) |
| `POST`   | `/v1/tractors` | Create (with `aruco_ids: int[]`) |
| `GET`    | `/v1/tractors` | List |
| `POST`   | `/v1/videos/upload` | Multipart upload, SHA-256 dedup + background processing |
| `POST`   | `/v1/videos/handle` | Server-side file path → enqueue for processing (used by ingestion) |
| `GET`    | `/v1/videos` | List videos |
| `GET`    | `/v1/videos/{id}` | Read one with frames/triggers/events counts |
| `POST`   | `/v1/admin/scheduler/tick` | Run one pass of the retry scheduler on demand |

See [docs/API.md](docs/API.md) (WIP) for full request/response shapes.

---

## Development workflow

```bash
uv sync                              # install deps
uv run ruff check src/ ingestion/ scripts/   # lint
uv run ruff format src/ ingestion/ scripts/  # auto-format
uv run alembic check                 # drift detection vs models
uv run pytest                        # tests (added in stage 9)
```

The repo's `pyproject.toml` configures `ruff` for strict linting and
`pytest-asyncio` for async tests.

---

## Roadmap

| Stage | Title | Status |
|:---:|---|:---:|
| 1 | Project skeleton + PostgreSQL + healthcheck | ✅ |
| 2 | Models, schemas, migration, CRUD for stations/tractors | ✅ |
| 3 | Video upload via API (`POST /v1/videos/upload`) | ✅ |
| 4 | Folder watcher (ingestion) → E2E | ✅ |
| 5 | Video processor (MOG2 + ArUco + ROI + Parked) | ✅ |
| 6 | `ProcessPoolExecutor` integration with `/handle` + events table | ✅ |
| 7 | Scheduler for retry FAILED | ✅ |
| 8 | Visit aggregator (state machine) + API | ⏳ |
| 9 | SSE for realtime + integration tests | ⏳ |

---

## License

MIT — see [LICENSE](LICENSE).
