# Agro Tracking

API + worker для отслеживания визитов тракторов на станции фермы по видео с ArUco-маркерами.

- **Стек:** Python 3.12, FastAPI, SQLModel + asyncpg, PostgreSQL 16, Alembic, loguru, OpenCV (ArUco).

## Структура проекта

```
agro_prj/
├── compose.yaml              # сервис db (PostgreSQL 16-alpine)
├── pyproject.toml
├── stack.env / stack.env.example
├── alembic.ini
├── alembic/                  # миграции (env.py адаптирован под async)
├── src/
│   ├── main.py               # FastAPI app, /health
│   ├── dependencies.py       # async engine + get_async_session
│   ├── logging_setup.py
│   ├── config/               # BaseSettings (database, logging, ...)
│   ├── models/               # SQLModel (Этап 2+)
│   ├── schemas/              # pydantic/SQLModel schemas
│   ├── repositories/
│   ├── services/
│   ├── router/v1/
│   └── sse/
├── ingestion/                # watcher-процесс (Этап 4+)
├── tests/
├── scripts/                  # generate_test_video.py (Этап 5) и т.п.
└── data/
    ├── queue/STATION_01..35/ # входящие видео
    ├── videos/               # cold storage
    └── failed_videos/        # битые/исчерпавшие retry
```

---

## Tech stack

* **Python 3.12** — modern typing (`StrEnum`, `type` statement, PEP 695 generics)
* **FastAPI 0.115** + **uvicorn** — HTTP API
* **SQLModel 0.0.38** on top of **SQLAlchemy 2.x** with **asyncpg** driver
* **PostgreSQL 16+** (works on **paradedb/pg17** for local fallback) — JSONB, GENERATED columns, ENUMs, GIN + partial indexes
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

# offline fallback — uses already-cached paradedb image (PG 17)
DB_IMAGE=paradedb/paradedb:0.20.0-pg17 docker compose up -d db
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
| 7 | Scheduler for retry FAILED | ⏳ |
| 8 | Visit aggregator (state machine) + API | ⏳ |
| 9 | SSE for realtime + integration tests | ⏳ |

---

## License

MIT — see [LICENSE](LICENSE).
