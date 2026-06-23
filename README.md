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
