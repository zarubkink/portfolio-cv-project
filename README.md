# Agro Tracking

API + worker для отслеживания визитов тракторов на станции фермы по видео с ArUco-маркерами.

- **Стек:** Python 3.12, FastAPI, SQLModel + asyncpg, PostgreSQL 16, Alembic, loguru, OpenCV (ArUco).
- **Документ с ТЗ:** [`PROJECT_SCAFFOLD_PROMPT.md`](./PROJECT_SCAFFOLD_PROMPT.md).

---

## Этап 1 — Скелет + PostgreSQL + healthcheck

Поднимается PostgreSQL в Docker, есть запускаемый FastAPI, Alembic инициализирован, `GET /health` проверяет БД.

### Подготовка

```bash
cd /home/kirill/work_dir/agro_prj

# 1. Установить зависимости
uv sync

# 2. Скопировать env-шаблон (уже скопирован в stack.env для удобства)
cp stack.env.example stack.env

# 3. Поднять PostgreSQL
docker compose up -d db

# Проверить, что healthy
docker compose ps        # db — Up (healthy)
```

### Запуск API

```bash
# В одном терминале
uv run uvicorn src.main:app --port 8000 --reload

# В другом — проверить
curl http://localhost:8000/health
# {"status":"ok","db":"ok"}
```

### Alembic

```bash
# Проверить, что alembic видит модели (пока пусто — ничего не мигрирует)
uv run alembic check

# Создать первую миграцию (после этапа 2, когда появятся модели):
# uv run alembic revision --autogenerate -m "initial: stations, tractors"
# uv run alembic upgrade head
```

### Доступ к БД с хоста

```bash
psql postgresql://agro:agro@localhost:5432/agro -c "\dt"
```

---

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
