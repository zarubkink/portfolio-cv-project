# Agro Tracking — production-ish image for the api and ingestion services.
# Built once via `docker compose build`, reused by both `api` and `ingestion`.
#
# Why uv: deterministic lock-free install, ~10× faster than pip for cold
# installs, and matches local development exactly (`uv sync`).

# Use gcr.io mirror because Docker Hub is sometimes unreachable from
# corporate / VPN networks. Override with --build-arg PYTHON_IMAGE=...
# if you need a different registry (e.g. docker.io directly).
ARG PYTHON_IMAGE=mirror.gcr.io/library/python:3.12-slim
FROM ${PYTHON_IMAGE} AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/usr/local \
    UV_COMPILE_BYTECODE=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        curl \
        ca-certificates \
        libgl1 \
        libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Install uv (pinned to the same minor version used in development)
RUN pip install --no-cache-dir "uv==0.11.23"

WORKDIR /app

# Copy only the manifest first to leverage Docker layer caching
COPY pyproject.toml ./

# Pre-install dependencies (no project yet). This layer is reused on
# code-only changes, which is the common case in iteration.
RUN uv sync --no-install-project --no-group dev

# Now copy the project sources and install the project itself in editable mode
COPY src ./src
COPY ingestion ./ingestion
COPY scripts ./scripts
COPY alembic ./alembic
COPY alembic.ini ./
COPY README.md ./README.md
COPY LICENSE ./LICENSE
RUN uv sync --no-group dev

# OpenCV needs /tmp writable; nothing else special at runtime
RUN mkdir -p /app/data/videos /app/data/failed_videos /app/data/queue

EXPOSE 8000

# Default command — overridden by compose.yaml per service
CMD ["uv", "run", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
