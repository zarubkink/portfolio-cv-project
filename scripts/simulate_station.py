"""Кладёт тестовое видео в очередь станции.

Использование:
    uv run python scripts/simulate_station.py STATION_01 /tmp/test.mp4

Создаёт data/queue/STATION_01/YYYYMMDD_HHMMSS.mp4 (копию исходного файла).
"""

import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
QUEUE = ROOT / "data" / "queue"


def main() -> None:
    if len(sys.argv) < 3:
        print("usage: simulate_station.py STATION_XX /path/to/video.mp4")
        sys.exit(1)
    station = sys.argv[1]
    src = Path(sys.argv[2])
    if not src.exists():
        print(f"source not found: {src}")
        sys.exit(1)
    target_dir = QUEUE / station
    target_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    target = target_dir / f"{ts}{src.suffix.lower()}"
    shutil.copyfile(src, target)
    print(f"Placed: {target}")


if __name__ == "__main__":
    main()
