"""Сидит 35 станций STATION_01..STATION_35 и 6 тракторов.

Использование:
    uv run python scripts/seed_reference.py

Опционально:
    --reset   сначала удалить существующие строки
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from sqlmodel import select  # noqa: E402
from sqlmodel.ext.asyncio.session import AsyncSession  # noqa: E402

from src.dependencies import engine  # noqa: E402
from src.models.station import Station  # noqa: E402
from src.models.tractor import Tractor  # noqa: E402

DEFAULT_ROI = [
    [100, 100],
    [540, 100],
    [540, 380],
    [100, 380],
]


async def seed(reset: bool = False) -> None:
    async with AsyncSession(engine) as session:
        if reset:
            for model in (Station, Tractor):
                rows = (await session.exec(select(model))).all()
                for r in rows:
                    await session.delete(r)
            await session.commit()
            print("Cleared existing stations/tractors")

        existing_stations = (await session.exec(select(Station.code))).all()
        existing_codes = set(existing_stations)
        created = 0
        for i in range(1, 36):
            code = f"STATION_{i:02d}"
            if code in existing_codes:
                continue
            session.add(
                Station(
                    code=code,
                    name=f"Станция {i:02d}",
                    location=f"Ферма, зона {i}",
                    video_dir=f"./data/queue/STATION_{i:02d}",
                    roi_polygon=DEFAULT_ROI,
                    is_entry_zone=True,
                    is_active=True,
                )
            )
            created += 1
        await session.commit()
        print(f"Stations: {created} created")

        existing_tractors = (await session.exec(select(Tractor.primary_aruco_id))).all()
        existing_ids = {int(x) for x in existing_tractors if x is not None}
        created_t = 0
        for aruco_id in [1, 2, 3, 4, 5, 6]:
            if aruco_id in existing_ids:
                continue
            session.add(
                Tractor(
                    name=f"Трактор #{aruco_id}",
                    model="JD 6155M",
                    notes=f"Primary ArUco={aruco_id}",
                    is_active=True,
                    aruco_ids=[aruco_id, aruco_id + 10, aruco_id + 20],
                )
            )
            created_t += 1
        await session.commit()
        print(f"Tractors: {created_t} created")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()

    import asyncio

    asyncio.run(seed(reset=args.reset))


if __name__ == "__main__":
    main()
