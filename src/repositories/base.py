import builtins
from datetime import datetime
from typing import Any, TypeVar

from sqlalchemy import text
from sqlalchemy.sql import Select
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from src.models.base import BaseFields

T = TypeVar("T", bound=BaseFields)


class AsyncRepository[T]:
    """Типизированный CRUD поверх AsyncSession.

    Паттерн взят из sbr/src/repositories/base.py и адаптирован."""

    def __init__(self, model: type[T], session: AsyncSession):
        self.model = model
        self.session = session

    async def get(self, id: Any) -> T | None:
        stmt = select(self.model).where(self.model.id == id).limit(1)
        res = await self.session.exec(stmt)
        return res.first()

    async def list(
        self,
        limit: int = 100,
        offset: int = 0,
        created_before: datetime | None = None,
        created_after: datetime | None = None,
    ) -> list[T]:
        stmt = select(self.model)
        stmt = self._add_filters(
            stmt, created_before=created_before, created_after=created_after
        )
        stmt = stmt.offset(offset).limit(limit).order_by(self.model.id)
        res = await self.session.exec(stmt)
        return res.all()

    async def get_filtered(
        self,
        options: dict,
        limit: int = 100,
        offset: int = 0,
        reverse: bool = False,
        attr_none: builtins.list[str] | None = None,
        attr_not_none: builtins.list[str] | None = None,
        created_before: datetime | None = None,
        created_after: datetime | None = None,
    ) -> builtins.list[T]:
        stmt = select(self.model)

        for attr, value in options.items():
            if value is None:
                continue
            if isinstance(value, list):
                if hasattr(self.model, attr):
                    stmt = stmt.where(getattr(self.model, attr).in_(value))
                continue
            if hasattr(self.model, attr):
                stmt = stmt.filter_by(**{attr: value})

        if attr_none:
            for attr in attr_none:
                if hasattr(self.model, attr):
                    stmt = stmt.where(getattr(self.model, attr).is_(None))
        if attr_not_none:
            for attr in attr_not_none:
                if hasattr(self.model, attr):
                    stmt = stmt.where(getattr(self.model, attr).is_not(None))

        stmt = self._add_filters(
            stmt, created_before=created_before, created_after=created_after
        )
        stmt = stmt.limit(limit).offset(offset)
        stmt = stmt.order_by(self.model.id.desc() if reverse else self.model.id)
        res = await self.session.exec(stmt)
        return res.all()

    async def create(self, data: dict) -> T:
        obj = self.model(**data)
        self.session.add(obj)
        await self.session.flush()
        await self.session.refresh(obj)
        return obj

    async def update(self, obj: T, data: dict) -> T:
        for k, v in data.items():
            setattr(obj, k, v)
        self.session.add(obj)
        await self.session.flush()
        await self.session.refresh(obj)
        return obj

    async def update_by_id(self, obj_id: int, data: dict) -> T:
        obj = await self.get(obj_id)
        if obj is None:
            raise ValueError(f"{self.model.__name__} id={obj_id} not found")
        return await self.update(obj, data)

    async def delete(self, obj: T) -> None:
        await self.session.delete(obj)

    async def count(self) -> int:
        stmt = select(text("COUNT(*)")).select_from(self.model)
        res = await self.session.exec(stmt)
        return int(res.one() or 0)

    def _add_filters(
        self,
        stmt: Select,
        created_before: datetime | None = None,
        created_after: datetime | None = None,
    ) -> Select:
        if created_after is not None:
            stmt = stmt.where(self.model.created_at >= created_after)
        if created_before is not None:
            stmt = stmt.where(self.model.created_at <= created_before)
        return stmt
