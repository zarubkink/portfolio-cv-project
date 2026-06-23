from typing import Annotated, TypeVar

from fastapi import APIRouter, Body, Depends, HTTPException
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

from src.dependencies import get_async_session
from src.repositories.base import AsyncRepository

M = TypeVar("M", bound=SQLModel)
C = TypeVar("C", bound=SQLModel)
U = TypeVar("U", bound=SQLModel)
R = TypeVar("R", bound=SQLModel)
F = TypeVar("F", bound=SQLModel)


def crud_router(  # type: ignore[no-untyped-def] # noqa: UP047
    *,
    model: type[M],
    create_schema: type[C],
    update_schema: type[U],
    read_schema: type[R],
    filter_schema: type[F],
    prefix: str,
    tags: list[str],
    include_create: bool = True,
    include_read: bool = True,
    include_update: bool = True,
    include_delete: bool = True,
    include_filter: bool = True,
) -> APIRouter:
    """Generic CRUD-роутер (адаптация sbr/src/router/factory.py).

    POST   /        — create
    GET    /        — list (limit/offset)
    POST   /filter  — list с фильтрами
    GET    /{id}    — read
    PATCH  /{id}    — partial update
    DELETE /{id}    — delete
    """
    router = APIRouter(prefix=prefix, tags=tags)
    CreateBody = Annotated[create_schema, Body(...)]
    UpdateBody = Annotated[update_schema, Body(...)]
    FilterBody = Annotated[filter_schema, Body(...)]

    def repo(session: AsyncSession = Depends(get_async_session)) -> AsyncRepository:
        return AsyncRepository(model, session)

    @router.get("/", response_model=list[read_schema])
    async def list_items(
        limit: int = 100,
        offset: int = 0,
        r: AsyncRepository = Depends(repo),
    ):
        return await r.list(limit=limit, offset=offset)

    if include_filter:

        @router.post("/filter", response_model=list[read_schema])
        async def list_items_filtered(
            filters: FilterBody,
            limit: int = 100,
            offset: int = 0,
            reverse: bool = False,
            r: AsyncRepository = Depends(repo),
        ):
            return await r.get_filtered(
                options=filters.model_dump(exclude_unset=True),
                limit=limit,
                offset=offset,
                reverse=reverse,
            )

    if include_read:

        @router.get("/{item_id}", response_model=read_schema)
        async def get_item(item_id: int, r: AsyncRepository = Depends(repo)):
            obj = await r.get(item_id)
            if not obj:
                raise HTTPException(404, f"{model.__name__} id={item_id} not found")
            return obj

    if include_create:

        @router.post("/", response_model=read_schema, status_code=201)
        async def create_item(payload: CreateBody, r: AsyncRepository = Depends(repo)):
            return await r.create(payload.model_dump(exclude_unset=True))

    if include_update:

        @router.patch("/{item_id}", response_model=read_schema)
        async def update_item(
            item_id: int,
            payload: UpdateBody,
            r: AsyncRepository = Depends(repo),
        ):
            obj = await r.get(item_id)
            if not obj:
                raise HTTPException(404)
            return await r.update(obj, payload.model_dump(exclude_unset=True))

    if include_delete:

        @router.delete("/{item_id}", status_code=204)
        async def delete_item(item_id: int, r: AsyncRepository = Depends(repo)):
            obj = await r.get(item_id)
            if not obj:
                raise HTTPException(404)
            await r.delete(obj)

    return router
