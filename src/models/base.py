from datetime import datetime

from sqlmodel import Field, SQLModel, text


class BaseFields(SQLModel):
    """Общие поля для всех таблиц: id + аудит-метки.

    Все таблицы, наследующие BaseFields и помеченные `table=True`,
    получат эти колонки. SQLModel собирает их в `SQLModel.metadata`,
    которую видит Alembic."""

    id: int | None = Field(default=None, primary_key=True)
    created_at: datetime | None = Field(
        default=None,
        sa_column_kwargs={"server_default": text("CURRENT_TIMESTAMP")},
        nullable=False,
    )
    updated_at: datetime | None = Field(
        default=None,
        sa_column_kwargs={
            "server_default": text("CURRENT_TIMESTAMP"),
            "onupdate": text("CURRENT_TIMESTAMP"),
        },
        nullable=False,
    )
    deleted_at: datetime | None = Field(default=None)
