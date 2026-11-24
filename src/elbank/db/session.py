"""Async engine, session factory and the FastAPI session dependency."""

from collections.abc import AsyncIterator
from typing import cast

from fastapi import Request
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from elbank.core.config import PostgresSettings


def create_engine(settings: PostgresSettings) -> AsyncEngine:
    return create_async_engine(
        settings.dsn,
        echo=settings.echo,
        pool_size=settings.pool_size,
        max_overflow=settings.max_overflow,
        pool_pre_ping=True,
    )


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False, autoflush=False)


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    """FastAPI dependency: one session per request.

    Transactions are committed explicitly by the service layer (unit of work);
    anything left uncommitted is rolled back when the session closes, and an
    exception raised by the handler rolls back immediately.
    """
    factory = cast(async_sessionmaker[AsyncSession], request.app.state.resources.session_factory)
    async with factory() as session:
        try:
            yield session
        except BaseException:
            await session.rollback()
            raise
