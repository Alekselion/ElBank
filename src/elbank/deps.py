"""FastAPI dependencies.

Long-lived resources (DB engine, Redis, Kafka producer) are created in the
application lifespan and stored on ``app.state``; dependencies expose them
to route handlers with precise types.
"""

from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Annotated, cast

from fastapi import Depends, Request
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from elbank.core.config import Settings, get_settings
from elbank.db.session import session_scope
from elbank.kafka import KafkaProducer


@dataclass(slots=True, frozen=True)
class Resources:
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    redis: Redis
    kafka: KafkaProducer


def get_resources(request: Request) -> Resources:
    return cast(Resources, request.app.state.resources)


ResourcesDep = Annotated[Resources, Depends(get_resources)]
SettingsDep = Annotated[Settings, Depends(get_settings)]


async def get_db_session(resources: ResourcesDep) -> AsyncIterator[AsyncSession]:
    async for session in session_scope(resources.session_factory):
        yield session


def get_redis(resources: ResourcesDep) -> Redis:
    return resources.redis


def get_kafka(resources: ResourcesDep) -> KafkaProducer:
    return resources.kafka


DbSessionDep = Annotated[AsyncSession, Depends(get_db_session)]
RedisDep = Annotated[Redis, Depends(get_redis)]
KafkaDep = Annotated[KafkaProducer, Depends(get_kafka)]
