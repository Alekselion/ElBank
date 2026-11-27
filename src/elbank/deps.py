"""FastAPI dependencies.

Long-lived resources (DB engine, Redis, Kafka producer) are created in the
application lifespan and stored on ``app.state``; dependencies expose them
to route handlers with precise types.
"""

from dataclasses import dataclass
from typing import Annotated, cast

import httpx
from fastapi import Depends, Request
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from elbank.clients import (
    CardProcessorClient,
    CbrClient,
    GosuslugiClient,
    InterbankClient,
    ProviderClient,
    SbpClient,
    SmsClient,
)
from elbank.core.config import Settings, get_settings
from elbank.db.session import get_session
from elbank.kafka import EventPublisher, KafkaProducer


@dataclass(slots=True, frozen=True)
class Resources:
    engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    redis: Redis
    kafka: KafkaProducer
    http: httpx.AsyncClient  # WireMock-backed external services


def get_resources(request: Request) -> Resources:
    return cast(Resources, request.app.state.resources)


ResourcesDep = Annotated[Resources, Depends(get_resources)]
SettingsDep = Annotated[Settings, Depends(get_settings)]


def get_redis(resources: ResourcesDep) -> Redis:
    return resources.redis


def get_kafka(resources: ResourcesDep) -> KafkaProducer:
    return resources.kafka


def get_event_publisher(resources: ResourcesDep) -> EventPublisher:
    return EventPublisher(resources.kafka)


def get_sms_client(resources: ResourcesDep) -> SmsClient:
    return SmsClient(resources.http)


def get_gosuslugi_client(resources: ResourcesDep, settings: SettingsDep) -> GosuslugiClient:
    return GosuslugiClient(resources.http, settings.gosuslugi)


def get_interbank_client(resources: ResourcesDep, settings: SettingsDep) -> InterbankClient:
    return InterbankClient(resources.http, settings.interbank)


def get_sbp_client(resources: ResourcesDep, settings: SettingsDep) -> SbpClient:
    return SbpClient(resources.http, settings.sbp)


def get_cbr_client(resources: ResourcesDep, settings: SettingsDep) -> CbrClient:
    return CbrClient(resources.http, settings.cbr)


def get_card_processor_client(
    resources: ResourcesDep, settings: SettingsDep
) -> CardProcessorClient:
    return CardProcessorClient(resources.http, settings.card_processor)


def get_provider_client(resources: ResourcesDep, settings: SettingsDep) -> ProviderClient:
    return ProviderClient(resources.http, settings.providers)


DbSessionDep = Annotated[AsyncSession, Depends(get_session)]
RedisDep = Annotated[Redis, Depends(get_redis)]
KafkaDep = Annotated[KafkaProducer, Depends(get_kafka)]
EventPublisherDep = Annotated[EventPublisher, Depends(get_event_publisher)]
SmsClientDep = Annotated[SmsClient, Depends(get_sms_client)]
GosuslugiClientDep = Annotated[GosuslugiClient, Depends(get_gosuslugi_client)]
InterbankClientDep = Annotated[InterbankClient, Depends(get_interbank_client)]
SbpClientDep = Annotated[SbpClient, Depends(get_sbp_client)]
CbrClientDep = Annotated[CbrClient, Depends(get_cbr_client)]
ProviderClientDep = Annotated[ProviderClient, Depends(get_provider_client)]
CardProcessorClientDep = Annotated[CardProcessorClient, Depends(get_card_processor_client)]
