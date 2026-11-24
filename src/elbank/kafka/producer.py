"""Kafka producer with lazy, retryable start.

The producer is started during application startup, but if Kafka is not reachable
yet the app still boots: ``/ready`` reports Kafka as down and retries the start on
each probe until the broker becomes available.
"""

import asyncio
import json
from typing import Any

from aiokafka import AIOKafkaProducer
from aiokafka.structs import RecordMetadata

from elbank.core.config import KafkaSettings


def _serialize(value: Any) -> bytes:
    return json.dumps(value, default=str, separators=(",", ":")).encode()


class KafkaProducer:
    def __init__(self, settings: KafkaSettings) -> None:
        self._settings = settings
        self._producer: AIOKafkaProducer | None = None
        self._lock = asyncio.Lock()

    @property
    def is_started(self) -> bool:
        return self._producer is not None

    async def start(self) -> None:
        async with self._lock:
            if self._producer is not None:
                return
            producer = AIOKafkaProducer(
                bootstrap_servers=self._settings.servers,
                client_id=self._settings.client_id,
                value_serializer=_serialize,
                key_serializer=lambda k: k.encode() if isinstance(k, str) else k,
                acks="all",
                enable_idempotence=True,
            )
            try:
                await producer.start()
            except BaseException:
                await producer.stop()
                raise
            self._producer = producer

    async def stop(self) -> None:
        async with self._lock:
            if self._producer is None:
                return
            producer, self._producer = self._producer, None
            await producer.stop()

    async def ping(self) -> None:
        """Ensure the producer is started and the cluster metadata is reachable."""
        await self.start()
        assert self._producer is not None
        if not await self._producer.client.force_metadata_update():
            raise ConnectionError("Kafka metadata update failed")

    async def send(self, topic: str, value: Any, key: str | None = None) -> RecordMetadata:
        if self._producer is None:
            raise RuntimeError("Kafka producer is not started")
        return await self._producer.send_and_wait(topic, value=value, key=key)
