"""Operator CLI.

``elbank-set-role --phone +79991234567 --role admin`` gives a registered user a role.
It is the way to appoint the first admin (``POST /api/v1/admin/users/{id}/role``
needs an admin already); it is recorded in the audit trail (``audit.event``) with ``user_id=null``.

In docker-compose: ``docker compose run --rm elbank-app elbank-set-role --phone ... --role admin``.
"""

import argparse
import asyncio
import sys

from sqlalchemy import select

from elbank.core.config import get_settings
from elbank.core.logging import setup_logging
from elbank.db.models import User, UserRole, UserStatus
from elbank.db.session import create_engine, create_session_factory
from elbank.kafka import EventPublisher, KafkaProducer
from elbank.services.audit_service import AuditService


async def _set_role(phone: str, role: UserRole) -> str:
    settings = get_settings()
    engine = create_engine(settings.postgres)
    try:
        async with create_session_factory(engine)() as session:
            user = await session.scalar(select(User).where(User.phone == phone).with_for_update())
            if user is None:
                raise LookupError(f"no user with phone {phone}")
            if user.status is UserStatus.DELETED:
                raise LookupError(f"user {user.id} is deleted")
            previous = user.role
            user.role = role
            await session.commit()
    finally:
        await engine.dispose()

    kafka = KafkaProducer(settings.kafka)
    try:
        await AuditService(EventPublisher(kafka)).record(
            "cli.user.role_change",
            user_id=None,  # operator at the console
            entity="user",
            entity_id=user.id,
            payload={"from": previous, "to": role},
        )
    finally:
        await kafka.stop()
    return f"user {user.id}: role {previous.value} -> {role.value}"


def set_role_main() -> None:
    parser = argparse.ArgumentParser(description="Set the role of a registered user")
    parser.add_argument("--phone", required=True, help="E.164, e.g. +79991234567")
    parser.add_argument("--role", required=True, choices=[r.value for r in UserRole])
    args = parser.parse_args()

    setup_logging(get_settings())
    try:
        message = asyncio.run(_set_role(args.phone, UserRole(args.role)))
    except LookupError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(1)
    print(message)
