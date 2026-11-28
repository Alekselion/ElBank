"""Tariffs: fee rules per operation code, editable in the back office.

The rule is read when an operation is created, so an edit applies to new
operations only (already booked fees never change). Without a ``tariffs`` row
the defaults from the environment apply (``TRANSFERS_FEE_*``,
``TRANSFERS_CONVERSION_FEE_PERCENT``).
"""

import uuid
from dataclasses import dataclass
from datetime import datetime

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from elbank.core.config import TransferSettings
from elbank.core.logging import get_logger
from elbank.db.models import Tariff, TariffCode, User
from elbank.schemas.admin import FeeRule
from elbank.services.audit_service import AuditService

logger = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class TariffView:
    code: TariffCode
    rule: FeeRule
    is_default: bool  # no row in the DB: the environment default applies
    updated_at: datetime | None
    updated_by_id: uuid.UUID | None


def default_rule(code: TariffCode, settings: TransferSettings) -> FeeRule:
    match code:
        case TariffCode.INTERNAL_TRANSFER:
            return FeeRule(percent=settings.fee_percent, min=settings.fee_min, max=settings.fee_max)
        case TariffCode.CONVERSION:
            return FeeRule(percent=settings.conversion_fee_percent)


def resolve_rule(code: TariffCode, tariff: Tariff | None, settings: TransferSettings) -> FeeRule:
    """Stored rule, or the default; a corrupted stored config falls back to the default."""
    if tariff is None:
        return default_rule(code, settings)
    try:
        return FeeRule.model_validate(tariff.config)
    except ValidationError as exc:
        logger.error(  # noqa: TRY400 - the validation error is logged
            "invalid tariff config, default used", code=code.value, error=str(exc)
        )
        return default_rule(code, settings)


class TariffService:
    def __init__(
        self, session: AsyncSession, settings: TransferSettings, audit: AuditService
    ) -> None:
        self._session = session
        self._audit = audit
        self._settings = settings

    async def fee_rule(self, code: TariffCode) -> FeeRule:
        """Rule for a new operation."""
        return self._rule(code, await self._session.get(Tariff, code))

    async def list_all(self) -> list[TariffView]:
        stored = {t.code: t for t in await self._session.scalars(select(Tariff))}
        views = []
        for code in TariffCode:
            tariff = stored.get(code)
            rule = self._rule(code, tariff)
            if tariff is None:
                views.append(TariffView(code, rule, True, None, None))
            else:
                views.append(TariffView(code, rule, False, tariff.updated_at, tariff.updated_by_id))
        return views

    async def update(
        self, admin: User, rules: dict[TariffCode, FeeRule | None]
    ) -> list[TariffView]:
        """Set (or, with ``None``, reset to the default) the given tariffs in one transaction."""
        changes: dict[str, object] = {}
        for code, rule in rules.items():
            tariff = await self._session.get(Tariff, code, with_for_update=True)
            old = self._rule(code, tariff)
            if rule is None:
                if tariff is not None:
                    await self._session.delete(tariff)
            elif tariff is None:
                self._session.add(
                    Tariff(code=code, config=rule.model_dump(mode="json"), updated_by_id=admin.id)
                )
            else:
                tariff.config = rule.model_dump(mode="json")
                tariff.updated_by_id = admin.id
            new = rule or default_rule(code, self._settings)
            changes[code.value] = {
                "old": old.model_dump(mode="json"),
                "new": new.model_dump(mode="json"),
                "reset": rule is None,
            }
        await self._session.commit()
        await self._audit.record(
            "admin.tariffs.update",
            user_id=admin.id,
            entity="tariff",
            entity_id=",".join(sorted(changes)),
            payload=changes,
        )
        logger.info("tariffs updated", codes=sorted(changes))
        return await self.list_all()

    def _rule(self, code: TariffCode, tariff: Tariff | None) -> FeeRule:
        return resolve_rule(code, tariff, self._settings)
