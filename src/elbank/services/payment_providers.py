# ruff: noqa: RUF001 - provider names and descriptions are in Russian on purpose
"""Catalog of service providers (constants: the set is fixed by the integrations)."""

from dataclasses import dataclass
from decimal import Decimal

from elbank.db.models import PaymentProvider


@dataclass(frozen=True, slots=True)
class ProviderInfo:
    code: PaymentProvider
    name: str
    description: str
    min_amount: Decimal
    max_amount: Decimal


PROVIDERS: dict[PaymentProvider, ProviderInfo] = {
    p.code: p
    for p in (
        ProviderInfo(
            PaymentProvider.UTILITY,
            "ЖКУ",
            "Квартплата и коммунальные услуги по лицевому счёту",
            Decimal("1.00"),
            Decimal("500000.00"),
        ),
        ProviderInfo(
            PaymentProvider.MOBILE,
            "Мобильная связь",
            "Пополнение баланса телефона любого российского оператора",
            Decimal("10.00"),
            Decimal("15000.00"),
        ),
        ProviderInfo(
            PaymentProvider.INTERNET,
            "Интернет и ТВ",
            "Оплата домашнего интернета по номеру договора",
            Decimal("1.00"),
            Decimal("100000.00"),
        ),
        ProviderInfo(
            PaymentProvider.TAX,
            "Налоги (ФНС)",
            "Имущественные налоги и НДФЛ по УИН из налогового уведомления",
            Decimal("1.00"),
            Decimal("5000000.00"),
        ),
        ProviderInfo(
            PaymentProvider.TRAFFIC_FINE,
            "Штрафы ГИБДД",
            "Оплата штрафа по УИН постановления",
            Decimal("1.00"),
            Decimal("500000.00"),
        ),
        ProviderInfo(
            PaymentProvider.QR,
            "Оплата по QR-коду",
            "Платёж по реквизитам из QR-кода (ГОСТ Р 56042-2014)",
            Decimal("1.00"),
            Decimal("1000000.00"),
        ),
        ProviderInfo(
            PaymentProvider.RECEIPT,
            "Оплата по квитанции",
            "Платёж по номеру бумажной квитанции",
            Decimal("1.00"),
            Decimal("1000000.00"),
        ),
    )
}
