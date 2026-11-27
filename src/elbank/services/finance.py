"""Money math of credits and deposits: annuity schedules and daily interest.

Amounts are ``Decimal`` rounded half-up to kopecks; schedule entries are JSON-ready
dicts (dates and amounts as strings) stored in JSONB.
"""

import calendar
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from elbank.services.ledger import CENT

ZERO = Decimal("0.00")


def money(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def fmt(value: Decimal) -> str:
    return format(money(value), "f")


def add_months(start: date, months: int) -> date:
    """Same day ``months`` later, clamped to the month end (31 Jan + 1 → 28/29 Feb)."""
    month_index = start.month - 1 + months
    year, month = start.year + month_index // 12, month_index % 12 + 1
    return date(year, month, min(start.day, calendar.monthrange(year, month)[1]))


# ---------- annuity ----------


def annuity_payment(principal: Decimal, annual_rate: Decimal, months: int) -> Decimal:
    """Monthly payment ``P * r / (1 - (1 + r) ** -n)``, ``r`` = monthly rate."""
    if months <= 0:
        raise ValueError("months must be positive")
    monthly_rate = annual_rate / 100 / 12
    if monthly_rate == 0:
        return money(principal / months)
    return money(principal * monthly_rate / (1 - (1 + monthly_rate) ** -months))


def annuity_schedule(
    principal: Decimal,
    annual_rate: Decimal,
    due_dates: list[date],
    *,
    first_number: int = 1,
) -> list[dict[str, Any]]:
    """Installments for ``principal`` paid on ``due_dates`` (one per month).

    Interest of a month is the remaining principal times the monthly rate; the last
    installment takes the rounding remainder, so the principal parts sum exactly
    to ``principal``.
    """
    payment = annuity_payment(principal, annual_rate, len(due_dates))
    monthly_rate = annual_rate / 100 / 12
    balance = principal
    entries: list[dict[str, Any]] = []
    for index, due_date in enumerate(due_dates):
        interest = money(balance * monthly_rate)
        is_last = index == len(due_dates) - 1
        principal_part = balance if is_last else min(payment - interest, balance)
        balance -= principal_part
        entries.append(
            {
                "n": first_number + index,
                "due_date": due_date.isoformat(),
                "payment": fmt(principal_part + interest),
                "principal": fmt(principal_part),
                "interest": fmt(interest),
                "paid_principal": fmt(ZERO),
                "paid_interest": fmt(ZERO),
                "paid_at": None,
            }
        )
    return entries


def due_dates_from(start: date, months: int) -> list[date]:
    return [add_months(start, n) for n in range(1, months + 1)]


def installment_left(entry: dict[str, Any]) -> tuple[Decimal, Decimal]:
    """Unpaid (interest, principal) of a schedule entry."""
    return (
        Decimal(entry["interest"]) - Decimal(entry["paid_interest"]),
        Decimal(entry["principal"]) - Decimal(entry["paid_principal"]),
    )


def is_paid(entry: dict[str, Any]) -> bool:
    return entry["paid_at"] is not None


def is_untouched(entry: dict[str, Any]) -> bool:
    return Decimal(entry["paid_interest"]) == 0 and Decimal(entry["paid_principal"]) == 0


# ---------- daily interest ----------


def _days_in_year(year: int) -> int:
    return 366 if calendar.isleap(year) else 365


def daily_interest(
    principal: Decimal, annual_rate: Decimal, date_from: date, date_to: date
) -> Decimal:
    """Simple interest for the days in ``[date_from, date_to)``, actual/actual by year."""
    if date_to <= date_from:
        return ZERO
    total = Decimal(0)
    current = date_from
    while current < date_to:
        year_end = date(current.year + 1, 1, 1)
        segment_end = min(year_end, date_to)
        days = (segment_end - current).days
        total += principal * annual_rate / 100 * days / _days_in_year(current.year)
        current = segment_end
    return money(total)


def accrual_entry(
    principal: Decimal, annual_rate: Decimal, date_from: date, date_to: date
) -> dict[str, Any]:
    return {
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "days": (date_to - date_from).days,
        "principal": fmt(principal),
        "interest": fmt(daily_interest(principal, annual_rate, date_from, date_to)),
    }


def interest_at_rate(entries: list[dict[str, Any]], annual_rate: Decimal) -> Decimal:
    """Recalculate accrual entries at another rate (early closing of a deposit)."""
    return sum(
        (
            daily_interest(
                Decimal(e["principal"]),
                annual_rate,
                date.fromisoformat(e["date_from"]),
                date.fromisoformat(e["date_to"]),
            )
            for e in entries
        ),
        ZERO,
    )
