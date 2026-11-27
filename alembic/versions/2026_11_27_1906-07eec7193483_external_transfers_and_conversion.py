"""external transfers and conversion

Revision ID: 07eec7193483
Revises: 02dbb0245bdc
Create Date: 2026-11-27 19:06:00.000000

- operations.external_id: transaction id from the interbank / SBP gateway.
- operations.exchange_rate: rate of a currency conversion.
- operation_type: new values external_transfer, sbp_transfer, conversion_out,
  conversion_in (VARCHAR + CHECK, so only the CHECK constraint changes;
  autogenerate does not detect it).
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "07eec7193483"
down_revision: str | Sequence[str] | None = "02dbb0245bdc"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

OPERATION_TYPE_CK = "ck_operations_operation_type"
OLD_TYPES = ("transfer_in", "transfer_out", "payment", "fee", "interest")
NEW_TYPES = (
    "transfer_in",
    "transfer_out",
    "external_transfer",
    "sbp_transfer",
    "conversion_out",
    "conversion_in",
    "payment",
    "fee",
    "interest",
)


def _type_check(types: Sequence[str]) -> str:
    values = ", ".join(f"'{t}'" for t in types)
    return f"type IN ({values})"


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("operations", sa.Column("external_id", sa.String(length=64), nullable=True))
    op.add_column(
        "operations",
        sa.Column("exchange_rate", sa.Numeric(precision=18, scale=8), nullable=True),
    )
    op.drop_constraint(op.f(OPERATION_TYPE_CK), "operations", type_="check")
    op.create_check_constraint(op.f(OPERATION_TYPE_CK), "operations", sa.text(_type_check(NEW_TYPES)))


def downgrade() -> None:
    """Downgrade schema.

    Fails if operations of the new types exist: they have no equivalent in the old schema.
    """
    op.drop_constraint(op.f(OPERATION_TYPE_CK), "operations", type_="check")
    op.create_check_constraint(op.f(OPERATION_TYPE_CK), "operations", sa.text(_type_check(OLD_TYPES)))
    op.drop_column("operations", "exchange_rate")
    op.drop_column("operations", "external_id")
