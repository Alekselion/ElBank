"""payments

Revision ID: 94ed16639a43
Revises: 07eec7193483
Create Date: 2026-11-27 19:25:00.000000

- payment_provider: values utility, mobile, internet, tax, traffic_fine, qr,
  receipt (old utilities/taxes/fines are renamed). VARCHAR + CHECK, so only the
  CHECK constraint changes; autogenerate does not detect it.
- payment_orders: operation_id (the debit posting, required, unique) and
  provider_response (JSONB). Existing payment orders would have no operation,
  so the migration requires the table to be empty (nothing created them before).
- transfer_templates: provider + payload for payment templates; recipient_account
  becomes optional, one of provider / recipient_account is required.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "94ed16639a43"
down_revision: str | Sequence[str] | None = "07eec7193483"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ORDERS_PROVIDER_CK = "ck_payment_orders_payment_provider"
TEMPLATES_PROVIDER_CK = "ck_transfer_templates_payment_provider"
TEMPLATES_RECIPIENT_CK = "ck_transfer_templates_has_recipient"
OLD_PROVIDERS = ("mobile", "utilities", "internet", "taxes", "fines")
NEW_PROVIDERS = ("utility", "mobile", "internet", "tax", "traffic_fine", "qr", "receipt")
RENAMES = {"utilities": "utility", "taxes": "tax", "fines": "traffic_fine"}


def _provider_check(values: Sequence[str]) -> sa.TextClause:
    return sa.text(f"provider IN ({', '.join(f"'{v}'" for v in values)})")


def _rename_providers(mapping: dict[str, str]) -> None:
    for old, new in mapping.items():
        op.execute(
            sa.text("UPDATE payment_orders SET provider = :new WHERE provider = :old").bindparams(
                old=old, new=new
            )
        )


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_constraint(op.f(ORDERS_PROVIDER_CK), "payment_orders", type_="check")
    _rename_providers(RENAMES)
    op.create_check_constraint(
        op.f(ORDERS_PROVIDER_CK), "payment_orders", _provider_check(NEW_PROVIDERS)
    )

    op.add_column("payment_orders", sa.Column("operation_id", sa.Uuid(), nullable=False))
    op.add_column(
        "payment_orders",
        sa.Column("provider_response", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.create_unique_constraint(
        op.f("uq_payment_orders_operation_id"), "payment_orders", ["operation_id"]
    )
    op.create_foreign_key(
        op.f("fk_payment_orders_operation_id_operations"),
        "payment_orders",
        "operations",
        ["operation_id"],
        ["id"],
        ondelete="RESTRICT",
    )

    op.add_column("transfer_templates", sa.Column("provider", sa.String(32), nullable=True))
    op.create_check_constraint(
        op.f(TEMPLATES_PROVIDER_CK), "transfer_templates", _provider_check(NEW_PROVIDERS)
    )
    op.add_column(
        "transfer_templates",
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
    )
    op.alter_column(
        "transfer_templates", "recipient_account", existing_type=sa.String(34), nullable=True
    )
    op.create_check_constraint(
        op.f(TEMPLATES_RECIPIENT_CK),
        "transfer_templates",
        sa.text("provider IS NOT NULL OR recipient_account IS NOT NULL"),
    )


def downgrade() -> None:
    """Downgrade schema. Fails if payment templates or qr/receipt payments exist."""
    op.drop_constraint(op.f(TEMPLATES_RECIPIENT_CK), "transfer_templates", type_="check")
    op.alter_column(
        "transfer_templates", "recipient_account", existing_type=sa.String(34), nullable=False
    )
    op.drop_column("transfer_templates", "payload")
    op.drop_constraint(op.f(TEMPLATES_PROVIDER_CK), "transfer_templates", type_="check")
    op.drop_column("transfer_templates", "provider")

    op.drop_constraint(
        op.f("fk_payment_orders_operation_id_operations"), "payment_orders", type_="foreignkey"
    )
    op.drop_constraint(op.f("uq_payment_orders_operation_id"), "payment_orders", type_="unique")
    op.drop_column("payment_orders", "provider_response")
    op.drop_column("payment_orders", "operation_id")

    op.drop_constraint(op.f(ORDERS_PROVIDER_CK), "payment_orders", type_="check")
    _rename_providers({new: old for old, new in RENAMES.items()})
    op.create_check_constraint(
        op.f(ORDERS_PROVIDER_CK), "payment_orders", _provider_check(OLD_PROVIDERS)
    )
