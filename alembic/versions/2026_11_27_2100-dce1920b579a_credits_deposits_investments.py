"""credits deposits investments

Revision ID: dce1920b579a
Revises: 5b1c8e2d7a90
Create Date: 2026-11-27 21:00:00.000000

- operation_type: new values credit_disbursement, credit_repayment, deposit_open,
  deposit_top_up, deposit_close, investment_buy, investment_sell (CHECK only).
- credits: account_id, purpose, scoring result, monthly_payment,
  principal_outstanding, signed_at, closed_at.
- deposits: currency, term dates, accrual state, paid_interest, closed_at. The
  CHECK constraints created by the init migration were named ck_credits_* (a
  constraint object shared by both models) and are renamed to ck_deposits_*.
- investment_orders, investment_positions: new tables.

credits and deposits get required columns without defaults, so the migration
requires both tables to be empty (nothing created them before).
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "dce1920b579a"
down_revision: str | Sequence[str] | None = "5b1c8e2d7a90"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

OPERATION_TYPE_CK = "ck_operations_operation_type"
OLD_TYPES = (
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
NEW_TYPES = (
    *OLD_TYPES,
    "credit_disbursement",
    "credit_repayment",
    "deposit_open",
    "deposit_top_up",
    "deposit_close",
    "investment_buy",
    "investment_sell",
)
DEPOSIT_CK_RENAMES = {
    "ck_credits_amount_positive": "ck_deposits_amount_positive",
    "ck_credits_rate_non_negative": "ck_deposits_rate_non_negative",
    "ck_credits_term_positive": "ck_deposits_term_positive",
}
CREDIT_CHECKS = {
    "ck_credits_outstanding_non_negative": "principal_outstanding >= 0",
    "ck_credits_outstanding_lte_amount": "principal_outstanding <= amount",
}
DEPOSIT_CHECKS = {
    "ck_deposits_currency_iso4217": "currency ~ '^[A-Z]{3}$'",
    "ck_deposits_interest_non_negative": "accrued_interest >= 0",
    "ck_deposits_end_after_start": "end_date > start_date",
    "ck_deposits_accrued_within_term": "accrued_until BETWEEN start_date AND end_date",
}


def _type_check(values: Sequence[str]) -> sa.TextClause:
    return sa.text(f"type IN ({', '.join(f"'{v}'" for v in values)})")


def _rename_constraints(table: str, renames: dict[str, str]) -> None:
    for old, new in renames.items():
        op.execute(f'ALTER TABLE {table} RENAME CONSTRAINT "{old}" TO "{new}"')


def _timestamps() -> list[sa.Column[object]]:
    return [
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    ]


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_constraint(op.f(OPERATION_TYPE_CK), "operations", type_="check")
    op.create_check_constraint(op.f(OPERATION_TYPE_CK), "operations", _type_check(NEW_TYPES))

    # ---------- credits ----------
    op.add_column("credits", sa.Column("account_id", sa.Uuid(), nullable=False))
    op.add_column("credits", sa.Column("purpose", sa.String(255), nullable=False))
    op.add_column("credits", sa.Column("score", sa.Integer(), nullable=True))
    op.add_column("credits", sa.Column("scoring_comment", sa.String(500), nullable=True))
    op.add_column("credits", sa.Column("monthly_payment", sa.Numeric(18, 2), nullable=True))
    op.add_column(
        "credits",
        sa.Column("principal_outstanding", sa.Numeric(18, 2), server_default="0", nullable=False),
    )
    op.add_column("credits", sa.Column("signed_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("credits", sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True))
    op.create_index(op.f("ix_credits_account_id"), "credits", ["account_id"], unique=False)
    op.create_foreign_key(
        op.f("fk_credits_account_id_accounts"),
        "credits",
        "accounts",
        ["account_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    for name, condition in CREDIT_CHECKS.items():
        op.create_check_constraint(op.f(name), "credits", sa.text(condition))

    # ---------- deposits ----------
    _rename_constraints("deposits", DEPOSIT_CK_RENAMES)
    op.add_column("deposits", sa.Column("currency", sa.String(3), nullable=False))
    op.add_column("deposits", sa.Column("start_date", sa.Date(), nullable=False))
    op.add_column("deposits", sa.Column("end_date", sa.Date(), nullable=False))
    op.add_column("deposits", sa.Column("accrued_until", sa.Date(), nullable=False))
    op.add_column(
        "deposits",
        sa.Column("accrued_interest", sa.Numeric(18, 2), server_default="0", nullable=False),
    )
    op.add_column("deposits", sa.Column("paid_interest", sa.Numeric(18, 2), nullable=True))
    op.add_column("deposits", sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True))
    for name, condition in DEPOSIT_CHECKS.items():
        op.create_check_constraint(op.f(name), "deposits", sa.text(condition))

    # ---------- investments ----------
    op.create_table(
        "investment_positions",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("ticker", sa.String(12), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("avg_price", sa.Numeric(18, 4), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        *_timestamps(),
        sa.CheckConstraint(
            "currency ~ '^[A-Z]{3}$'", name=op.f("ck_investment_positions_currency_iso4217")
        ),
        sa.CheckConstraint("quantity > 0", name=op.f("ck_investment_positions_quantity_positive")),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_investment_positions_user_id_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_investment_positions")),
        sa.UniqueConstraint(
            "user_id", "ticker", name=op.f("uq_investment_positions_user_id_ticker")
        ),
    )
    op.create_table(
        "investment_orders",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("account_id", sa.Uuid(), nullable=False),
        sa.Column("ticker", sa.String(12), nullable=False),
        sa.Column(
            "side",
            sa.Enum(
                "buy",
                "sell",
                name="investment_side",
                native_enum=False,
                create_constraint=True,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("limit_price", sa.Numeric(18, 4), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "filled",
                "rejected",
                name="investment_order_status",
                native_enum=False,
                create_constraint=True,
                length=32,
            ),
            nullable=False,
        ),
        sa.Column("price", sa.Numeric(18, 4), nullable=True),
        sa.Column("amount", sa.Numeric(18, 2), nullable=True),
        sa.Column("broker_order_id", sa.String(64), nullable=True),
        sa.Column("reject_code", sa.String(64), nullable=True),
        sa.Column("reject_reason", sa.String(255), nullable=True),
        sa.Column("operation_id", sa.Uuid(), nullable=True),
        *_timestamps(),
        sa.CheckConstraint(
            "currency ~ '^[A-Z]{3}$'", name=op.f("ck_investment_orders_currency_iso4217")
        ),
        sa.CheckConstraint(
            "status <> 'filled' OR (price IS NOT NULL AND amount IS NOT NULL"
            " AND operation_id IS NOT NULL)",
            name=op.f("ck_investment_orders_filled_has_execution"),
        ),
        sa.CheckConstraint(
            "limit_price > 0", name=op.f("ck_investment_orders_limit_price_positive")
        ),
        sa.CheckConstraint("quantity > 0", name=op.f("ck_investment_orders_quantity_positive")),
        sa.ForeignKeyConstraint(
            ["account_id"],
            ["accounts.id"],
            name=op.f("fk_investment_orders_account_id_accounts"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["operation_id"],
            ["operations.id"],
            name=op.f("fk_investment_orders_operation_id_operations"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_investment_orders_user_id_users"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_investment_orders")),
        sa.UniqueConstraint("operation_id", name=op.f("uq_investment_orders_operation_id")),
    )
    op.create_index(
        op.f("ix_investment_orders_user_id"), "investment_orders", ["user_id"], unique=False
    )


def downgrade() -> None:
    """Downgrade schema. Fails if operations of the new types exist."""
    op.drop_index(op.f("ix_investment_orders_user_id"), table_name="investment_orders")
    op.drop_table("investment_orders")
    op.drop_table("investment_positions")

    for name in DEPOSIT_CHECKS:
        op.drop_constraint(op.f(name), "deposits", type_="check")
    for column in (
        "closed_at",
        "paid_interest",
        "accrued_interest",
        "accrued_until",
        "end_date",
        "start_date",
        "currency",
    ):
        op.drop_column("deposits", column)
    _rename_constraints("deposits", {new: old for old, new in DEPOSIT_CK_RENAMES.items()})

    for name in CREDIT_CHECKS:
        op.drop_constraint(op.f(name), "credits", type_="check")
    op.drop_constraint(op.f("fk_credits_account_id_accounts"), "credits", type_="foreignkey")
    op.drop_index(op.f("ix_credits_account_id"), table_name="credits")
    for column in (
        "closed_at",
        "signed_at",
        "principal_outstanding",
        "monthly_payment",
        "scoring_comment",
        "score",
        "purpose",
        "account_id",
    ):
        op.drop_column("credits", column)

    op.drop_constraint(op.f(OPERATION_TYPE_CK), "operations", type_="check")
    op.create_check_constraint(op.f(OPERATION_TYPE_CK), "operations", _type_check(OLD_TYPES))
