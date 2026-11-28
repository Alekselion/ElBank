"""budgets

Revision ID: 3f6a9c1e2b47
Revises: dce1920b579a
Create Date: 2026-11-28 12:00:00.000000

- budgets: monthly spending limit of a user per expense category and currency.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "3f6a9c1e2b47"
down_revision: str | Sequence[str] | None = "dce1920b579a"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CATEGORIES = (
    "utilities",
    "mobile",
    "internet",
    "taxes",
    "fines",
    "qr_payments",
    "bills",
    "transfers",
    "fees",
    "loans",
    "savings",
    "investments",
    "other",
)


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "budgets",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("category", sa.String(32), nullable=False),
        sa.Column("currency", sa.String(3), nullable=False),
        sa.Column("monthly_limit", sa.Numeric(18, 2), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            f"category IN ({', '.join(f"'{c}'" for c in CATEGORIES)})",
            name=op.f("ck_budgets_spending_category"),
        ),
        sa.CheckConstraint("monthly_limit > 0", name=op.f("ck_budgets_monthly_limit_positive")),
        sa.CheckConstraint("currency ~ '^[A-Z]{3}$'", name=op.f("ck_budgets_currency_iso4217")),
        sa.ForeignKeyConstraint(
            ["user_id"], ["users.id"], name=op.f("fk_budgets_user_id_users"), ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_budgets")),
        sa.UniqueConstraint(
            "user_id", "category", "currency", name=op.f("uq_budgets_user_id_category_currency")
        ),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("budgets")
