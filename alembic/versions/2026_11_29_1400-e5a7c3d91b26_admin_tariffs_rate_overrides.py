"""admin: tariffs and rate overrides

Revision ID: e5a7c3d91b26
Revises: b41e6d2c8a75
Create Date: 2026-11-29 14:00:00.000000

- tariffs: fee config (jsonb) per operation code; no row means env defaults.
- rate_overrides: exchange rates set by an admin instead of the Bank of Russia rate.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e5a7c3d91b26"
down_revision: str | Sequence[str] | None = "b41e6d2c8a75"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TARIFF_CODES = ("internal_transfer", "conversion")


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "tariffs",
        sa.Column("code", sa.String(32), nullable=False),
        sa.Column(
            "config",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("updated_by_id", sa.Uuid(), nullable=True),
        sa.CheckConstraint(
            f"code IN ({', '.join(f"'{c}'" for c in TARIFF_CODES)})",
            name=op.f("ck_tariffs_tariff_code"),
        ),
        sa.ForeignKeyConstraint(
            ["updated_by_id"],
            ["users.id"],
            name=op.f("fk_tariffs_updated_by_id_users"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("code", name=op.f("pk_tariffs")),
    )
    op.create_table(
        "rate_overrides",
        sa.Column("base", sa.String(3), nullable=False),
        sa.Column("quote", sa.String(3), nullable=False),
        sa.Column("rate", sa.Numeric(18, 8), nullable=False),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("updated_by_id", sa.Uuid(), nullable=True),
        sa.CheckConstraint("rate > 0", name=op.f("ck_rate_overrides_rate_positive")),
        sa.CheckConstraint("base <> quote", name=op.f("ck_rate_overrides_distinct_currencies")),
        sa.CheckConstraint("base ~ '^[A-Z]{3}$'", name=op.f("ck_rate_overrides_base_iso4217")),
        sa.CheckConstraint("quote ~ '^[A-Z]{3}$'", name=op.f("ck_rate_overrides_quote_iso4217")),
        sa.ForeignKeyConstraint(
            ["updated_by_id"],
            ["users.id"],
            name=op.f("fk_rate_overrides_updated_by_id_users"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("base", "quote", name=op.f("pk_rate_overrides")),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("rate_overrides")
    op.drop_table("tariffs")
