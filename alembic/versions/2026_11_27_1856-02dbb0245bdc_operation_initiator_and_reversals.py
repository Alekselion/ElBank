"""operation initiator and reversals

Revision ID: 02dbb0245bdc
Revises: c5c38f41d469
Create Date: 2026-11-27 18:56:30.872049

- operations.user_id: initiator of the operation (NULL for bank postings).
- Idempotency key is now unique per initiator (user_id, idempotency_key)
  instead of per account.
- operations.reversed_operation_id: a compensating posting of a cancelled
  transfer references the posting it reverses.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "02dbb0245bdc"
down_revision: str | Sequence[str] | None = "c5c38f41d469"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("operations", sa.Column("user_id", sa.Uuid(), nullable=True))
    op.add_column("operations", sa.Column("reversed_operation_id", sa.Uuid(), nullable=True))
    op.drop_constraint(
        op.f("uq_operations_account_id_idempotency_key"), "operations", type_="unique"
    )
    op.create_unique_constraint(
        op.f("uq_operations_user_id_idempotency_key"), "operations", ["user_id", "idempotency_key"]
    )
    op.create_foreign_key(
        op.f("fk_operations_reversed_operation_id_operations"),
        "operations",
        "operations",
        ["reversed_operation_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        op.f("fk_operations_user_id_users"),
        "operations",
        "users",
        ["user_id"],
        ["id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(op.f("fk_operations_user_id_users"), "operations", type_="foreignkey")
    op.drop_constraint(
        op.f("fk_operations_reversed_operation_id_operations"), "operations", type_="foreignkey"
    )
    op.drop_constraint(op.f("uq_operations_user_id_idempotency_key"), "operations", type_="unique")
    op.create_unique_constraint(
        op.f("uq_operations_account_id_idempotency_key"),
        "operations",
        ["account_id", "idempotency_key"],
    )
    op.drop_column("operations", "reversed_operation_id")
    op.drop_column("operations", "user_id")
