"""profile fields and kyc decision

Revision ID: c5c38f41d469
Revises: eb62f5675775
Create Date: 2026-11-25 02:46:35.902742

- users: full_name, address, notification_settings (JSONB).
- kyc_requests: decided_at, decided_by_id, index for the manager queue.
- kyc_status: "in_review" is replaced by "manual_review" (VARCHAR + CHECK,
  so only the CHECK constraint changes; autogenerate does not detect it).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c5c38f41d469"
down_revision: str | Sequence[str] | None = "eb62f5675775"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

KYC_STATUS_CK = "ck_kyc_requests_kyc_status"
OLD_KYC_STATUSES = ("pending", "in_review", "approved", "rejected")
NEW_KYC_STATUSES = ("pending", "manual_review", "approved", "rejected")


def _status_check(statuses: Sequence[str]) -> str:
    values = ", ".join(f"'{s}'" for s in statuses)
    return f"status IN ({values})"


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("users", sa.Column("full_name", sa.String(length=255), nullable=True))
    op.add_column("users", sa.Column("address", sa.String(length=500), nullable=True))
    op.add_column(
        "users",
        sa.Column(
            "notification_settings",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
    )

    op.add_column(
        "kyc_requests", sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column("kyc_requests", sa.Column("decided_by_id", sa.Uuid(), nullable=True))
    op.create_foreign_key(
        op.f("fk_kyc_requests_decided_by_id_users"),
        "kyc_requests",
        "users",
        ["decided_by_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        op.f("ix_kyc_requests_status_created_at"),
        "kyc_requests",
        ["status", "created_at"],
        unique=False,
    )

    op.drop_constraint(op.f(KYC_STATUS_CK), "kyc_requests", type_="check")
    op.execute("UPDATE kyc_requests SET status = 'manual_review' WHERE status = 'in_review'")
    op.create_check_constraint(
        op.f(KYC_STATUS_CK), "kyc_requests", sa.text(_status_check(NEW_KYC_STATUSES))
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(op.f(KYC_STATUS_CK), "kyc_requests", type_="check")
    op.execute("UPDATE kyc_requests SET status = 'in_review' WHERE status = 'manual_review'")
    op.create_check_constraint(
        op.f(KYC_STATUS_CK), "kyc_requests", sa.text(_status_check(OLD_KYC_STATUSES))
    )

    op.drop_index(op.f("ix_kyc_requests_status_created_at"), table_name="kyc_requests")
    op.drop_constraint(
        op.f("fk_kyc_requests_decided_by_id_users"), "kyc_requests", type_="foreignkey"
    )
    op.drop_column("kyc_requests", "decided_by_id")
    op.drop_column("kyc_requests", "decided_at")

    op.drop_column("users", "notification_settings")
    op.drop_column("users", "address")
    op.drop_column("users", "full_name")
