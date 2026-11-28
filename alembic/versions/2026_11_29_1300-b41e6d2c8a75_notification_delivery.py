"""notification delivery

Revision ID: b41e6d2c8a75
Revises: 7c2d4e8a9f13
Create Date: 2026-11-29 13:00:00.000000

- notifications.dedup_key: ``{event_id}:{user_id}``, unique (idempotent consumer);
  rows created before get ``legacy:{id}``.
- notifications: delivery_status (pending/sent/failed/skipped), delivery_note,
  delivery_attempts, provider_message_id, sent_at.
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b41e6d2c8a75"
down_revision: str | Sequence[str] | None = "7c2d4e8a9f13"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

STATUSES = ("pending", "sent", "failed", "skipped")


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("notifications", sa.Column("dedup_key", sa.String(100), nullable=True))
    op.execute(sa.text("UPDATE notifications SET dedup_key = 'legacy:' || id::text"))
    op.alter_column("notifications", "dedup_key", existing_type=sa.String(100), nullable=False)
    op.create_unique_constraint(op.f("uq_notifications_dedup_key"), "notifications", ["dedup_key"])

    op.add_column(
        "notifications",
        sa.Column("delivery_status", sa.String(32), server_default="pending", nullable=False),
    )
    op.create_check_constraint(
        op.f("ck_notifications_notification_delivery_status"),
        "notifications",
        sa.text(f"delivery_status IN ({', '.join(f"'{s}'" for s in STATUSES)})"),
    )
    op.add_column("notifications", sa.Column("delivery_note", sa.String(255), nullable=True))
    op.add_column(
        "notifications",
        sa.Column("delivery_attempts", sa.SmallInteger(), server_default="0", nullable=False),
    )
    op.add_column("notifications", sa.Column("provider_message_id", sa.String(64), nullable=True))
    op.add_column("notifications", sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("notifications", "sent_at")
    op.drop_column("notifications", "provider_message_id")
    op.drop_column("notifications", "delivery_attempts")
    op.drop_column("notifications", "delivery_note")
    op.drop_constraint(
        op.f("ck_notifications_notification_delivery_status"), "notifications", type_="check"
    )
    op.drop_column("notifications", "delivery_status")
    op.drop_constraint(op.f("uq_notifications_dedup_key"), "notifications", type_="unique")
    op.drop_column("notifications", "dedup_key")
