"""cards

Revision ID: 5b1c8e2d7a90
Revises: 94ed16639a43
Create Date: 2026-11-27 20:00:00.000000

- card_status: new value ``reissued``. VARCHAR + CHECK, so only the CHECK
  constraint changes; autogenerate does not detect it.
- cards: pan_hash (HMAC of the PAN, unique, required), mcc_restrictions (JSONB),
  delivery_address (JSONB, required for plastic cards), reissued_from_id (self FK).
  Existing cards would have no PAN hash, so the migration requires the table to be
  empty (nothing created cards before).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5b1c8e2d7a90"
down_revision: str | Sequence[str] | None = "94ed16639a43"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

STATUS_CK = "ck_cards_card_status"
OLD_STATUSES = ("active", "blocked", "expired", "closed")
NEW_STATUSES = (*OLD_STATUSES, "reissued")


def _status_check(values: Sequence[str]) -> sa.TextClause:
    return sa.text(f"status IN ({', '.join(f"'{v}'" for v in values)})")


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_constraint(op.f(STATUS_CK), "cards", type_="check")
    op.create_check_constraint(op.f(STATUS_CK), "cards", _status_check(NEW_STATUSES))

    op.add_column("cards", sa.Column("pan_hash", sa.String(64), nullable=False))
    op.create_unique_constraint(op.f("uq_cards_pan_hash"), "cards", ["pan_hash"])
    op.add_column(
        "cards",
        sa.Column(
            "mcc_restrictions",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("""'{"allow": [], "deny": []}'::jsonb"""),
            nullable=False,
        ),
    )
    op.add_column(
        "cards",
        sa.Column("delivery_address", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.create_check_constraint(
        op.f("ck_cards_plastic_has_address"),
        "cards",
        sa.text("type <> 'plastic' OR delivery_address IS NOT NULL"),
    )
    op.add_column("cards", sa.Column("reissued_from_id", sa.Uuid(), nullable=True))
    op.create_unique_constraint(op.f("uq_cards_reissued_from_id"), "cards", ["reissued_from_id"])
    op.create_foreign_key(
        op.f("fk_cards_reissued_from_id_cards"),
        "cards",
        "cards",
        ["reissued_from_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        op.f("ck_cards_not_reissued_from_itself"), "cards", sa.text("reissued_from_id <> id")
    )


def downgrade() -> None:
    """Downgrade schema. Fails if reissued cards exist."""
    op.drop_constraint(op.f("ck_cards_not_reissued_from_itself"), "cards", type_="check")
    op.drop_constraint(op.f("fk_cards_reissued_from_id_cards"), "cards", type_="foreignkey")
    op.drop_constraint(op.f("uq_cards_reissued_from_id"), "cards", type_="unique")
    op.drop_column("cards", "reissued_from_id")
    op.drop_constraint(op.f("ck_cards_plastic_has_address"), "cards", type_="check")
    op.drop_column("cards", "delivery_address")
    op.drop_column("cards", "mcc_restrictions")
    op.drop_constraint(op.f("uq_cards_pan_hash"), "cards", type_="unique")
    op.drop_column("cards", "pan_hash")

    op.drop_constraint(op.f(STATUS_CK), "cards", type_="check")
    op.create_check_constraint(op.f(STATUS_CK), "cards", _status_check(OLD_STATUSES))
