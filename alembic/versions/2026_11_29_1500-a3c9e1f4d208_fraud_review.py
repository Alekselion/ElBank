"""fraud review: JSON rule conditions, fraud cases, fraud tickets

Revision ID: a3c9e1f4d208
Revises: e5a7c3d91b26
Create Date: 2026-11-29 15:00:00.000000

- fraud_rules: ``expression`` (text, never evaluated before) → ``condition`` (jsonb DSL)
  + ``description``. Existing rules cannot be converted and are removed.
- fraud_cases: operations held by anti-fraud rules and the manual decision.
- ticket_category: + ``fraud`` (support_tickets and faq_articles).
- support_messages.author_id: nullable (automatic messages of the bank).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "a3c9e1f4d208"
down_revision: str | Sequence[str] | None = "e5a7c3d91b26"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

OLD_CATEGORIES = (
    "accounts",
    "cards",
    "transfers",
    "payments",
    "credits",
    "deposits",
    "investments",
    "technical",
    "other",
)
NEW_CATEGORIES = (*OLD_CATEGORIES, "fraud")
CASE_STATUSES = ("open", "confirmed", "rejected")


def _category_check(categories: Sequence[str]) -> sa.TextClause:
    return sa.text(f"category IN ({', '.join(f"'{c}'" for c in categories)})")


def _set_categories(categories: Sequence[str]) -> None:
    for table in ("support_tickets", "faq_articles"):
        name = op.f(f"ck_{table}_ticket_category")
        op.drop_constraint(name, table, type_="check")
        op.create_check_constraint(name, table, _category_check(categories))


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(sa.text("DELETE FROM fraud_rules"))
    op.drop_column("fraud_rules", "expression")
    op.add_column(
        "fraud_rules",
        sa.Column("condition", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    )
    op.add_column("fraud_rules", sa.Column("description", sa.Text(), nullable=True))

    _set_categories(NEW_CATEGORIES)
    op.alter_column("support_messages", "author_id", existing_type=sa.Uuid(), nullable=True)

    op.create_table(
        "fraud_cases",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("operation_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(32), server_default="open", nullable=False),
        sa.Column("rules", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "resume",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("ticket_id", sa.Uuid(), nullable=True),
        sa.Column("decided_by_id", sa.Uuid(), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("decision_reason", sa.String(500), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            f"status IN ({', '.join(f"'{s}'" for s in CASE_STATUSES)})",
            name=op.f("ck_fraud_cases_fraud_case_status"),
        ),
        sa.ForeignKeyConstraint(
            ["operation_id"],
            ["operations.id"],
            name=op.f("fk_fraud_cases_operation_id_operations"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_fraud_cases_user_id_users"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["ticket_id"],
            ["support_tickets.id"],
            name=op.f("fk_fraud_cases_ticket_id_support_tickets"),
            ondelete="SET NULL",
        ),
        sa.ForeignKeyConstraint(
            ["decided_by_id"],
            ["users.id"],
            name=op.f("fk_fraud_cases_decided_by_id_users"),
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_fraud_cases")),
        sa.UniqueConstraint("operation_id", name=op.f("uq_fraud_cases_operation_id")),
    )
    op.create_index(
        op.f("ix_fraud_cases_status_created_at"), "fraud_cases", ["status", "created_at"]
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f("ix_fraud_cases_status_created_at"), table_name="fraud_cases")
    op.drop_table("fraud_cases")

    op.execute(sa.text("DELETE FROM support_messages WHERE author_id IS NULL"))
    op.alter_column("support_messages", "author_id", existing_type=sa.Uuid(), nullable=False)
    op.execute(sa.text("UPDATE support_tickets SET category = 'other' WHERE category = 'fraud'"))
    op.execute(sa.text("UPDATE faq_articles SET category = 'other' WHERE category = 'fraud'"))
    _set_categories(OLD_CATEGORIES)

    op.execute(sa.text("DELETE FROM fraud_rules"))
    op.drop_column("fraud_rules", "description")
    op.drop_column("fraud_rules", "condition")
    op.add_column(
        "fraud_rules", sa.Column("expression", sa.Text(), server_default="", nullable=False)
    )
    op.alter_column("fraud_rules", "expression", server_default=None)
