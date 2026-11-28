"""support

Revision ID: 7c2d4e8a9f13
Revises: 3f6a9c1e2b47
Create Date: 2026-11-29 12:00:00.000000

- support_tickets.category: free text → ticket_category (VARCHAR(32) + CHECK); unknown
  values become ``other``. New columns: assignee_id, closed_at, rating (CSAT 1..5),
  rating_comment, rated_at.
- support_messages.from_staff.
- support_attachments: files attached to messages.
- faq_articles: FAQ, seeded with starter articles.
"""

import uuid
from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7c2d4e8a9f13"
down_revision: str | Sequence[str] | None = "3f6a9c1e2b47"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CATEGORIES = (
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
CATEGORY_CHECK = f"category IN ({', '.join(f"'{c}'" for c in CATEGORIES)})"

FAQ_SEED = (
    (
        "open-account",
        "accounts",
        "Как открыть счёт?",
        "Подтвердите личность (KYC) в разделе «Профиль», затем откройте счёт в разделе «Счета»: "
        "выберите тип (текущий, накопительный, валютный) и валюту.",
    ),
    (
        "statement",
        "accounts",
        "Как получить выписку по счёту?",
        "Закажите выписку за период в карточке счёта. Файл CSV формируется в фоне и доступен "
        "для скачивания 24 часа.",
    ),
    (
        "block-card",
        "cards",
        "Что делать, если карта потеряна?",
        "Сразу заблокируйте карту в приложении. Заблокированную карту можно перевыпустить; "
        "деньги на счёте остаются в сохранности.",
    ),
    (
        "transfer-cancel",
        "transfers",
        "Можно ли отменить перевод?",
        "Внутренний перевод можно отменить в течение минуты после отправки. Межбанковские "
        "переводы и переводы по СБП отменить нельзя.",
    ),
    (
        "transfer-fee",
        "transfers",
        "Какая комиссия за перевод?",
        "Внутренний перевод — 0,5 % от суммы, не меньше 10 ₽ и не больше 5000 ₽. Конвертация "
        "между своими счетами — 1 %.",
    ),
    (
        "payment-pending",
        "payments",
        "Платёж завис в статусе «в обработке»",
        "Если провайдер не ответил вовремя, платёж ждёт подтверждения. Неподтверждённый платёж "
        "автоматически отменяется, деньги возвращаются на счёт.",
    ),
    (
        "credit-early-repayment",
        "credits",
        "Можно ли погасить кредит досрочно?",
        "Да, без комиссии: внесите сумму больше ежемесячного платежа в разделе «Кредиты».",
    ),
    (
        "deposit-early-close",
        "deposits",
        "Что будет при досрочном закрытии вклада?",
        "Сумма вклада вернётся на счёт, а проценты будут пересчитаны по ставке досрочного "
        "закрытия.",
    ),
    (
        "support-response",
        "technical",
        "Как быстро отвечает поддержка?",
        "Обращения обрабатываются в порядке приоритета. Новые ответы приходят в чат обращения "
        "в реальном времени.",
    ),
)


def upgrade() -> None:
    """Upgrade schema."""
    # ---------- support_tickets ----------
    op.execute(
        sa.text(f"UPDATE support_tickets SET category = 'other' WHERE NOT ({CATEGORY_CHECK})")
    )
    op.alter_column(
        "support_tickets",
        "category",
        existing_type=sa.String(50),
        type_=sa.String(32),
        existing_nullable=False,
    )
    op.create_check_constraint(
        op.f("ck_support_tickets_ticket_category"), "support_tickets", sa.text(CATEGORY_CHECK)
    )
    op.add_column("support_tickets", sa.Column("assignee_id", sa.Uuid(), nullable=True))
    op.create_foreign_key(
        op.f("fk_support_tickets_assignee_id_users"),
        "support_tickets",
        "users",
        ["assignee_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.add_column(
        "support_tickets", sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column("support_tickets", sa.Column("rating", sa.SmallInteger(), nullable=True))
    op.add_column("support_tickets", sa.Column("rating_comment", sa.String(1000), nullable=True))
    op.add_column(
        "support_tickets", sa.Column("rated_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.create_check_constraint(
        op.f("ck_support_tickets_rating_range"),
        "support_tickets",
        sa.text("rating IS NULL OR rating BETWEEN 1 AND 5"),
    )

    # ---------- support_messages ----------
    op.add_column(
        "support_messages",
        sa.Column("from_staff", sa.Boolean(), server_default=sa.false(), nullable=False),
    )

    # ---------- support_attachments ----------
    op.create_table(
        "support_attachments",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("message_id", sa.Uuid(), nullable=False),
        sa.Column("file_name", sa.String(255), nullable=False),
        sa.Column("content_type", sa.String(100), nullable=False),
        sa.Column("size", sa.Integer(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint("size > 0", name=op.f("ck_support_attachments_size_positive")),
        sa.ForeignKeyConstraint(
            ["message_id"],
            ["support_messages.id"],
            name=op.f("fk_support_attachments_message_id_support_messages"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_support_attachments")),
    )
    op.create_index(
        op.f("ix_support_attachments_message_id"), "support_attachments", ["message_id"]
    )

    # ---------- faq_articles ----------
    faq = op.create_table(
        "faq_articles",
        sa.Column("id", sa.Uuid(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("slug", sa.String(100), nullable=False),
        sa.Column("category", sa.String(32), nullable=False),
        sa.Column("question", sa.String(300), nullable=False),
        sa.Column("answer", sa.Text(), nullable=False),
        sa.Column("position", sa.Integer(), server_default="0", nullable=False),
        sa.Column("is_published", sa.Boolean(), server_default=sa.true(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(CATEGORY_CHECK, name=op.f("ck_faq_articles_ticket_category")),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_faq_articles")),
        sa.UniqueConstraint("slug", name=op.f("uq_faq_articles_slug")),
    )
    op.create_index(
        op.f("ix_faq_articles_category_position"), "faq_articles", ["category", "position"]
    )
    op.bulk_insert(
        faq,
        [
            {
                "id": uuid.uuid4(),
                "slug": slug,
                "category": category,
                "question": question,
                "answer": answer,
                "position": position,
            }
            for position, (slug, category, question, answer) in enumerate(FAQ_SEED)
        ],
    )


def downgrade() -> None:
    """Downgrade schema. Attachments, ratings and FAQ are dropped."""
    op.drop_index(op.f("ix_faq_articles_category_position"), table_name="faq_articles")
    op.drop_table("faq_articles")
    op.drop_index(op.f("ix_support_attachments_message_id"), table_name="support_attachments")
    op.drop_table("support_attachments")
    op.drop_column("support_messages", "from_staff")
    op.drop_constraint(op.f("ck_support_tickets_rating_range"), "support_tickets", type_="check")
    op.drop_column("support_tickets", "rated_at")
    op.drop_column("support_tickets", "rating_comment")
    op.drop_column("support_tickets", "rating")
    op.drop_column("support_tickets", "closed_at")
    op.drop_constraint(
        op.f("fk_support_tickets_assignee_id_users"), "support_tickets", type_="foreignkey"
    )
    op.drop_column("support_tickets", "assignee_id")
    op.drop_constraint(op.f("ck_support_tickets_ticket_category"), "support_tickets", type_="check")
    op.alter_column(
        "support_tickets",
        "category",
        existing_type=sa.String(32),
        type_=sa.String(50),
        existing_nullable=False,
    )
