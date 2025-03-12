"""Initial migration

Revision ID: 8193bb2d2789
Revises:
Create Date: 2025-03-12 22:46:30.770091

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "8193bb2d2789"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "exchanges",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("api_rate_limit", sa.Integer(), nullable=True),
        sa.Column("trade_fees", sa.Numeric(precision=18, scale=8), nullable=True),
        sa.Column("additional_info", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=True,
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )
    op.create_table(
        "trading_pairs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("base_currency", sa.String(), nullable=False),
        sa.Column("quote_currency", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("base_currency", "quote_currency", name="uix_base_quote"),
    )
    op.create_table(
        "trade_opportunities",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("trading_pair_id", sa.Integer(), nullable=True),
        sa.Column("buy_exchange_id", sa.Integer(), nullable=True),
        sa.Column("sell_exchange_id", sa.Integer(), nullable=True),
        sa.Column("buy_price", sa.Numeric(precision=18, scale=8), nullable=False),
        sa.Column("sell_price", sa.Numeric(precision=18, scale=8), nullable=False),
        sa.Column("spread", sa.Numeric(precision=18, scale=8), nullable=False),
        sa.Column("volume", sa.Numeric(precision=18, scale=8), nullable=False),
        sa.Column("opportunity_timestamp", sa.TIMESTAMP(), nullable=False),
        sa.ForeignKeyConstraint(
            ["buy_exchange_id"],
            ["exchanges.id"],
        ),
        sa.ForeignKeyConstraint(
            ["sell_exchange_id"],
            ["exchanges.id"],
        ),
        sa.ForeignKeyConstraint(
            ["trading_pair_id"],
            ["trading_pairs.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "trade_executions",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("trading_pair_id", sa.Integer(), nullable=True),
        sa.Column("buy_exchange_id", sa.Integer(), nullable=True),
        sa.Column("sell_exchange_id", sa.Integer(), nullable=True),
        sa.Column(
            "executed_buy_price", sa.Numeric(precision=18, scale=8), nullable=False
        ),
        sa.Column(
            "executed_sell_price", sa.Numeric(precision=18, scale=8), nullable=False
        ),
        sa.Column("spread", sa.Numeric(precision=18, scale=8), nullable=False),
        sa.Column("volume", sa.Numeric(precision=18, scale=8), nullable=False),
        sa.Column("execution_timestamp", sa.TIMESTAMP(), nullable=False),
        sa.Column("execution_id", sa.String(), nullable=True),
        sa.Column("opportunity_id", sa.UUID(), nullable=True),
        sa.ForeignKeyConstraint(
            ["buy_exchange_id"],
            ["exchanges.id"],
        ),
        sa.ForeignKeyConstraint(
            ["opportunity_id"],
            ["trade_opportunities.id"],
        ),
        sa.ForeignKeyConstraint(
            ["sell_exchange_id"],
            ["exchanges.id"],
        ),
        sa.ForeignKeyConstraint(
            ["trading_pair_id"],
            ["trading_pairs.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    """Downgrade schema."""
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("trade_executions")
    op.drop_table("trade_opportunities")
    op.drop_table("trading_pairs")
    op.drop_table("exchanges")
    # ### end Alembic commands ###
