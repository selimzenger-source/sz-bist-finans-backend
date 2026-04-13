"""add notification_sent_at and tweet_sent_at to kurum_onerileri

Revision ID: a1b2c3d4e5f6
Revises: cde12b0a71be
Create Date: 2026-04-13

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, None] = 'cde12b0a71be'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('kurum_onerileri', sa.Column('notification_sent_at', sa.DateTime(timezone=True), nullable=True))
    op.add_column('kurum_onerileri', sa.Column('tweet_sent_at', sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column('kurum_onerileri', 'tweet_sent_at')
    op.drop_column('kurum_onerileri', 'notification_sent_at')
