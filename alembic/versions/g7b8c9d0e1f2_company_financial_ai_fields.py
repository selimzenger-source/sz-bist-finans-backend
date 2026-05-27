"""company_financials: ai_score / ai_label / ai_summary / ai_analyzed_at

Revision ID: g7b8c9d0e1f2
Revises: f6a7b8c9d0e1
Create Date: 2026-05-28

Bilanco pipeline AI analizini company_financials tablosuna kaydetmek icin
4 yeni alan ekler. Onceden AI sonucu sadece tweet/notification icin uretiliyor,
DB'ye yazilmiyordu - bu yuzden bilanco havuzu hep "AI: 5.0" (KapAllDisclosure
defaultu) gosteriyordu.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'g7b8c9d0e1f2'
down_revision: Union[str, None] = 'f6a7b8c9d0e1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('company_financials', sa.Column('ai_score', sa.Float(), nullable=True))
    op.add_column('company_financials', sa.Column('ai_label', sa.String(32), nullable=True))
    op.add_column('company_financials', sa.Column('ai_summary', sa.Text(), nullable=True))
    op.add_column('company_financials', sa.Column('ai_analyzed_at', sa.DateTime(timezone=True), nullable=True))


def downgrade() -> None:
    op.drop_column('company_financials', 'ai_analyzed_at')
    op.drop_column('company_financials', 'ai_summary')
    op.drop_column('company_financials', 'ai_label')
    op.drop_column('company_financials', 'ai_score')
