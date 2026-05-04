"""rename Notr -> Nötr in ai_sentiment columns

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-05-04

Tum tablolardaki 'Notr' degerlerini Turkce dogru yazimla 'Nötr' yapar.
- kap_all_disclosures.ai_sentiment
"""
from typing import Sequence, Union

from alembic import op


revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # kap_all_disclosures.ai_sentiment: 'Notr' -> 'Nötr'
    op.execute(
        "UPDATE kap_all_disclosures SET ai_sentiment = 'Nötr' WHERE ai_sentiment = 'Notr'"
    )


def downgrade() -> None:
    op.execute(
        "UPDATE kap_all_disclosures SET ai_sentiment = 'Notr' WHERE ai_sentiment = 'Nötr'"
    )
