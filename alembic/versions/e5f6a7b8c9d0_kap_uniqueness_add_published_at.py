"""kap_all_disclosures uniqueness: (company_code, title) -> (company_code, title, published_at)

Revision ID: e5f6a7b8c9d0
Revises: d4e5f6a7b8c9
Create Date: 2026-05-27

SORUN: Eski unique constraint `(company_code, title)` aynı şirket aynı başlıkla
2. KAP atınca silently REJECT ediyordu. Örnek: ASELS "Yeni İş İlişkisi" başlığı
bir yılda 20+ kere gelebilir; sadece ilki kaydedilir, gerisi sessizce kayboluyor.

ÇÖZÜM: Unique key'e `published_at` ekle. Aynı zamanda (saniye düzeyinde) dedup
korunur ama farklı zamanlardaki aynı başlık kabul edilir.
"""
from typing import Sequence, Union

from alembic import op


revision: str = 'e5f6a7b8c9d0'
down_revision: Union[str, None] = 'd4e5f6a7b8c9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Eski constraint'i dusur
    op.drop_constraint("uq_kap_company_title", "kap_all_disclosures", type_="unique")
    # Yeni constraint: published_at dahil
    op.create_unique_constraint(
        "uq_kap_company_title_published",
        "kap_all_disclosures",
        ["company_code", "title", "published_at"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_kap_company_title_published", "kap_all_disclosures", type_="unique")
    op.create_unique_constraint(
        "uq_kap_company_title",
        "kap_all_disclosures",
        ["company_code", "title"],
    )
