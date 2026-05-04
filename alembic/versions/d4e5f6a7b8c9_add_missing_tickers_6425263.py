"""6425263 cok-sembollu KAP haberi icin eksik ticker kayitlarini ekle

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-05-04

Sembol: CEMZY,DAPGM,ISBIR,IZENR,LKMNH,PGSUS,RUBNS
Baslik: Borsada Islem Goren Tipe Donusum Duyurusu

Telegram poller multi-symbol desteklesindi de _is_valid_bist_ticker
BigPara cache'inde olmayan tickerlari (yeni listelenen RUBNS, IZENR vb.)
filtreliyordu. Sonuc: sadece CEMZY DB'ye yazildi.

Bu migration eksik 6 ticker icin (DAPGM, ISBIR, IZENR, LKMNH, PGSUS,
RUBNS) ayni AI sonuclari ile yeni kap_all_disclosures kayitlari ekler.
"""
from typing import Sequence, Union

from alembic import op


revision: str = 'd4e5f6a7b8c9'
down_revision: Union[str, None] = 'c3d4e5f6a7b8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # CEMZY kaydindaki AI sonuclarini kullanarak eksik tickerlari ekle.
    # Title bazli unique constraint var, ayni title farkli company_code ile
    # cakismaz.
    op.execute("""
        INSERT INTO kap_all_disclosures
            (company_code, title, body, category, is_bilanco, kap_url,
             source, published_at, ai_sentiment, ai_impact_score, ai_summary,
             ai_analyzed_at, created_at)
        SELECT
            t.code AS company_code,
            existing.title,
            existing.body,
            existing.category,
            existing.is_bilanco,
            existing.kap_url,
            existing.source,
            existing.published_at,
            existing.ai_sentiment,
            existing.ai_impact_score,
            existing.ai_summary,
            existing.ai_analyzed_at,
            NOW()
        FROM (
            SELECT title, body, category, is_bilanco, kap_url, source,
                   published_at, ai_sentiment, ai_impact_score, ai_summary,
                   ai_analyzed_at
            FROM kap_all_disclosures
            WHERE company_code = 'CEMZY'
              AND (kap_url LIKE '%6425263%'
                   OR title = 'Borsada İşlem Gören Tipe Dönüşüm Duyurusu')
            ORDER BY created_at DESC
            LIMIT 1
        ) existing
        CROSS JOIN (
            VALUES ('DAPGM'), ('ISBIR'), ('IZENR'), ('LKMNH'), ('PGSUS'), ('RUBNS')
        ) AS t(code)
        WHERE NOT EXISTS (
            SELECT 1 FROM kap_all_disclosures
            WHERE company_code = t.code AND title = existing.title
        )
    """)


def downgrade() -> None:
    op.execute("""
        DELETE FROM kap_all_disclosures
        WHERE company_code IN ('DAPGM', 'ISBIR', 'IZENR', 'LKMNH', 'PGSUS', 'RUBNS')
          AND title = 'Borsada İşlem Gören Tipe Dönüşüm Duyurusu'
    """)
