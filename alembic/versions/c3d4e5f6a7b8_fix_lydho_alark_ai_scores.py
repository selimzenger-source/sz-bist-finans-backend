"""fix LYDHO 6424999 ve ALARK/EGGUB/KFEIN 6423117 yanlis AI puanlari

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-05-04

Eski AI prompt'u bu iki haberi yanlis puanlamisti:
- LYDHO 6424999: 6.1 pozitif olarak isaretlenmis (push gitmis), oysa
  icerik "kar payi dagitilmamasi" karari → 3.8 NEGATIF olmali
- ALARK 6423117: 6.5 pozitif olarak isaretlenmis, oysa BISTECH ex-div
  gunu teknik bildirimi (temettu zaten onceden ilan edilmis) → 5.1 NÖTR

Yeni puanlar yeni AI prompt ile manuel test edilip dogrulanmistir.
Bu migration sadece ai_sentiment, ai_impact_score, ai_summary alanlarini
gunceller — kayitlarin kendisi silinmez.
"""
from typing import Sequence, Union

from alembic import op


revision: str = 'c3d4e5f6a7b8'
down_revision: Union[str, None] = 'b2c3d4e5f6a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# Doğrulanmış yeni AI sonuçları
LYDHO_SUMMARY = (
    "Lydia Holding A.Ş., 04.05.2026 tarihli Olağan Genel Kurul'unda 2025 yılı için "
    "nakit veya bedelsiz hiçbir kar payı dağıtılmaması kararını onayladı; hisse başına "
    "brüt temettü 0,00 TL olarak gerçekleşti. SPK'ya göre dönem karı 385,9 milyon TL "
    "olarak raporlanmış olsa da yasal kayıtlarda dönem zararı -200,5 milyon TL'dir; bu "
    "çelişkili tablo, şirketin temettü dağıtmama gerekçesini güçlendirmektedir. Temettü "
    "beklentisiyle pozisyon almış yatırımcılar için bu karar doğrudan olumsuz bir sürpriz "
    "niteliği taşımaktadır. Yasal kayıtlardaki zarar, şirketin operasyonel ve finansal "
    "sağlığına ilişkin soru işaretleri doğurmakta; orta vadede temettü politikasının ne "
    "zaman normalleşeceği belirsizliğini korumaktadır. Holding yapısındaki bu tablo, "
    "yatırımcıların bağlı ortaklıkların performansını ve konsolide kar kalitesini daha "
    "yakından takip etmesi gerektiğine işaret etmektedir."
)

ALARK_SUMMARY = (
    "Bu bildirim, Borsa İstanbul BISTECH Pay Piyasası Alım Satım Sistemi tarafından "
    "yayımlanan teknik bir ex-temettü duyurusudur. ALARK için pay başına brüt temettü "
    "3,185 TL ve teorik fiyat 92,465 TL olarak açıklanmıştır. Söz konusu temettü miktarı "
    "şirket tarafından daha önce ilan edilmiş ve piyasa tarafından fiyatlanmıştır; bu "
    "duyuru yalnızca ex-temettü gününe ilişkin teknik fiyat bildirimidir. Hisse fiyatına "
    "ek pozitif bir etki beklenmemektedir."
)


def upgrade() -> None:
    # ── LYDHO 6424999 → 3.8 Olumsuz ──
    op.execute(f"""
        UPDATE kap_all_disclosures
        SET ai_sentiment = 'Olumsuz',
            ai_impact_score = 3.8,
            ai_summary = $${LYDHO_SUMMARY}$$,
            body = $${LYDHO_SUMMARY}$$
        WHERE company_code = 'LYDHO'
          AND (kap_url LIKE '%6424999%' OR kap_url LIKE '%1600571%')
    """)

    op.execute(f"""
        UPDATE telegram_news
        SET ai_score = 3.8,
            ai_summary = $${LYDHO_SUMMARY}$$
        WHERE ticker = 'LYDHO' AND kap_notification_id = '6424999'
    """)

    # ── ALARK / EGGUB / KFEIN 6423117 → 5.1 Nötr ──
    op.execute(f"""
        UPDATE kap_all_disclosures
        SET ai_sentiment = 'Nötr',
            ai_impact_score = 5.1,
            ai_summary = $${ALARK_SUMMARY}$$,
            body = $${ALARK_SUMMARY}$$
        WHERE company_code IN ('ALARK', 'EGGUB', 'KFEIN')
          AND (kap_url LIKE '%6423117%' OR kap_url LIKE '%1600207%')
    """)

    op.execute(f"""
        UPDATE telegram_news
        SET ai_score = 5.1,
            ai_summary = $${ALARK_SUMMARY}$$
        WHERE ticker IN ('ALARK', 'EGGUB', 'KFEIN')
          AND kap_notification_id = '6423117'
    """)


def downgrade() -> None:
    # Bu migration veri duzeltmesi — geri alma anlamli degil.
    # Eski yanlis puanlar zaten kaybedildi.
    pass
