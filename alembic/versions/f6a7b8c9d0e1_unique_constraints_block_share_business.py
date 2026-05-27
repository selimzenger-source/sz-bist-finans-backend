"""block_trades / share_type_conversions / business_deals UNIQUE constraint

Revision ID: f6a7b8c9d0e1
Revises: e5f6a7b8c9d0
Create Date: 2026-05-27

SORUN: 3 tabloda race condition'da duplicate kayıt riski vardı (kod
tarafında dedup vardı ama DB seviyesinde garanti yok).

ÇÖZÜM: (kap_url, ticker) üzerinde UNIQUE constraint. NULL kap_url'li
kayıtlar etkilenmez (Postgres NULL ≠ NULL semantiği — concurrent ekleme
sırasında garanti).

NOT: share_type_conversions için (kap_url, ticker, investor_name) — aynı
KAP'ta aynı ticker birden fazla yatırımcıya devir olabilir.
"""
from typing import Sequence, Union

from alembic import op


revision: str = 'f6a7b8c9d0e1'
down_revision: Union[str, None] = 'e5f6a7b8c9d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # block_trades
    try:
        op.create_unique_constraint(
            "uq_block_trade_kap_ticker", "block_trades", ["kap_url", "ticker"],
        )
    except Exception:
        pass

    # share_type_conversions — kap+ticker+investor (multi-investor support)
    try:
        op.create_unique_constraint(
            "uq_share_type_conv_kap_ticker_investor",
            "share_type_conversions",
            ["kap_url", "ticker", "investor_name"],
        )
    except Exception:
        pass

    # business_deals
    try:
        op.create_unique_constraint(
            "uq_business_deal_kap_ticker", "business_deals", ["kap_url", "ticker"],
        )
    except Exception:
        pass


def downgrade() -> None:
    op.drop_constraint("uq_block_trade_kap_ticker", "block_trades", type_="unique")
    op.drop_constraint("uq_share_type_conv_kap_ticker_investor", "share_type_conversions", type_="unique")
    op.drop_constraint("uq_business_deal_kap_ticker", "business_deals", type_="unique")
