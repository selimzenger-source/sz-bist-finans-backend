"""Stock Market Segment — Hisse senedinin BIST pazar bilgisi.

Kaynak: https://borsaistanbul.com/datum/hisse_endeks_ds.csv
Gunde 1 kez senkronize edilir, KAP bildirim filtreleme icin kullanilir.
"""

from datetime import datetime
from sqlalchemy import String, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class StockMarket(Base):
    """Hisse pazar segmenti.

    market_segment degerleri (BIST resmi adlandirma):
      - 'ana_pazar'       -> BIST Ana Pazar
      - 'yildiz_pazar'    -> BIST Yildiz Pazar
      - 'alt_pazar'       -> BIST Alt Pazar
      - 'yakin_izleme'    -> Yakin Izleme Pazari
      - 'kollektif_yat'   -> Kollektif Yatirim Urunleri / GIP Aday Pazari
      - 'diger'           -> Diger
    """

    __tablename__ = "stock_markets"

    id: Mapped[int] = mapped_column(primary_key=True)
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, unique=True, index=True)
    company_name: Mapped[str | None] = mapped_column(String(255))
    market_segment: Mapped[str] = mapped_column(
        String(32), nullable=False, default="diger",
        comment="ana_pazar | yildiz_pazar | alt_pazar | yakin_izleme | kollektif_yat | diger"
    )
    # Endeks uyelikleri — virgulle ayrilmis liste (XU100, XU030 vs.)
    indexes: Mapped[str | None] = mapped_column(String(500), comment="Uye oldugu endeksler (CSV)")
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        Index("idx_stock_market_segment", "market_segment"),
    )
