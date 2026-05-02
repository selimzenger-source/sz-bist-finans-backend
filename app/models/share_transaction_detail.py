"""Pay Alım Satım Detay — Yapilandirilmis bildirim verisi.

Kaynak: KAP "Pay Alım Satım Bildirimi" disclosure body'si
+ Ucretsizderinlikbot'tan manuel import.

KAP bildirimlerinde su yapilar bulunur:
- Hisse, Sirket, Tarih
- Islem Tipi (Alici/Satici)
- Taraf adi (kisi veya sirket)
- Gorev (varsa: Yonetim Kurulu Baskani vs)
- Fiyat (tek deger veya araliik 15.60 - 15.63 TL)
- Nominal Lot
- Oy Hakki % (mevcut + degisim)
- Pay Orani % (mevcut + degisim)

Frontend Pay Alim Satim sayfasinda bu yapilandirilmis veri gosterilir.
"""

from datetime import date, datetime
from sqlalchemy import String, Float, Integer, Date, DateTime, Index, Text, BigInteger
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class ShareTransactionDetail(Base):
    """Pay alim satim — zenginlestirilmis kayit."""

    __tablename__ = "share_transaction_details"

    id: Mapped[int] = mapped_column(primary_key=True)

    # ─── Hisse + Sirket ───
    ticker: Mapped[str] = mapped_column(String(10), nullable=False, index=True)
    company_name: Mapped[str | None] = mapped_column(String(255))

    # ─── Islem ───
    transaction_date: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    transaction_type: Mapped[str] = mapped_column(
        String(10), nullable=False,
        comment="alici | satici"
    )

    # ─── Taraf ───
    party_name: Mapped[str] = mapped_column(
        String(255), nullable=False,
        comment="Alan/satan kisi veya sirket"
    )
    party_role: Mapped[str | None] = mapped_column(
        String(200),
        comment="Görev (orn: Yönetim Kurulu Başkanı). Bos olabilir."
    )

    # ─── Fiyat (tek veya aralik) ───
    price_low: Mapped[float | None] = mapped_column(Float)
    price_high: Mapped[float | None] = mapped_column(
        Float,
        comment="Aralik ust degeri. Tek fiyat ise null."
    )

    # ─── Miktar + Oranlar ───
    nominal_lot: Mapped[int | None] = mapped_column(BigInteger)
    oy_hakki_pct: Mapped[float | None] = mapped_column(Float)
    oy_hakki_change_pct: Mapped[float | None] = mapped_column(Float)
    pay_orani_pct: Mapped[float | None] = mapped_column(Float)
    pay_orani_change_pct: Mapped[float | None] = mapped_column(Float)

    # ─── Referans ───
    kap_disclosure_id: Mapped[int | None] = mapped_column(
        BigInteger,
        comment="kap_all_disclosures.id referansi (varsa)"
    )
    kap_url: Mapped[str | None] = mapped_column(Text)
    source: Mapped[str] = mapped_column(
        String(20), nullable=False, default="kap_ai_parse",
        comment="kap_ai_parse | manual_import | telegram_bot"
    )

    # ─── Meta ───
    raw_excerpt: Mapped[str | None] = mapped_column(
        Text,
        comment="Kaynak metnin ham parcacik (debug icin)"
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    __table_args__ = (
        Index("idx_stx_ticker_date", "ticker", "transaction_date"),
        Index("idx_stx_date", "transaction_date"),
    )
