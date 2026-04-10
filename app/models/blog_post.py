"""Blog Post Modeli — AI ile uretilen finans egitim icerikleri.

Admin panelden AI ile blog yazisi uretilir, web sitesinde statik olarak yayinlanir.
AdSense onayı icin zengin, benzersiz ve kaliteli icerik saglar.
"""

from datetime import datetime

from sqlalchemy import Boolean, String, Text, DateTime, Index
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.database import Base


class BlogPost(Base):
    """AI ile uretilen blog yazilari."""

    __tablename__ = "blog_posts"

    id: Mapped[int] = mapped_column(primary_key=True)

    # Icerik
    slug: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, comment="URL slug")
    title: Mapped[str] = mapped_column(Text, nullable=False, comment="Blog basligi")
    content: Mapped[str] = mapped_column(Text, nullable=False, comment="HTML icerik (h2/h3/p/strong/ul)")
    meta_description: Mapped[str | None] = mapped_column(String(300), comment="SEO meta aciklama (150-160 karakter)")

    # Gorseller
    cover_image_url: Mapped[str | None] = mapped_column(Text, comment="Kapak gorseli URL")

    # Kategori & Yazar
    category: Mapped[str] = mapped_column(
        String(50), default="borsa_rehberi",
        comment="halka_arz, kap, tavan_taban, viop, spk, borsa_rehberi, teknoloji, temel_analiz"
    )
    author_name: Mapped[str] = mapped_column(String(100), default="Borsa Cebimde", comment="Yazar adi")

    # Durum
    is_published: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false", comment="Yayinda mi")
    ai_generated: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true", comment="AI ile uretildi mi")

    # Zaman damgasi
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), comment="Yayin tarihi")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index("idx_blog_slug", "slug", unique=True),
        Index("idx_blog_published", "is_published"),
        Index("idx_blog_category", "category"),
    )
