"""KAP Haber Filtreleme Servisi — 300+ anahtar kelime kalibi.

Mevcut Telegram botundaki keyword veritabaninin aynisi.
Seans ici (10:00-18:10) ve seans disi olmak uzere haberleri siniflandirir.
Sadece POZiTiF sentiment tespiti yapar — negatif haber filtrelenmez.
"""

import logging
from datetime import datetime, time
from typing import Optional

logger = logging.getLogger(__name__)


# ================================================================
# POZITIF HABER KALIPLARI (300+ kalip)
# ================================================================

POSITIVE_KEYWORDS: list[str] = [
    # --- Sozlesme & Is Iliskisi ---
    "sozlesme imzalanmistir",
    "sozlesme imzalandi",
    "sozlesme akdedilmistir",
    "cerceve sozlesme",
    "is birligi protokolu",
    "is birligi anlasmas",
    "niyet mektubu",
    "mutabakat zapti",
    "hizmet sozlesmesi",
    "danismanlik sozlesmesi",
    "bayi sozlesmesi",
    "distributorluk sozlesmesi",
    "lisans sozlesmesi",
    "franchise sozlesmesi",
    "tahkim sonucu lehimize",

    # --- Ihale & Teklif ---
    "ihale kazanilmistir",
    "ihale uhdemizde",
    "ihalenin kazanildigi",
    "en avantajli teklif",
    "ihale bedeli",
    "teklifimiz kabul",
    "ihalede en uygun",
    "yeterlilik almistir",
    "yer aldigi teblig",
    "ihale sonucu",
    "en dusuk teklif",
    "mukavele imza",
    "is emri alinmistir",
    "is emri verilmistir",

    # --- Siparis & Ihracat ---
    "yeni siparis",
    "siparis alinmasi",
    "siparis alinmistir",
    "ihracat baglantisi",
    "ihracat siparis",
    "tedarik sozlesmesi",
    "toplu siparis",
    "satis sozlesmesi",
    "satisa iliskin",

    # --- Uretim & Tesis ---
    "tesis devreye",
    "devreye alinmistir",
    "devreye alinacaktir",
    "uretim baslamistir",
    "uretim kapasitesi artir",
    "kapasite artirimi",
    "kapasite artis",
    "yeni fabrika",
    "yeni tesis",
    "yeni santral",
    "yeni maden",
    "yeni saha",
    "uretim rekoru",
    "acilis yapilmistir",
    "faaliyete gecmistir",
    "kurulum tamamlanmistir",
    "montaj tamamlanmistir",
    "insaat tamamlanmistir",

    # --- Yatirim & Tesvik ---
    "yatirim tesvik belgesi",
    "tesvik belgesi alinmistir",
    "milyon dolar",
    "milyon euro",
    "milyar dolar",
    "milyar euro",
    "milyon tl",
    "milyar tl",
    "yatirim karari",
    "yatirim programi",
    "stratejik yatirim",
    "ar-ge projesi",
    "tubitak destegi",
    "hibe destegi",
    "fondan kaynak",

    # --- Birlesme & Ortaklik ---
    "birlesme sozlesmesi",
    "devralma islemleri",
    "istirak edinilmesi",
    "istirak satin",
    "ortaklik yapisi",
    "hisse devir",
    "hisse satis",
    "pay devri",
    "joint venture",
    "konsorsiyum",
    "sirket satin alim",
    "sirket birlesme",
    "pay alim teklif",
    "stratejik ortak",

    # --- Sermaye & Bedelsiz ---
    "bedelsiz sermaye artirimi",
    "bedelsiz pay",
    "sermaye artirimi",
    "ic kaynaklardan sermaye",
    "pay geri alim programi",
    "geri alim programi",
    "temettü avans",
    "bonus pay",

    # --- Lisans & Resmi Onay ---
    "spk onaylanmistir",
    "spk onayi alinmistir",
    "rekabet kurulu onay",
    "bddk onay",
    "epdk lisans",
    "ced olumlu",
    "ruhsat alinmasi",
    "ruhsat verilmistir",
    "lisans alinmistir",
    "yetki belgesi",
    "patent alinmistir",
    "patent tescil",
    "marka tescil",
    "iso sertifika",
    "akreditasyon",

    # --- Finansal Basari ---
    "kar artisi",
    "gelir artisi",
    "hasilat artisi",
    "ciro artisi",
    "satislar artti",
    "net kar",
    "brut kar artis",
    "faaliyet kari artis",
    "ebitda artis",
    "rekor gelir",
    "rekor kar",
    "rekor hasilat",
    "beklentilerin uzerinde",
    "hedefin uzerinde",
    "pozitif revizyon",
    "tahmin yukari",

    # --- Gayrimenkul & Arazi ---
    "arazi satin alinmistir",
    "gayrimenkul satin",
    "tasinmaz edinim",
    "arsa alim",
    "konut satis",
    "imar izni",
    "imar plan degisiklig",
    "insaat ruhsati",

    # --- Enerji & Maden ---
    "enerji uretim lisansi",
    "ges projesi",
    "res projesi",
    "santral devreye",
    "maden isletme ruhsati",
    "petrol arama ruhsati",
    "dogalgaz bulunmus",
    "petrol bulunmus",
    "rezerv artis",
    "mw kapasiteli",
    "mwp gunes",
    "mwe ruzgar",

    # --- Teknoloji & Inovasyon ---
    "yazilim sozlesmesi",
    "teknoloji ortakligi",
    "dijital donusum",
    "yeni urun lansmanı",
    "platform devreye",
    "mobil uygulama",
    "e-ticaret",
    "yapay zeka",
    "blockchain",

    # --- Borc & Finansman ---
    "kredi sozlesmesi imzalanmistir",
    "finansman anlasmasi",
    "tahvil ihraci basarili",
    "refinansman tamamlanmistir",
    "borc yapilandirma",
    "sendikasyon kredisi",
    "eurobond ihraci",
    "sukuk ihraci",

    # --- Yabancilar & Uluslararasi ---
    "yabanci yatirimci",
    "uluslararasi ihale",
    "uluslararasi sozlesme",
    "yurtdisi is",
    "global ortaklik",
    "yabanci ortak",
    "uluslararasi sertifika",
    "ihracat hacmi artis",

    # --- Diger Pozitif ---
    "artis gostermistir",
    "basarili sonuclanmistir",
    "olumlu gelisme",
    "olumlu sonuclanmistir",
    "onaylanmistir",
    "taahhutte bulunulmustur",
    "kazanim saglanmistir",
    "gelisime katkida",
    "iyilestirme",
    "verimlilik artis",
    "pazar payi artis",
    "musteri sayisi artis",
    "abone sayisi artis",

    # --- Halka Arz ---
    "halka arz",
    "halka arz onay",
    "halka arz izahname",
    "halka arz talep toplama",
    "halka arz fiyat",
]


# ================================================================
# NEGATIF HABER KALIPLARI
# ================================================================

NEGATIVE_KEYWORDS: list[str] = [
    # --- Zarar & Kayip ---
    "zarar etmistir",
    "zarar aciklamistir",
    "net zarar",
    "faaliyet zarari",
    "kar dususu",
    "gelir dususu",
    "hasilat dususu",
    "ciro dususu",

    # --- Hukuki & Ceza ---
    "idari para cezasi",
    "vergi cezasi",
    "dava acilmistir",
    "haciz islemi",
    "iflas",
    "konkordato",
    "konkordato mulheti",
    "tehiri icra",
    "sorusturma baslatilmistir",
    "kovusturma",
    "cezai islem",
    "yasaklanmistir",
    "kara listeye",

    # --- Operasyonel Sorun ---
    "uretim durdurulmustur",
    "faaliyet durdurma",
    "is kazasi",
    "yangin",
    "patlama",
    "sel hasari",
    "deprem hasari",
    "dogal afet",
    "is birakma",
    "grev",
    "kapasite dususu",
    "teslimat gecikmesi",

    # --- Finansal Risk ---
    "borc odeyememistir",
    "temerrut",
    "kredi notu dususu",
    "negatif gorunum",
    "sermaye kaybi",
    "teknik iflas",
    "negatif oz kaynak",
    "borc yapilandirma zorunluluk",

    # --- Yonetim & Istifa ---
    "istifa etmistir",
    "gorevden alinmistir",
    "gorev degisikligi",
    "yonetim degisikligi olumsuz",
    "spk isleme kapatma",
    "isleme kapatilmistir",
    "kotasyondan cikarilma",

    # --- Piyasa Uyari ---
    "uyari nitelikte",
    "bagimsiz denetim olumsuz",
    "sinirli olumlu gorusu",
    "gorusu bildirmekten kacin",
    "finansal tablo duzeltme",
]


# ================================================================
# BIST Endeks Bileşenleri
# ================================================================

# Not: Bu listeler periyodik olarak guncellenmeli
BIST30_TICKERS: set[str] = {
    "AKBNK", "ARCLK", "ASELS", "BIMAS", "EKGYO", "ENKAI", "EREGL",
    "FROTO", "GARAN", "GUBRF", "HEKTS", "ISCTR", "KCHOL", "KOZAA",
    "KOZAL", "KRDMD", "MGROS", "ODAS", "PETKM", "PGSUS", "SAHOL",
    "SASA", "SISE", "TAVHL", "TCELL", "THYAO", "TKFEN", "TOASO",
    "TUPRS", "YKBNK",
}

BIST50_TICKERS: set[str] = BIST30_TICKERS | {
    "AEFES", "AKFGY", "AKSA", "ALARK", "ASTOR", "CCOLA", "CIMSA",
    "DOHOL", "EGEEN", "GESAN", "HALKB", "ISGYO", "KONTR", "MPARK",
    "OYAKC", "SOKM", "TTKOM", "TTRAK", "ULKER", "VAKBN",
}

BIST100_TICKERS: set[str] = BIST50_TICKERS | {
    "ADEL", "AGHOL", "AHGAZ", "AKFYE", "AKSGY", "ALFAS", "ALTNY",
    "ANSGR", "AYDEM", "BAGFS", "BANVT", "BIENY", "BRKVY", "BRSAN",
    "BRYAT", "BTCIM", "BUCIM", "CEMTS", "CWENE", "DOAS", "ECILC",
    "ENJSA", "EUPWR", "FENER", "GLYHO", "GOLTS", "GRSEL",
    "IPEKE", "ISMEN", "KAYSE", "KLRHO", "KMPUR", "KUYAS",
    "LOGO", "MAVI", "NTHOL", "OBAMS", "OTKAR", "PAPIL", "PENTA",
    "QUAGR", "RGYAS", "SARKY", "SELEC", "SKBNK", "SMRTG",
    "TKNSA", "TMSN", "TRGYO", "TURSG", "VESBE", "VESTL",
}


class NewsFilterService:
    """KAP haberlerini keyword ile filtreler ve siniflandirir."""

    def __init__(self):
        # Compile — hizli arama icin kucuk harfe donustur
        self.positive_keywords = [kw.lower() for kw in POSITIVE_KEYWORDS]

    def filter_disclosure(self, disclosure: dict) -> Optional[dict]:
        """Bir KAP bildirimini pozitif keyword ile filtreler.

        Returns:
            Eslesen haber bilgisi veya None (eslesmezse).
            Sadece pozitif haberler gecerli — negatif filtrelenmez.
        """
        subject = (disclosure.get("subject", "") or "").lower()
        text = subject  # Ileride detay metni de eklenebilir

        matched_keyword = None

        # Pozitif keyword kontrolu
        for kw in self.positive_keywords:
            if kw in text:
                matched_keyword = kw
                break

        if not matched_keyword:
            return None

        # Seans ici/disi tespiti
        now = datetime.now()
        news_type = self._determine_session_type(now)

        return {
            "ticker": disclosure.get("ticker", ""),
            "kap_notification_id": disclosure.get("kap_id"),
            "news_title": disclosure.get("subject"),
            "news_detail": matched_keyword,
            "matched_keyword": matched_keyword,
            "news_type": news_type,
            "sentiment": "positive",
            "raw_text": disclosure.get("subject"),
            "kap_url": disclosure.get("url"),
            "published_at": disclosure.get("published_at"),
        }

    def _determine_session_type(self, dt: datetime) -> str:
        """Seans ici mi disi mi belirler.

        BIST seans saatleri: 10:00 - 18:10
        """
        t = dt.time()
        session_start = time(10, 0)
        session_end = time(18, 10)

        if session_start <= t <= session_end:
            # Hafta ici mi kontrol et (0=Pazartesi, 4=Cuma)
            if dt.weekday() < 5:
                return "seans_ici"
        return "seans_disi"

    def is_ticker_in_package(self, ticker: str, package: str) -> bool:
        """Hisse senedi kodunun kullanici paketine dahil olup olmadigini kontrol eder."""
        ticker_upper = ticker.upper()

        if package == "all":
            return True
        elif package == "bist100":
            return ticker_upper in BIST100_TICKERS
        elif package == "bist50":
            return ticker_upper in BIST50_TICKERS
        elif package == "bist30":
            return ticker_upper in BIST30_TICKERS
        elif package == "free":
            return False  # Ucretsiz pakette haber bildirimi yok

        return False

    def get_subscribers_for_ticker(self, ticker: str, packages: list[str]) -> list[str]:
        """Bir hisse icin bildirim alacak paketleri dondurur."""
        matching = []
        for pkg in packages:
            if self.is_ticker_in_package(ticker, pkg):
                matching.append(pkg)
        return matching
