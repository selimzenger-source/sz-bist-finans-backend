"""SPK Ihrac Verileri — Ilk Halka Arz Verileri scraper.

Kaynak API: https://ws.spk.gov.tr/BorclanmaAraclari/api/IlkHalkaArzVerileri?yil={yil}
Web sayfasi: https://spk.gov.tr/ihrac-verileri/ilk-halka-arz-verileri

Bu API halka arzi tamamlanmis ve borsada islem gormeye baslayan
halka arzlarin detayli listesini JSON olarak dondurur.

Dondurulen alanlar:
  - borsaKodu: Ticker (UCAYM, NETCD, AKHAN)
  - sirketUnvani: Sirket adi
  - halkaArzFiyatiTl: Halka arz fiyati
  - borsadaIslemGormeTarihi: Islem baslangic tarihi (ISO format)
  - ilkIslemGorduguPazar: Pazar (Ana Pazar, Yildiz Pazar)
  - halkaArzaAracilikEdenKurum: Araci kurum
  - halkaArzOrani: Halka arz orani (%)
  - halkaArzSekli: Sermaye artirimi / ortak satisi
  - satisaSunulanToplamTutarPiyasaDegeriBinTl: Halka arz buyuklugu (bin TL)

Amac:
  1. awaiting_trading statusundaki IPO'larin islem tarihi tespit edildigi anda
     trading_start alanini set etmek.
  2. Yeni halka arz tamamlandiginda detay bilgilerini guncellemek
     (fiyat, pazar, araci kurum, buyukluk).

Her 2 saatte bir calisir (scheduler.py job #10).
"""

import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# SPK web servis API endpoint'i
SPK_API_URL = "https://ws.spk.gov.tr/BorclanmaAraclari/api/IlkHalkaArzVerileri"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "tr-TR,tr;q=0.9",
}


class SPKIhracScraper:
    """SPK ihrac verileri — REST API ile halka arz islem verileri."""

    def __init__(self):
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers=HEADERS,
            follow_redirects=True,
            verify=False,  # SPK SSL sertifika sorunu
        )

    async def close(self):
        await self.client.aclose()

    async def fetch_trading_dates(self, year: int | None = None) -> list[dict]:
        """SPK API'den halka arz islem tarihlerini ve detaylarini ceker.

        Args:
            year: Yil (varsayilan: mevcut yil)

        Returns:
            [{ticker, company_name, trading_start_date, ipo_price,
              market_segment, lead_broker, offering_size_tl, ...}, ...]
        """
        if year is None:
            year = date.today().year

        results = []

        try:
            resp = await self.client.get(
                SPK_API_URL,
                params={"yil": year},
            )

            if resp.status_code != 200:
                logger.warning("SPK ihrac API yaniti: %d", resp.status_code)
                return results

            data = resp.json()
            if not isinstance(data, list):
                logger.warning("SPK ihrac API: Beklenmeyen format — list degil")
                return results

            for item in data:
                parsed = self._parse_item(item)
                if parsed:
                    results.append(parsed)

            logger.info("SPK ihrac API: %d halka arz islem verisi (%d)", len(results), year)

        except Exception as e:
            logger.error("SPK ihrac API hatasi: %s", e)

        return results

    async def fetch_all_years(self, years: list[int] | None = None) -> list[dict]:
        """Birden fazla yilin verilerini ceker.

        Args:
            years: Yil listesi (varsayilan: mevcut yil + onceki yil)

        Returns:
            Tum yillarin birlesmis listesi
        """
        if years is None:
            current_year = date.today().year
            years = [current_year, current_year - 1]

        all_results = []
        for year in years:
            results = await self.fetch_trading_dates(year)
            all_results.extend(results)

        return all_results

    def _parse_item(self, item: dict) -> dict | None:
        """API JSON objesini standart formata donusturur."""
        if not isinstance(item, dict):
            return None

        ticker = item.get("borsaKodu", "").strip()
        company_name = item.get("sirketUnvani", "").strip()

        if not ticker or not company_name:
            return None

        # Islem tarihi parse — ISO format: "2026-01-22T00:00:00"
        trading_date = self._parse_iso_date(item.get("borsadaIslemGormeTarihi"))

        # Fiyat
        ipo_price = None
        raw_price = item.get("halkaArzFiyatiTl")
        if raw_price is not None:
            try:
                ipo_price = Decimal(str(raw_price))
            except Exception:
                pass

        # Halka arz buyuklugu (bin TL → TL)
        offering_size_tl = None
        raw_size = item.get("satisaSunulanToplamTutarPiyasaDegeriBinTl")
        if raw_size is not None:
            try:
                offering_size_tl = Decimal(str(raw_size)) * 1000  # bin TL → TL
            except Exception:
                pass

        # Araci kurum — tirnak isareti ve newline temizle
        lead_broker = item.get("halkaArzaAracilikEdenKurum", "")
        if lead_broker:
            lead_broker = lead_broker.strip().strip('"').replace("\n", ", ")

        # Halka arz orani
        public_float_pct = None
        raw_pct = item.get("halkaArzOrani")
        if raw_pct is not None:
            try:
                public_float_pct = Decimal(str(raw_pct))
            except Exception:
                pass

        return {
            "source": "spk_ihrac",
            "ticker": ticker,
            "company_name": company_name,
            "trading_start_date": trading_date,
            "ipo_price": ipo_price,
            "market_segment": item.get("ilkIslemGorduguPazar", ""),
            "lead_broker": lead_broker,
            "offering_size_tl": offering_size_tl,
            "public_float_pct": public_float_pct,
            "ipo_method": item.get("halkaArzSekli", ""),
            "period": item.get("donem", ""),
        }

    def _parse_iso_date(self, date_str: str | None) -> date | None:
        """ISO tarih formatini parse eder.

        Desteklenen formatlar:
        - 2026-01-22T00:00:00
        - 2026-01-22
        """
        if not date_str:
            return None

        try:
            # ISO format: "2026-01-22T00:00:00"
            return datetime.fromisoformat(date_str).date()
        except (ValueError, TypeError):
            pass

        try:
            # Sadece tarih: "2026-01-22"
            return date.fromisoformat(date_str[:10])
        except (ValueError, TypeError):
            pass

        return None
