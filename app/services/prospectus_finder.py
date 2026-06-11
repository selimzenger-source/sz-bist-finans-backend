"""Nihai (SPK onaylı) izahname bulucu.

SORUN: halkarz.com ve aracı kurumlar başvuru aşamasındaki TASLAK izahnameyi
linkler; sistem de onu analiz eder. SPK onayı sonrası yayınlanan NİHAİ
izahname (güncel finansallar + kesin fiyat) farklı bir dosyadır ve çoğu
zaman şirketin kendi sitesine + KAP'a yüklenir (yasal zorunluluk).

ÇÖZÜM: SPK onayı almış, izahname'si hâlâ "taslak" görünen IPO'lar için
periyodik tarama:
  1. Şirketin kendi domain'i (mevcut taslak URL'inden çıkarılır) —
     ana sayfa + yatırımcı ilişkileri/halka arz alt sayfaları
  2. halkarz.com şirket sayfası (final çıkınca linki güncelliyorlar)

"Taslak" İÇERMEYEN izahname PDF linki bulunursa:
  - prospectus_url güncellenir
  - eski analiz silinir → analyzer otomatik yeniden çalışır
  - tweet DUPLICATE OLMAZ: IPO.prospectus_tweeted flag'i korur
    (ilk analiz tweetlendiyse yeniden tweet atılmaz, sadece DB güncellenir)
"""

import asyncio
import logging
import re
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "tr-TR,tr;q=0.9",
}

HALKARZ_BASE = "https://halkarz.com"

# Alt sayfa takibi için anahtar kelimeler (yatırımcı ilişkileri vb.)
_SUBPAGE_HINTS = ("yatirimci", "yatırımcı", "halka-arz", "halka arz", "izahname", "kap", "investor")


def _norm_text(s: str | None) -> str:
    return (s or "").strip().lower()


def _is_izahname_candidate(text: str, href: str) -> bool:
    """Link izahname adayı mı?

    NOT: 'taslak' kelimesi RED SEBEBİ DEĞİL — şirketler onaylı dosyaya bile
    'Taslak İzahname-revize' diyebiliyor (Beta Enerji vakası). Gerçek ayrım
    _pick_final_candidate'ta TARİH ile yapılır (onay tarihine yakın = nihai).
    """
    t = _norm_text(text)
    h = _norm_text(href)
    if "izahname" not in t and "izahname" not in h and "izah" not in h:
        return False
    # PDF dosyası veya KAP dosya indirme linki olmalı
    return h.endswith(".pdf") or "kap.org.tr" in h


def _extract_url_date(href: str):
    """URL'den yükleme tarihi çıkarır (WordPress /YYYY/MM/ veya YYYY-MM-DD).

    Returns: date veya None
    """
    from datetime import date as _date
    h = href or ""
    # WordPress upload path: /2026/06/
    m = re.search(r"/(20\d{2})/(\d{1,2})/", h)
    if m:
        try:
            return _date(int(m.group(1)), int(m.group(2)), 1)
        except ValueError:
            pass
    # Acik tarih: 2026-06-05 / 2026_06_05
    m = re.search(r"(20\d{2})[-_.](\d{1,2})[-_.](\d{1,2})", h)
    if m:
        try:
            return _date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass
    return None


def _pick_final_candidate(candidates: list[str], current_url: str | None, approval_date):
    """Adaylar içinden NİHAİ izahnameyi seçer.

    Kural:
      - KAP dosya linki → doğrudan kabul (KAP'a sadece onaylı belge yüklenir)
      - Tarihli aday → SPK onayından en fazla 1 ay öncesine kadar yeniyse kabul,
        en yenisi kazanır ('taslak' etiketi olsa bile — şirketler onaylı
        dosyaya 'Taslak İzahname-revize' diyebiliyor, tarih esas alınır;
        2024 tarihli eski başvuru nüshaları böylece elenir — Beta vakası)
      - Tarihsiz aday → sadece url'inde 'onayl' (onaylı) geçiyorsa kabul
      - Mevcut URL ile aynıysa atla
    """
    from datetime import timedelta

    best = None
    best_date = None
    for cand in candidates:
        if not cand or cand == (current_url or ""):
            continue
        cl = _norm_text(cand)
        if "kap.org.tr" in cl:
            return cand  # KAP linki = en güvenilir, direkt al
        d = _extract_url_date(cand)
        if d is not None:
            if approval_date is not None and d < approval_date - timedelta(days=31):
                continue  # onaydan cok eski → başvuru nüshası, atla
            if approval_date is None:
                # Onay tarihi bilinmiyorsa: mevcut URL'den daha yeni olmalı
                cur_d = _extract_url_date(current_url or "")
                if cur_d is not None and d <= cur_d:
                    continue
            # Ayni tarihte: 'taslak' icermeyen aday tercih edilir
            if best_date is None or d > best_date or (
                d == best_date and "taslak" in _norm_text(best or "") and "taslak" not in cl
            ):
                best, best_date = cand, d
        elif "onayl" in cl:
            if best is None:
                best = cand
    return best


async def _fetch_links(client: httpx.AsyncClient, url: str) -> list[tuple[str, str]]:
    """Sayfadaki tüm (text, absolute_href) çiftlerini döndürür."""
    try:
        resp = await client.get(url)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, "lxml")
        out = []
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if not href or href.startswith(("javascript:", "mailto:", "#")):
                continue
            out.append((a.get_text(strip=True), urljoin(url, href)))
        return out
    except Exception as e:
        logger.debug("prospectus_finder fetch hatasi (%s): %s", url, e)
        return []


async def _scan_company_site(client: httpx.AsyncClient, domain_url: str) -> list[str]:
    """Şirket sitesinde izahname adaylarını toplar (ana sayfa + max 4 alt sayfa)."""
    found: list[str] = []
    links = await _fetch_links(client, domain_url)
    for text, href in links:
        if _is_izahname_candidate(text, href) and href not in found:
            found.append(href)

    # Alt sayfalar: yatırımcı ilişkileri / halka arz görünümlü linkler
    sub_pages = []
    base_host = urlparse(domain_url).netloc
    for text, href in links:
        blob = _norm_text(text) + " " + _norm_text(href)
        if any(k in blob for k in _SUBPAGE_HINTS):
            if urlparse(href).netloc == base_host and href not in sub_pages:
                sub_pages.append(href)
        if len(sub_pages) >= 4:
            break

    for sp in sub_pages:
        sub_links = await _fetch_links(client, sp)
        for text, href in sub_links:
            if _is_izahname_candidate(text, href) and href not in found:
                found.append(href)
    return found


async def _scan_halkarz(client: httpx.AsyncClient, company_name: str) -> list[str]:
    """halkarz.com şirket sayfasındaki izahname adaylarını toplar."""
    found: list[str] = []
    try:
        # Arama ile şirket sayfası slug'ını bul
        q = company_name.split(" A.")[0].strip()  # "Beta Enerji ve Teknoloji"
        resp = await client.get(f"{HALKARZ_BASE}/", params={"s": q})
        if resp.status_code != 200:
            return found
        slugs = re.findall(r'https://halkarz\.com/([a-z0-9-]+)/', resp.text)
        # İlk 2 kelime eşleşen slug'ı seç
        first_words = "-".join(
            re.sub(r"[^a-z0-9 ]", "", q.lower().replace("ı", "i").replace("ş", "s")
                   .replace("ç", "c").replace("ğ", "g").replace("ü", "u").replace("ö", "o")
                   ).split()[:2]
        )
        page_url = None
        for slug in slugs:
            if slug.startswith(first_words[:12]):
                page_url = f"{HALKARZ_BASE}/{slug}/"
                break
        if not page_url:
            return found

        links = await _fetch_links(client, page_url)
        for text, href in links:
            if _is_izahname_candidate(text, href) and href not in found:
                found.append(href)
    except Exception as e:
        logger.debug("prospectus_finder halkarz hatasi (%s): %s", company_name, e)
    return found


async def find_final_prospectus_url(
    company_name: str, current_url: str | None, approval_date=None,
) -> str | None:
    """Nihai izahname URL'i arar.

    Tüm adaylar toplanır, _pick_final_candidate ile tarih/KAP kuralına göre
    seçilir. Bulunamazsa veya mevcutla aynıysa None.
    """
    async with httpx.AsyncClient(
        timeout=25.0, headers=HEADERS, follow_redirects=True, verify=False,
    ) as client:
        candidates: list[str] = []

        # 1) Şirketin kendi sitesi (taslak URL'inden domain çıkar)
        if current_url:
            parsed = urlparse(current_url)
            if parsed.netloc and "halkarz" not in parsed.netloc and "kap.org" not in parsed.netloc:
                for c in await _scan_company_site(client, f"{parsed.scheme}://{parsed.netloc}/"):
                    if c not in candidates:
                        candidates.append(c)

        # 2) halkarz şirket sayfası
        for c in await _scan_halkarz(client, company_name):
            if c not in candidates:
                candidates.append(c)

        return _pick_final_candidate(candidates, current_url, approval_date)


async def check_final_prospectuses():
    """Scheduler girişi — taslak izahname'li aktif IPO'lar için nihai arama.

    Filtre: SPK onayı son 45 günde + status aktif + (URL'de 'taslak' geçiyor
    VEYA analiz yok). Final bulununca URL güncellenir, analiz sıfırlanır ve
    analyzer yeniden tetiklenir (tweet, prospectus_tweeted flag'i ile korunur).
    """
    from datetime import date, timedelta
    from sqlalchemy import select, and_, or_
    from app.database import async_session
    from app.models.ipo import IPO

    try:
        today = date.today()
        async with async_session() as db:
            stmt = select(IPO).where(
                and_(
                    IPO.status.in_(["newly_approved", "in_distribution", "awaiting_trading"]),
                    IPO.spk_approval_date.is_not(None),
                    IPO.spk_approval_date >= today - timedelta(days=45),
                    or_(
                        IPO.prospectus_url.ilike("%taslak%"),
                        IPO.prospectus_analysis.is_(None),
                    ),
                )
            ).limit(5)
            ipos = (await db.execute(stmt)).scalars().all()

            if not ipos:
                return

            for ipo in ipos:
                try:
                    new_url = await find_final_prospectus_url(
                        ipo.company_name or "", ipo.prospectus_url,
                        approval_date=ipo.spk_approval_date,
                    )
                    if not new_url:
                        logger.info(
                            "[IZAHNAME-FINDER] %s: nihai izahname henuz yok (mevcut: %s)",
                            ipo.ticker or ipo.company_name,
                            (ipo.prospectus_url or "-")[:80],
                        )
                        continue

                    old_url = ipo.prospectus_url
                    ipo.prospectus_url = new_url
                    # Eski (taslak) analiz varsa sifirla → yeniden analiz edilsin
                    had_analysis = bool(ipo.prospectus_analysis)
                    ipo.prospectus_analysis = None
                    ipo.prospectus_analyzed_at = None
                    await db.commit()
                    logger.info(
                        "[IZAHNAME-FINDER] %s: NIHAI izahname bulundu: %s (eski: %s)",
                        ipo.ticker or ipo.company_name, new_url, (old_url or "-")[:80],
                    )

                    try:
                        from app.services.admin_telegram import send_admin_message
                        await send_admin_message(
                            f"📄 Nihai izahname bulundu: {ipo.ticker or ipo.company_name}\n"
                            f"Yeni: {new_url}\n"
                            f"{'Eski taslak analizi silindi, ' if had_analysis else ''}"
                            f"AI analizi yeniden başlatılıyor."
                        )
                    except Exception:
                        pass

                    # Analizi arka planda tetikle (tweet dedup: prospectus_tweeted)
                    from app.services.prospectus_analyzer import analyze_prospectus
                    asyncio.create_task(analyze_prospectus(ipo.id, new_url))

                except Exception as ipo_err:
                    logger.warning(
                        "[IZAHNAME-FINDER] %s hata: %s",
                        ipo.ticker or ipo.company_name, ipo_err,
                    )
    except Exception as e:
        logger.error("[IZAHNAME-FINDER] Genel hata: %s", e)
