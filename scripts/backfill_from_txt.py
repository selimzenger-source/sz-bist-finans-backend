"""
Tek-seferlik backfill: Kullanicinin verdigi 3 txt dosyasinu parse edip DB'ye yazar.

Dosyalar:
  C:/Users/PC/Desktop/toptan satis.txt              -> block_trades
  C:/Users/PC/Desktop/borsada islem tipe donusum.txt -> share_type_conversions
  C:/Users/PC/Desktop/cezalli hisseler.txt          -> cautious_stocks (tarihli)
"""

import asyncio
import os
import re
from datetime import date, datetime
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text, select


DB_URL = "postgresql+asyncpg://bist_finans_user:rjBmvvququhrjAl429scWixOFtOeieXS@dpg-d651ohsr85hc73anuv60-a.frankfurt-postgres.render.com/bist_finans"

TR_MONTHS = {
    "Oca": 1, "Şub": 2, "Mar": 3, "Nis": 4, "May": 5, "Haz": 6,
    "Tem": 7, "Ağu": 8, "Eyl": 9, "Eki": 10, "Kas": 11, "Ara": 12,
}


def parse_dd_mm_yyyy(s: str) -> date | None:
    """'30.04.2026' -> date(2026,4,30)"""
    try:
        d, m, y = s.strip().split(".")
        return date(int(y), int(m), int(d))
    except Exception:
        return None


def parse_tr_short_date(s: str, year: int = 2026) -> date | None:
    """'06 Mar' -> date(2026,3,6). Year defaults to 2026."""
    parts = s.strip().split()
    if len(parts) < 2:
        return None
    try:
        d = int(parts[0])
        m = TR_MONTHS.get(parts[1][:3])
        if not m:
            return None
        return date(year, m, d)
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# 1) TOPTAN SATIS PARSER
# ─────────────────────────────────────────────────────────────────────────────

def parse_toptan_satis(text_content: str) -> list[dict]:
    """Format:
    TICKER
    COMPANY
    DD.MM.YYYY
    İşlem Tipi
    Satis veya Alis
    Aracı Kurum
    BROKER NAME
    Alıcılar veya Satıcılar
    PARTIES (1 veya cok satir)
    Lot Miktarı
    NUMBER Lot
    Maliyet Fiyatı
    NUMBER TL

    Bos satir kayitlari ayirir.
    """
    blocks = re.split(r"\n\s*\n", text_content.strip())
    records = []
    for block in blocks:
        lines = [l.rstrip() for l in block.split("\n") if l.strip()]
        if len(lines) < 5:
            continue
        try:
            ticker = lines[0].strip()
            if not re.match(r"^[A-Z]{3,6}$", ticker):
                continue
            company = lines[1].strip()
            tx_date = parse_dd_mm_yyyy(lines[2])

            kap_url = None
            for ln in lines:
                m = re.search(r"https?://(?:www\.)?kap\.org\.tr/[^\s]+", ln)
                if m:
                    kap_url = m.group(0).rstrip(".,;)")
                    break

            rec = {
                "ticker": ticker,
                "company_name": company,
                "transaction_date": tx_date,
                "transaction_type": None,
                "broker": None,
                "counterparties": None,
                "lot_amount": None,
                "cost_price": None,
                "kap_url": kap_url,
            }

            # Label-based extraction
            i = 3
            while i < len(lines):
                line = lines[i].strip()
                next_line = lines[i + 1].strip() if i + 1 < len(lines) else ""

                if line == "İşlem Tipi":
                    val = next_line.lower()
                    if "alış" in val or "alis" in val:
                        rec["transaction_type"] = "alis"
                    elif "satış" in val or "satis" in val:
                        rec["transaction_type"] = "satis"
                    i += 2
                elif line in ("Aracı Kurum", "Aracı Kurumu"):
                    rec["broker"] = next_line[:200]
                    i += 2
                elif line in ("Alıcılar", "Alıcı", "Satıcılar", "Satıcı"):
                    # Counterparties — collect until next label
                    parties_lines = []
                    j = i + 1
                    while j < len(lines):
                        if lines[j].strip() in ("Lot Miktarı", "Maliyet Fiyatı", "Aracı Kurum"):
                            break
                        parties_lines.append(lines[j].strip())
                        j += 1
                    rec["counterparties"] = " ".join(parties_lines)[:500]
                    i = j
                elif line == "Lot Miktarı":
                    val = re.sub(r"[^\d]", "", next_line)
                    if val:
                        rec["lot_amount"] = int(val)
                    i += 2
                elif line == "Maliyet Fiyatı":
                    val = next_line.replace(",", ".").replace(" TL", "").strip()
                    val = re.sub(r"[^\d.]", "", val)
                    if val:
                        try:
                            rec["cost_price"] = float(val)
                        except Exception:
                            pass
                    i += 2
                else:
                    i += 1

            if rec["ticker"] and rec["transaction_date"]:
                records.append(rec)
        except Exception as e:
            print(f"  parse hata: {e}, block: {lines[:3]}")
            continue
    return records


# ─────────────────────────────────────────────────────────────────────────────
# 2) TIP DONUSUM PARSER
# ─────────────────────────────────────────────────────────────────────────────

def parse_tip_donusum(text_content: str) -> list[dict]:
    """Format:
    TICKER
    COMPANY
    DD.MM.YYYY
    Yatırımcı
    NAME
    Dönüştürülen Lot
    NUMBER Lot
    """
    blocks = re.split(r"\n\s*\n", text_content.strip())
    records = []
    for block in blocks:
        lines = [l.strip() for l in block.split("\n") if l.strip()]
        if len(lines) < 4:
            continue
        try:
            ticker = lines[0]
            if not re.match(r"^[A-Z]{3,6}$", ticker):
                continue
            kap_url = None
            for ln in lines:
                m = re.search(r"https?://(?:www\.)?kap\.org\.tr/[^\s]+", ln)
                if m:
                    kap_url = m.group(0).rstrip(".,;)")
                    break

            rec = {
                "ticker": ticker,
                "company_name": lines[1] if len(lines) > 1 else None,
                "conversion_date": None,
                "investor_name": None,
                "lot_amount": None,
                "kap_url": kap_url,
            }
            for i, line in enumerate(lines):
                if re.match(r"\d{2}\.\d{2}\.\d{4}", line):
                    rec["conversion_date"] = parse_dd_mm_yyyy(line)
                elif line == "Yatırımcı" and i + 1 < len(lines):
                    rec["investor_name"] = lines[i + 1][:200]
                elif line == "Dönüştürülen Lot" and i + 1 < len(lines):
                    val = re.sub(r"[^\d]", "", lines[i + 1])
                    if val:
                        rec["lot_amount"] = int(val)
            if rec["ticker"] and rec["conversion_date"]:
                records.append(rec)
        except Exception as e:
            print(f"  tip parse hata: {e}")
    return records


# ─────────────────────────────────────────────────────────────────────────────
# 3) CEZALI/TEDBIRLI PARSER
# ─────────────────────────────────────────────────────────────────────────────

def parse_cezali(text_content: str) -> list[dict]:
    """Format:
    TICKER
    COMPANY
    PRICE TL
    +/-PCT%
    DD AAA → DD AAA  (start_date → end_date)
    TAG1
    TAG2
    ...
    """
    blocks = re.split(r"\n\s*\n", text_content.strip())
    records = []
    for block in blocks:
        lines = [l.strip() for l in block.split("\n") if l.strip()]
        if len(lines) < 4:
            continue
        try:
            ticker = lines[0]
            if not re.match(r"^[A-Z]{3,6}$", ticker):
                continue
            kap_url = None
            for ln in lines:
                m = re.search(r"https?://(?:www\.)?kap\.org\.tr/[^\s]+", ln)
                if m:
                    kap_url = m.group(0).rstrip(".,;)")
                    break

            rec = {
                "ticker": ticker,
                "company_name": lines[1] if len(lines) > 1 else None,
                "last_price": None,
                "pct_change": None,
                "start_date": None,
                "end_date": None,
                "tags": [],
                "kap_url": kap_url,
            }
            for line in lines[2:]:
                # Fiyat: "21,44 ₺" / "21,44 TL"
                m = re.match(r"^([\d.,]+)\s*[₺TL]", line)
                if m:
                    try:
                        rec["last_price"] = float(m.group(1).replace(".", "").replace(",", "."))
                    except: pass
                    continue
                # Yüzde: "+9.95%" / "0.18%"
                m = re.match(r"^([+\-]?[\d.,]+)\s*%", line)
                if m:
                    try:
                        rec["pct_change"] = float(m.group(1).replace(",", "."))
                    except: pass
                    continue
                # Tarih aralığı: "06 Mar → 05 May"
                if "→" in line or "->" in line:
                    parts = re.split(r"→|->", line)
                    if len(parts) == 2:
                        rec["start_date"] = parse_tr_short_date(parts[0])
                        rec["end_date"] = parse_tr_short_date(parts[1])
                    continue
                # Etiket: KRD, AÇS, BRT, EMR, PEM, VEY, TEK
                if line.upper() in ("KRD", "AÇS", "ACS", "BRT", "EMR", "PEM", "VEY", "TEK"):
                    rec["tags"].append(line.upper().replace("Ç", "C"))
            if rec["ticker"]:
                # Tags listeyi string'e çevir
                rec["tags_str"] = ",".join(rec["tags"]) if rec["tags"] else ""
                records.append(rec)
        except Exception as e:
            print(f"  cezali parse hata: {e}")
    return records


# ─────────────────────────────────────────────────────────────────────────────
# 4) PAY ALIM SATIM PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_pct(s: str) -> float | None:
    """'%3.71' / '+0.37%' / '-1.33%' / '0.00%' -> 3.71 / 0.37 / -1.33 / 0.0"""
    s = s.strip().replace("%", "").replace(",", ".").replace(" ", "")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _parse_price_range(s: str) -> tuple[float | None, float | None]:
    """'15,60 - 15,63 TL' / '31,54 TL' / '15 - 15 TL' -> (low, high)"""
    s = s.replace(" TL", "").replace("TL", "").strip()
    parts = re.split(r"\s*-\s*", s)
    nums = []
    for p in parts:
        p = p.replace(".", "").replace(",", ".").strip()
        try:
            nums.append(float(p))
        except Exception:
            pass
    if len(nums) >= 2:
        return min(nums), max(nums)
    if len(nums) == 1:
        return nums[0], nums[0]
    return None, None


def parse_pay_alim_satim(text_content: str) -> list[dict]:
    """Format (KAP URL bos satirla ayri blok olarak gelebilir):
    TICKER
    COMPANY
    DD.MM.YYYY
    Alıcı veya Satıcı
    PARTY_NAME
    [Görev]
    [ROLE_TEXT]
    [Fiyat]
    [PRICE_RANGE TL]
    Nominal
    NUMBER Lot
    Oy Hakkı
    %X.XX
    [+/-]Y.YY%
    Pay Oranı
    %X.XX
    [+/-]Y.YY%

    https://www.kap.org.tr/tr/Bildirim/XXX  <- ayri blok
    """
    blocks = re.split(r"\n\s*\n", text_content.strip())
    records = []
    for block in blocks:
        lines = [l.rstrip() for l in block.split("\n") if l.strip()]
        if not lines:
            continue
        # Sadece KAP URL'den olusan blok - onceki kayda kap_url ekle
        if len(lines) == 1 and re.match(r"^https?://(?:www\.)?kap\.org\.tr/", lines[0]):
            if records:
                records[-1]["kap_url"] = lines[0].rstrip(".,;)")
            continue
        if len(lines) < 6:
            continue
        try:
            ticker = lines[0].strip()
            if not re.match(r"^[A-Z]{3,6}$", ticker):
                continue
            company = lines[1].strip()
            tx_date = parse_dd_mm_yyyy(lines[2])
            if not tx_date:
                continue
            role = lines[3].strip()
            if role not in ("Alıcı", "Satıcı"):
                continue
            party_name = lines[4].strip()
            tx_type = "alis" if role == "Alıcı" else "satis"

            # KAP URL — bloğun herhangi bir yerinde olabilir
            kap_url = None
            for ln in lines:
                m = re.search(r"https?://(?:www\.)?kap\.org\.tr/[^\s]+", ln)
                if m:
                    kap_url = m.group(0).rstrip(".,;)")
                    break

            rec = {
                "ticker": ticker,
                "company_name": company,
                "transaction_date": tx_date,
                "transaction_type": tx_type,
                "party_name": party_name[:200],
                "party_role": None,
                "price_low": None,
                "price_high": None,
                "nominal_lot": None,
                "oy_hakki_pct": None,
                "oy_hakki_change_pct": None,
                "pay_orani_pct": None,
                "pay_orani_change_pct": None,
                "kap_url": kap_url,
                "raw_excerpt": "\n".join(lines)[:1000],
            }

            # Sirayla isleyin (label-based)
            i = 5
            while i < len(lines):
                line = lines[i].strip()
                next_line = lines[i + 1].strip() if i + 1 < len(lines) else ""

                if line == "Görev":
                    # Sonraki satir gorev metni — bazen multi-line
                    role_text = next_line
                    rec["party_role"] = role_text[:200]
                    i += 2
                elif line == "Fiyat":
                    low, high = _parse_price_range(next_line)
                    rec["price_low"] = low
                    rec["price_high"] = high
                    i += 2
                elif line == "Nominal":
                    val = re.sub(r"[^\d]", "", next_line)
                    if val:
                        rec["nominal_lot"] = int(val)
                    i += 2
                elif line == "Oy Hakkı":
                    # Sonraki satir: "%3.71", sonraki: "-0.44%"
                    rec["oy_hakki_pct"] = _parse_pct(next_line)
                    if i + 2 < len(lines):
                        change_line = lines[i + 2].strip()
                        if "%" in change_line:
                            rec["oy_hakki_change_pct"] = _parse_pct(change_line)
                            i += 3
                        else:
                            i += 2
                    else:
                        i += 2
                elif line == "Pay Oranı":
                    rec["pay_orani_pct"] = _parse_pct(next_line)
                    if i + 2 < len(lines):
                        change_line = lines[i + 2].strip()
                        if "%" in change_line:
                            rec["pay_orani_change_pct"] = _parse_pct(change_line)
                            i += 3
                        else:
                            i += 2
                    else:
                        i += 2
                else:
                    i += 1

            records.append(rec)
        except Exception as e:
            print(f"  pay parse hata: {e}, ilk satir: {lines[0] if lines else ''}")
            continue
    return records


# ─────────────────────────────────────────────────────────────────────────────
# DB UPSERT FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────


async def upsert_pay_alim_satim(session, records: list[dict]) -> int:
    """share_transaction_details — (ticker, transaction_date, party_name) bazinda dedup."""
    upserted = 0
    for r in records:
        check = await session.execute(text("""
            SELECT id FROM share_transaction_details
            WHERE ticker=:tk AND transaction_date=:dt AND COALESCE(party_name,'')=:pn
        """), {"tk": r["ticker"], "dt": r["transaction_date"], "pn": r["party_name"] or ""})
        existing_id = check.scalar()

        if existing_id:
            await session.execute(text("""
                UPDATE share_transaction_details
                SET company_name=:cn, transaction_type=:tt, party_role=:pr,
                    price_low=:pl, price_high=:ph, nominal_lot=:lot,
                    oy_hakki_pct=:oy, oy_hakki_change_pct=:oyc,
                    pay_orani_pct=:po, pay_orani_change_pct=:poc,
                    kap_url=COALESCE(:kap, kap_url),
                    source='manual_txt_import', raw_excerpt=:raw
                WHERE id=:id
            """), {
                "id": existing_id, "cn": r["company_name"], "tt": r["transaction_type"],
                "pr": r["party_role"], "pl": r["price_low"], "ph": r["price_high"],
                "lot": r["nominal_lot"], "oy": r["oy_hakki_pct"], "oyc": r["oy_hakki_change_pct"],
                "po": r["pay_orani_pct"], "poc": r["pay_orani_change_pct"],
                "kap": r.get("kap_url"), "raw": r["raw_excerpt"],
            })
        else:
            await session.execute(text("""
                INSERT INTO share_transaction_details(ticker, company_name, transaction_date,
                    transaction_type, party_name, party_role, price_low, price_high,
                    nominal_lot, oy_hakki_pct, oy_hakki_change_pct,
                    pay_orani_pct, pay_orani_change_pct,
                    kap_url, source, raw_excerpt, created_at)
                VALUES(:tk, :cn, :dt, :tt, :pn, :pr, :pl, :ph, :lot,
                       :oy, :oyc, :po, :poc, :kap, 'manual_txt_import', :raw, NOW())
            """), {
                "tk": r["ticker"], "cn": r["company_name"], "dt": r["transaction_date"],
                "tt": r["transaction_type"], "pn": r["party_name"], "pr": r["party_role"],
                "pl": r["price_low"], "ph": r["price_high"], "lot": r["nominal_lot"],
                "oy": r["oy_hakki_pct"], "oyc": r["oy_hakki_change_pct"],
                "po": r["pay_orani_pct"], "poc": r["pay_orani_change_pct"],
                "kap": r.get("kap_url"), "raw": r["raw_excerpt"],
            })
        upserted += 1
    return upserted




async def upsert_block_trades(session, records: list[dict]) -> int:
    """block_trades — (ticker, transaction_date, lot_amount) bazinda dedup."""
    upserted = 0
    for r in records:
        # Mevcut kayit var mi (ayni ticker + tarih + lot)
        check = await session.execute(text("""
            SELECT id FROM block_trades
            WHERE ticker = :tk AND transaction_date = :dt
              AND COALESCE(lot_amount, 0) = COALESCE(:lot, 0)
        """), {"tk": r["ticker"], "dt": r["transaction_date"], "lot": r["lot_amount"] or 0})
        existing_id = check.scalar()

        if existing_id:
            await session.execute(text("""
                UPDATE block_trades
                SET company_name=:cn, transaction_type=:tt, broker=:br,
                    counterparties=:cp, lot_amount=:lot, cost_price=:price,
                    kap_url=COALESCE(:kap, kap_url), source='manual_txt_import'
                WHERE id=:id
            """), {
                "id": existing_id, "cn": r["company_name"], "tt": r["transaction_type"],
                "br": r["broker"], "cp": r["counterparties"],
                "lot": r["lot_amount"], "price": r["cost_price"], "kap": r.get("kap_url"),
            })
        else:
            await session.execute(text("""
                INSERT INTO block_trades(ticker, company_name, transaction_date,
                    transaction_type, broker, counterparties, lot_amount, cost_price,
                    kap_url, source, created_at)
                VALUES(:tk, :cn, :dt, :tt, :br, :cp, :lot, :price, :kap, 'manual_txt_import', NOW())
            """), {
                "tk": r["ticker"], "cn": r["company_name"], "dt": r["transaction_date"],
                "tt": r["transaction_type"], "br": r["broker"], "cp": r["counterparties"],
                "lot": r["lot_amount"], "price": r["cost_price"], "kap": r.get("kap_url"),
            })
        upserted += 1
    return upserted


async def upsert_tip_donusum(session, records: list[dict]) -> int:
    """share_type_conversions: column 'transaction_date' (not conversion_date), 'converted_lot' (not lot_amount)."""
    upserted = 0
    for r in records:
        check = await session.execute(text("""
            SELECT id FROM share_type_conversions
            WHERE ticker=:tk AND transaction_date=:dt AND COALESCE(investor_name,'')=:inv
        """), {"tk": r["ticker"], "dt": r["conversion_date"], "inv": r["investor_name"] or ""})
        if check.scalar():
            continue
        await session.execute(text("""
            INSERT INTO share_type_conversions(ticker, company_name, transaction_date,
                investor_name, converted_lot, kap_url, source, created_at)
            VALUES(:tk, :cn, :dt, :inv, :lot, :kap, 'manual_txt_import', NOW())
        """), {
            "tk": r["ticker"], "cn": r["company_name"], "dt": r["conversion_date"],
            "inv": r["investor_name"], "lot": r["lot_amount"], "kap": r.get("kap_url"),
        })
        upserted += 1
    return upserted


async def replace_cautious(session, records: list[dict]) -> int:
    """Tedbirli — replace all (eski hatali default tarih kayitlarini sil)."""
    await session.execute(text("DELETE FROM cautious_stocks"))
    inserted = 0
    for r in records:
        await session.execute(text("""
            INSERT INTO cautious_stocks(ticker, company_name, last_price, pct_change,
                start_date, end_date, tags, is_active, kap_url,
                source, created_at, updated_at)
            VALUES(:tk, :cn, :price, :pct, :sd, :ed, :tags, TRUE, :kap,
                   'manual_txt_import', NOW(), NOW())
        """), {
            "tk": r["ticker"], "cn": r["company_name"],
            "price": r["last_price"], "pct": r["pct_change"],
            "sd": r["start_date"], "ed": r["end_date"],
            "tags": r["tags_str"], "kap": r.get("kap_url"),
        })
        inserted += 1
    return inserted


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    base = r"C:/Users/PC/Desktop"
    files = {
        "toptan": f"{base}/toptan satış.txt",
        "tip": f"{base}/borsada işlem tipe dönüşüm.txt",
        "cezali": f"{base}/cezallı hisseler.txt",
        "pay": f"{base}/pay alım satım.txt",
    }

    eng = create_async_engine(DB_URL)

    # 1) TOPTAN
    print("=" * 60)
    print("1) TOPTAN SATIS")
    with open(files["toptan"], encoding="utf-8") as f:
        toptan_records = parse_toptan_satis(f.read())
    print(f"  Parsed: {len(toptan_records)} kayit")
    if toptan_records:
        print(f"  Ornek: {toptan_records[0]['ticker']} {toptan_records[0]['transaction_date']} lot={toptan_records[0]['lot_amount']} price={toptan_records[0]['cost_price']}")
    async with AsyncSession(eng) as s:
        n = await upsert_block_trades(s, toptan_records)
        await s.commit()
        print(f"  Upserted: {n}")

    # 2) TIP DONUSUM
    print("=" * 60)
    print("2) TIP DONUSUM")
    with open(files["tip"], encoding="utf-8") as f:
        tip_records = parse_tip_donusum(f.read())
    print(f"  Parsed: {len(tip_records)} kayit")
    if tip_records:
        print(f"  Ornek: {tip_records[0]['ticker']} {tip_records[0]['conversion_date']} inv={tip_records[0]['investor_name']} lot={tip_records[0]['lot_amount']}")
    async with AsyncSession(eng) as s:
        n = await upsert_tip_donusum(s, tip_records)
        await s.commit()
        print(f"  Inserted (yeni): {n}")

    # 3) CEZALI
    print("=" * 60)
    print("3) CEZALI/TEDBIRLI")
    with open(files["cezali"], encoding="utf-8") as f:
        cezali_records = parse_cezali(f.read())
    print(f"  Parsed: {len(cezali_records)} kayit")
    if cezali_records:
        print(f"  Ornek: {cezali_records[0]['ticker']} {cezali_records[0]['start_date']} -> {cezali_records[0]['end_date']} tags={cezali_records[0]['tags_str']}")
    async with AsyncSession(eng) as s:
        n = await replace_cautious(s, cezali_records)
        await s.commit()
        print(f"  Replaced: {n}")

    # 4) PAY ALIM SATIM
    print("=" * 60)
    print("4) PAY ALIM SATIM")
    if os.path.exists(files["pay"]):
        with open(files["pay"], encoding="utf-8") as f:
            pay_records = parse_pay_alim_satim(f.read())
        print(f"  Parsed: {len(pay_records)} kayit")
        if pay_records:
            ex = pay_records[0]
            print(f"  Ornek: {ex['ticker']} {ex['transaction_date']} {ex['transaction_type']} "
                  f"party={ex['party_name'][:25]} oy_hakki={ex['oy_hakki_pct']} "
                  f"pay={ex['pay_orani_pct']} kap={ex['kap_url']}")
        async with AsyncSession(eng) as s:
            n = await upsert_pay_alim_satim(s, pay_records)
            await s.commit()
            print(f"  Upserted: {n}")

    await eng.dispose()
    print("=" * 60)
    print("BACKFILL TAMAMLANDI")


if __name__ == "__main__":
    asyncio.run(main())
