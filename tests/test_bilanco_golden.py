# -*- coding: utf-8 -*-
"""ALTIN-ORNEK (GOLDEN) BILANCO PARSE TESTLERI.

Amac: parser'a dokunan HERHANGI bir degisiklik, gecmiste yasanmis ve
duzeltilmis hata siniflarini geri getirirse BURADA patlasin — uretimde degil.

Fixture'lar gercek KAP bildirim govdeleri (tests/golden/*.txt, 11-12.06.2026):
  - REEDR: dipnot referans kolonu ("11,12,19") deger sanilmasi → FAVOK bug'i
  - MARKA: cari kolon 0 iken sifir atlanip kolon kaymasi → onceki donem cari sanildi
  - EKDMR: yeni halka arz, OID cache'e girememe → bos parse
  - GUBRF: ana mesaj kaybi → set-uyesi bildirimden XBRL

Beklenen degerler REEDR ve MARKA icin FINTABLES ile birebir dogrulandi.

Calistirma:
    python -m pytest tests/test_bilanco_golden.py -v
    (veya pytest yoksa)  python tests/test_bilanco_golden.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.bilanco_kap_scraper import parse_kap_finansal_rapor  # noqa: E402

GOLDEN_DIR = os.path.join(os.path.dirname(__file__), "golden")


def _load(ticker: str) -> str:
    with open(os.path.join(GOLDEN_DIR, f"{ticker}_body.txt"), encoding="utf-8") as f:
        return f.read()


def _approx(actual, expected, tol=0.01):
    """Mutlak deger >1 icin %1 tolerans; None karsilastirmasi katidir."""
    if expected is None:
        return actual is None
    if actual is None:
        return False
    if expected == 0:
        return abs(actual) < 1.0
    return abs(actual - expected) / abs(expected) <= tol


def test_reedr_favok_fintables():
    """REEDR 2026-Q1 — FAVOK Fintables ile birebir (dipnot-kolon fix kaniti)."""
    out = parse_kap_finansal_rapor(_load("REEDR"), "REEDR")
    assert out["period"] == "2026-Q1", out["period"]
    assert _approx(out["revenue"], 472_624_297), out["revenue"]
    assert _approx(out["gross_profit"], 207_073_165), out["gross_profit"]
    # Fintables: 70.493.271 — amortisman (95.6mn) dogru satirdan okunmali
    assert _approx(out["ebitda"], 70_493_271), f"FAVOK REGRESYON: {out['ebitda']}"
    assert _approx(out["net_income"], -342_703_654), out["net_income"]


def test_marka_sifir_kolon_kaymasi():
    """MARKA 2026-Q1 — cari satis 0; sifir atlanirsa onceki donem cari sanilir."""
    out = parse_kap_finansal_rapor(_load("MARKA"), "MARKA")
    assert out["period"] == "2026-Q1", out["period"]
    # Fintables birebir: revenue=0, gross=0, ebitda=-4.619.350, net=2.910.217
    assert _approx(out["revenue"], 0), f"KOLON KAYMASI REGRESYONU: revenue={out['revenue']}"
    assert _approx(out["gross_profit"], 0), out["gross_profit"]
    assert _approx(out["ebitda"], -4_619_350), out["ebitda"]
    assert _approx(out["net_income"], 2_910_217), out["net_income"]


def test_ekdmr_yeni_sirket_dolu_parse():
    """EKDMR (yeni halka arz) — cekirdek alanlar BOS OLMAMALI (bos kart bug'i)."""
    out = parse_kap_finansal_rapor(_load("EKDMR"), "EKDMR")
    assert out["period"] == "2026-Q1", out["period"]
    assert _approx(out["revenue"], 4_657_066_601), out["revenue"]
    assert _approx(out["net_income"], 110_254_907), out["net_income"]
    assert out["total_assets"] is not None and out["total_assets"] > 1e9, out["total_assets"]
    assert out["ebitda"] is not None and out["ebitda"] > 0, out["ebitda"]


def test_gubrf_set_uyesi_kaynak():
    """GUBRF — Ozkaynaklar bildirimi ekinden tam finansal rapor parse edilebilmeli."""
    out = parse_kap_finansal_rapor(_load("GUBRF"), "GUBRF")
    assert out["period"] == "2026-Q1", out["period"]
    _core = ("revenue", "net_income", "total_assets", "total_equity")
    bos = [k for k in _core if out.get(k) is None]
    assert not bos, f"GUBRF cekirdek alanlar bos: {bos}"


if __name__ == "__main__":
    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"PASS  {name}")
            except AssertionError as e:
                fails += 1
                print(f"FAIL  {name}: {e}")
    print("SONUC:", "TUM TESTLER GECTI" if fails == 0 else f"{fails} TEST PATLADI")
    sys.exit(1 if fails else 0)
