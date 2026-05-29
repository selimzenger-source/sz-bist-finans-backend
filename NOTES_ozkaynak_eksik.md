# Özkaynak (total_equity) Eksik — Düzeltilecek (AI batch sonrası)

Tarih: 2026-05-29 tespiti. Kaynak: D:\bilanco\*.xlsx (İş Yatırım, indirme ~26 May 2026)

## Tespit
Bilanço Excel'lerinde **aktif tarafı (TOPLAM VARLIKLAR) dolu ama özkaynak tarafı
(ÖZSERMAYE TOPLAMI / Özkaynaklar) BOŞ** olan hisseler. Bu bir PARSE hatası DEĞİL —
İş Yatırım'ın o dönemdeki snapshot'ında pasif/özkaynak tarafı henüz dolmamış.
Aynı Excel'den re-parse düzeltmez. Çözüm: taze İş Yatırım çekimi VEYA KAP XBRL fallback.

## Liste (her biri tek dönem)
- ADESE  → 202503 (2025-Q1)
- AKGRT  → 202509 (2025-Q3)   [ayrıca 25Q4/26Q1 kolonları Excel'de tamamen boş]
- IMASM  → 202512 (2025-Q4)
- LRSHO  → 202503 (2025-Q1)
- SELVA  → 202506 (2025-Q2)

## Yapılacak (AI analizi bitince)
1. Bu 5 hisseyi İş Yatırım'dan TAZE indir (D:\bilanco güncelle).
2. Özkaynak hâlâ boşsa → KAP XBRL'den özkaynak çek (bilanco_kap_scraper ifrs-full_Equity).
3. company_financials.total_equity upsert (sadece bu dönemler).
4. Frontend Derin Analiz: balance-sheet metriklerinde null dönem 0 yerine "veri yok"
   olarak gösterilebilir (opsiyonel UX iyileştirme).

## Not
- 31 banka/faktoring (AKBNK, GARAN, HALKB, VAKBN, YKBNK, ISCTR, QNBTR, TSKB, KLNMA,
  SKBNK, ALBRK, ICBCT, *FA faktoring vb.) genel taramada "satır yok" verdi çünkü
  banka bilanço formatı farklı; sektör-aware parser bunları ayrı işliyor — sorun değil.
