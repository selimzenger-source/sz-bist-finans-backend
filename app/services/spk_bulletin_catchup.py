"""SPK Bülten Catch-Up — kendi kendini iyileştiren tamamlayıcı.

KÖK SORUN (01.07.2026): check_spk_bulletins tek uzun görevde çalışır ve bülten
no'yu ERKEN kaydeder (idempotent). IPO'lar oluşup commit edildikten SONRA görev
herhangi bir sebeple ölürse (Render restart/deploy, exception, AI down, Twitter
down), analiz+tweet+push adımları KALICI kaybolur — sonraki tur "bu bülten zaten
işlendi" deyip atlar, retry yok. Sonuç: kullanıcı sadece IPO'yu görür, SPK bülten
bildirimi/tweeti hiç gelmez (bülten 2026/43 vakası).

ÇÖZÜM: Bu job her 15 dk'da bir, son 24 saatte IPO'su oluşmuş ama SPK bülten
push flag'i (`spk_bulten_push_{no}`) EKSİK bültenleri bulup analiz+tweet+push'u
tamamlar. Her ihtimale karşı (restart/exception/AI/Twitter) bülten bildirimi
EN GEÇ 15 dk içinde kesin gider. AI o an hâlâ down ise flag yazılmaz → sonraki
turda tekrar denenir (AI düzelince otomatik tamamlanır).
"""
from __future__ import annotations

import logging

from sqlalchemy import text as sa_text

logger = logging.getLogger(__name__)


async def _complete_bulletin(bno_str: str, db) -> dict:
    """Tek bir bültenin analiz + tweet + push'unu tamamlar (reprocess çekirdeği)."""
    import asyncio as _aio
    from app.scrapers.spk_bulletin_scraper import SPKBulletinScraper
    from app.services.twitter_service import (
        tweet_spk_bulletin_analysis, _generate_bulletin_analysis_sync,
    )
    from app.models.pending_tweet import PendingTweet
    from app.services.notification import NotificationService

    try:
        _y, _n = bno_str.split("/")
        year, no = int(_y), int(_n)
    except Exception:
        return {"ok": False, "reason": "bad_bulletin_no"}

    sc = SPKBulletinScraper()
    full_text = ""
    try:
        lst = await sc.fetch_bulletin_list(year)
        target = next((b for b in lst if b.get("bulletin_no") == (year, no)), None)
        if not target:
            return {"ok": False, "reason": "not_in_list"}
        _appr, full_text = await sc.process_bulletin(target["pdf_url"], (year, no))
    except Exception as e:
        logger.error("[BULTEN-CATCHUP] %s fetch/process hata: %s", bno_str, e)
        return {"ok": False, "reason": f"fetch_error:{type(e).__name__}"}
    finally:
        try:
            await sc.client.aclose()
        except Exception:
            pass

    if not full_text:
        return {"ok": False, "reason": "empty_pdf"}

    # AI analiz (thread — event loop bloklanmasın)
    ai_text = await _aio.to_thread(_generate_bulletin_analysis_sync, full_text, bno_str)
    if not ai_text:
        # AI hâlâ down → flag YAZMA, sonraki turda tekrar denenir (self-heal)
        logger.warning("[BULTEN-CATCHUP] %s AI hâlâ başarısız — sonraki turda tekrar denenecek", bno_str)
        return {"ok": False, "reason": "ai_failed_will_retry"}

    # Özet çıkar (konu başlıkları)
    topics = [l.strip()[:80] for l in ai_text.split("\n")
              if l.strip() and any(l.strip().startswith(e) for e in
                                    ["🚀", "💰", "💵", "📊", "📈", "⚖️", "🏛", "🔔", "📋", "🏢", "🔍", "⚠️", "🎯", "🚫"])]
    summary = " | ".join(topics[:4]) if topics else (
        next((l.strip()[:200] for l in ai_text.split("\n") if len(l.strip()) > 15), ""))

    # ★ ÖNCE PUSH (kullanıcı kuralı: aslolan bildirim), SONRA tweet
    push_sent = 0
    already_push = (await db.execute(sa_text(
        "SELECT 1 FROM pending_tweets WHERE source = :s LIMIT 1"
    ), {"s": f"spk_bulten_push_{bno_str}"})).scalar_one_or_none()
    if not already_push:
        try:
            notif = NotificationService(db)
            push_sent = await notif.notify_spk_bulletin(bno_str, summary)
            db.add(PendingTweet(source=f"spk_bulten_push_{bno_str}", status="sent",
                                text=f"SPK {bno_str} push flag (catchup)"))
            await db.commit()
        except Exception as e:
            await db.rollback()
            logger.error("[BULTEN-CATCHUP] %s push hata: %s", bno_str, e)

    # Tweet (analiz) — precomputed ai_text ile
    tw_ok = False
    already_an = (await db.execute(sa_text(
        "SELECT 1 FROM pending_tweets WHERE source = :s LIMIT 1"
    ), {"s": f"spk_bulten_analiz_{bno_str}"})).scalar_one_or_none()
    if not already_an:
        try:
            tw_ok = await _aio.to_thread(tweet_spk_bulletin_analysis, full_text, bno_str, ai_text)
            db.add(PendingTweet(source=f"spk_bulten_analiz_{bno_str}", status="sent",
                                text=f"SPK {bno_str} analiz tweet flag (catchup)"))
            await db.commit()
        except Exception as e:
            await db.rollback()
            logger.error("[BULTEN-CATCHUP] %s tweet hata: %s", bno_str, e)

    logger.info("[BULTEN-CATCHUP] %s TAMAMLANDI — push=%d tweet=%s", bno_str, push_sent, bool(tw_ok))
    return {"ok": True, "push_sent": push_sent, "tweet_ok": bool(tw_ok)}


async def catchup_incomplete_bulletins() -> dict:
    """Son 24 saatte IPO'su oluşmuş ama push flag'i EKSİK bültenleri tamamlar."""
    from app.database import async_session

    completed = []
    try:
        async with async_session() as db:
            rows = (await db.execute(sa_text(
                "SELECT DISTINCT spk_bulletin_no FROM ipos "
                "WHERE spk_bulletin_no IS NOT NULL "
                "AND created_at >= NOW() - INTERVAL '24 hours'"
            ))).fetchall()
            recent = [r[0] for r in rows if r[0]]

            # ★ IPO'SUZ BÜLTEN BOŞLUĞU (2026/44 vakası, 08.07.2026): görev IPO
            # commit'inden ÖNCE ölürse (OOM restart) veya bülten hiç IPO onayı
            # içermiyorsa yukarıdaki sorgu bülteni GÖREMEZ → analiz+tweet+push
            # kalıcı kaybolur. Çözüm: scraper_state'teki son işlenen bülten
            # no'sunu da kontrol listesine ekle (son 48 saatte işaretlenmişse).
            try:
                st = (await db.execute(sa_text(
                    "SELECT value FROM scraper_state "
                    "WHERE key = 'spk_last_bulletin_no' "
                    "AND updated_at >= NOW() - INTERVAL '48 hours'"
                ))).scalar_one_or_none()
                if st and st not in recent:
                    recent.append(st)
                    logger.info(
                        "[BULTEN-CATCHUP] scraper_state'ten eklendi (IPO'suz olabilir): %s", st,
                    )
            except Exception as _st_err:
                logger.warning("[BULTEN-CATCHUP] scraper_state okunamadi: %s", _st_err)

            for bno in recent:
                push_exists = (await db.execute(sa_text(
                    "SELECT 1 FROM pending_tweets WHERE source = :s LIMIT 1"
                ), {"s": f"spk_bulten_push_{bno}"})).scalar_one_or_none()
                if push_exists:
                    continue  # tamam
                logger.warning(
                    "[BULTEN-CATCHUP] %s EKSİK (IPO var, push flag yok) — tamamlanıyor", bno,
                )
                res = await _complete_bulletin(bno, db)
                completed.append({"bulletin": bno, **res})

                # Admin'e bilgi
                try:
                    from app.services.admin_telegram import send_admin_message
                    if res.get("ok"):
                        await send_admin_message(
                            f"🔧 <b>Bülten Catch-Up</b>\n{bno} yarım kalmıştı, tamamlandı "
                            f"(push={res.get('push_sent', 0)}, tweet={'✅' if res.get('tweet_ok') else '❌'})."
                        )
                except Exception:
                    pass

        return {"ok": True, "checked": len(recent), "completed": completed}
    except Exception as e:
        logger.error("[BULTEN-CATCHUP] genel hata: %s", e)
        return {"ok": False, "error": str(e)[:200]}
