"""Admin panel route'lari — IPO CRUD + Dagitim Sonuclari + SPK Yonetimi + Kupon."""

import asyncio
import logging
import os
import re
import secrets
import string
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Optional

# ── Arka plan görev takibi (GC koruması) ──────────────────────────
# asyncio.create_task() weak reference kullanır — referans tutmazsan
# task GC tarafından silinebilir. Bu set tüm aktif görevleri tutar.
_bg_tasks: set = set()

def _fire_and_forget(coro) -> asyncio.Task:
    """Coroutine'i arka planda başlat; GC'den korumak için referans tut."""
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)
    return task

from fastapi import APIRouter, Request, Depends, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, desc, and_, or_, func as sa_func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import get_db
from app.models.ipo import IPO, IPOAllocation, IPOCeilingTrack
from app.models.spk_application import SPKApplication
from app.models.user import Coupon, ReplyTarget, AutoReply
from app.admin.auth import (
    verify_password, create_session, destroy_session,
    get_current_admin, SESSION_COOKIE_NAME,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])


def _normalize_company_name(name: str) -> str:
    """Sirket ismini normalize eder — bosluk, satir sonu, buyuk/kucuk harf farklarini giderir."""
    if not name:
        return ""
    # \n, \r, \t → bosluk, fazla bosluklari tek bosluga indir, strip, lowercase
    return re.sub(r"\s+", " ", name.strip()).lower()


def _is_company_in_ipo(spk_name: str, ipo_names_normalized: set[str]) -> bool:
    """SPK'daki sirket ismi IPO tablosundakilerden biriyle eslesiyor mu?

    SPK sitesi uzun isimleri kirpabilir, IPO tablosunda kisa isim olabilir.
    Ornekler:
      SPK:  'ata turizm isletmecilik madencilik san. ve dis tic. as'
      IPO:  'ata turizm'   (kisa isim)
      IPO:  'empa elektronik sanayi ve ticaret as'

    Cozum:
      1. Birebir eslesme
      2. IPO ismi SPK isminin basinda mi? (startswith)  — kisa isimler icin
      3. SPK ismi IPO isminin basinda mi? (startswith)  — SPK kirpmasi icin
      4. Ilk 3 anlamli kelime eslesmesi
    """
    name_norm = _normalize_company_name(spk_name)
    if not name_norm:
        return False
    # 1. Birebir eslesme
    if name_norm in ipo_names_normalized:
        return True
    for ipo_n in ipo_names_normalized:
        # 2. IPO ismi kisa olabilir: SPK ismi IPO ismiyle basliyor mu?
        if name_norm.startswith(ipo_n) or ipo_n.startswith(name_norm):
            return True
    # 3. Ilk 3 anlamli kelime eslesmesi
    skip_words = {"a.ş.", "a.s.", "aş", "as", "san.", "tic.", "ve", "ve/veya", "ltd.", "şti.", "sti."}
    spk_words = [w for w in name_norm.split() if w not in skip_words][:3]
    if len(spk_words) < 2:
        return False
    spk_key = " ".join(spk_words)
    for ipo_n in ipo_names_normalized:
        ipo_words = [w for w in ipo_n.split() if w not in skip_words][:3]
        ipo_key = " ".join(ipo_words)
        if spk_key == ipo_key:
            return True
    return False

templates = Jinja2Templates(directory="app/templates")


# -------------------------------------------------------
# Yardimci Fonksiyonlar
# -------------------------------------------------------

def parse_decimal(value: Optional[str]) -> Optional[Decimal]:
    """Form'dan gelen string'i Decimal'e cevirir."""
    if not value or value.strip() == "":
        return None
    try:
        # Turk formatindaki virgulu noktaya cevir
        cleaned = value.strip().replace(",", ".")
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None


def parse_int(value: Optional[str]) -> Optional[int]:
    """Form'dan gelen string'i int'e cevirir."""
    if not value or value.strip() == "":
        return None
    try:
        return int(value.strip())
    except (ValueError, TypeError):
        return None


def parse_date(value: Optional[str]) -> Optional[date]:
    """Form'dan gelen string'i date'e cevirir (YYYY-MM-DD)."""
    if not value or value.strip() == "":
        return None
    try:
        return date.fromisoformat(value.strip())
    except (ValueError, TypeError):
        return None


def parse_bool(value: Optional[str]) -> bool:
    """Form'dan gelen checkbox degerini bool'a cevirir."""
    return value == "on" or value == "true" or value == "1"


def _build_subscription_hours(form) -> Optional[str]:
    """Iki ayri select'ten subscription_hours string'i olusturur.

    subscription_hour_open = "09:00", subscription_hour_close = "17:00"
    → "09:00-17:00"
    """
    sub_open = form.get("subscription_hour_open", "").strip()
    sub_close = form.get("subscription_hour_close", "").strip()
    if sub_open and sub_close:
        return f"{sub_open}-{sub_close}"
    elif sub_open:
        return f"{sub_open}-17:00"
    elif sub_close:
        return f"09:00-{sub_close}"
    # Eski format uyumlulugu (text input ile girilmis olabilir)
    old_val = form.get("subscription_hours", "").strip()
    return old_val or None


# -------------------------------------------------------
# LOGIN
# -------------------------------------------------------

@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Admin giris sayfasi."""
    return templates.TemplateResponse("admin/login.html", {
        "request": request,
        "error": None,
    })


@router.post("/login")
async def login_submit(request: Request, password: str = Form(...)):
    """Admin giris islemi."""
    if verify_password(password):
        token = create_session()
        response = RedirectResponse(url="/admin/", status_code=303)

        from app.config import get_settings
        _settings = get_settings()
        response.set_cookie(
            key=SESSION_COOKIE_NAME,
            value=token,
            httponly=True,
            max_age=86400,  # 1 gun (7 gunden dusuruldu)
            samesite="strict",  # lax → strict (CSRF korumasini guclendir)
            secure=_settings.is_production,  # HTTPS-only in production
        )
        return response

    # Basarisiz giris — loglama
    import logging
    _logger = logging.getLogger(__name__)
    client_ip = request.client.host if request.client else "unknown"
    _logger.warning("Admin login basarisiz — IP: %s", client_ip)

    return templates.TemplateResponse("admin/login.html", {
        "request": request,
        "error": "Yanlis sifre!",
    })


@router.get("/logout")
async def logout(request: Request):
    """Admin cikis."""
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if token:
        destroy_session(token)
    response = RedirectResponse(url="/admin/login", status_code=303)
    response.delete_cookie(SESSION_COOKIE_NAME)
    return response


# -------------------------------------------------------
# DASHBOARD
# -------------------------------------------------------

@router.get("/", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    status: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Admin dashboard — tum IPO listesi."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    # Islem tarihine gore: belli olmayanlar + en yakin tarih en ustte, en eski en altta
    query = select(IPO).order_by(
        IPO.trading_start.is_(None).desc(),  # NULL'lar (yeni/dagitimda) en ustte
        desc(IPO.trading_start),             # sonra en yakin tarih → en eski
    )

    if status:
        query = query.where(IPO.status == status)

    result = await db.execute(query.limit(200))
    ipos = list(result.scalars().all())

    # Istatistikler
    total_result = await db.execute(select(sa_func.count(IPO.id)))
    total_count = total_result.scalar() or 0

    status_counts = {}
    for s in ["newly_approved", "in_distribution", "awaiting_trading", "trading", "archived"]:
        cnt_result = await db.execute(
            select(sa_func.count(IPO.id)).where(IPO.status == s)
        )
        status_counts[s] = cnt_result.scalar() or 0

    # SPK basvuru sayisi
    spk_result = await db.execute(
        select(sa_func.count(SPKApplication.id)).where(SPKApplication.status == "pending")
    )
    spk_count = spk_result.scalar() or 0

    return templates.TemplateResponse("admin/dashboard.html", {
        "request": request,
        "ipos": ipos,
        "total_count": total_count,
        "status_counts": status_counts,
        "spk_count": spk_count,
        "current_status": status,
    })


# -------------------------------------------------------
# IPO OLUSTUR
# -------------------------------------------------------

@router.get("/ipo/new", response_class=HTMLResponse)
async def new_ipo_form(request: Request):
    """Yeni IPO olusturma formu."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    return templates.TemplateResponse("admin/ipo_form.html", {
        "request": request,
        "ipo": None,
        "is_new": True,
        "success": None,
        "error": None,
    })


@router.post("/ipo/new")
async def create_ipo(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Yeni IPO olustur."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    form = await request.form()

    try:
        ipo = IPO(
            company_name=form.get("company_name", "").strip(),
            ticker=form.get("ticker", "").strip().upper() or None,
            logo_url=form.get("logo_url", "").strip() or None,
            status=form.get("status", "newly_approved"),
            ipo_price=parse_decimal(form.get("ipo_price")),
            total_lots=parse_int(form.get("total_lots")),
            offering_size_tl=parse_decimal(form.get("offering_size_tl")),
            capital_increase_lots=parse_int(form.get("capital_increase_lots")),
            partner_sale_lots=parse_int(form.get("partner_sale_lots")),
            subscription_start=parse_date(form.get("subscription_start")),
            subscription_end=parse_date(form.get("subscription_end")),
            subscription_hours=_build_subscription_hours(form),
            trading_start=parse_date(form.get("trading_start")),
            spk_approval_date=parse_date(form.get("spk_approval_date")),
            expected_trading_date=parse_date(form.get("expected_trading_date")),
            spk_bulletin_no=form.get("spk_bulletin_no", "").strip() or None,
            distribution_completed=parse_bool(form.get("distribution_completed")),
            distribution_method=form.get("distribution_method", "").strip() or None,
            distribution_description=form.get("distribution_description", "").strip() or None,
            participation_method=form.get("participation_method", "").strip() or None,
            participation_description=form.get("participation_description", "").strip() or None,
            public_float_pct=parse_decimal(form.get("public_float_pct")),
            discount_pct=parse_decimal(form.get("discount_pct")),
            market_segment=form.get("market_segment", "").strip() or None,
            lead_broker=form.get("lead_broker", "").strip() or None,
            estimated_lots_per_person=parse_int(form.get("estimated_lots_per_person")),
            lock_up_period_days=parse_int(form.get("lock_up_period_days")),
            price_stability_days=parse_int(form.get("price_stability_days")),
            min_application_lot=parse_int(form.get("min_application_lot")) or 1,
            company_description=form.get("company_description", "").strip() or None,
            sector=form.get("sector", "").strip() or None,
            fund_usage=form.get("fund_usage", "").strip() or None,
            revenue_current_year=parse_decimal(form.get("revenue_current_year")),
            revenue_previous_year=parse_decimal(form.get("revenue_previous_year")),
            gross_profit=parse_decimal(form.get("gross_profit")),
            kap_notification_url=form.get("kap_notification_url", "").strip() or None,
            prospectus_url=form.get("prospectus_url", "").strip() or None,
            spk_bulletin_url=form.get("spk_bulletin_url", "").strip() or None,
            allocation_announced=parse_bool(form.get("allocation_announced")),
            total_applicants=parse_int(form.get("total_applicants")),
            ceiling_tracking_active=parse_bool(form.get("ceiling_tracking_active")),
            ceiling_broken=parse_bool(form.get("ceiling_broken")),
            archived=parse_bool(form.get("archived")),
            trading_day_count=parse_int(form.get("trading_day_count")) or 0,
        )
        db.add(ipo)
        await db.flush()
        logger.info(f"Admin: Yeni IPO olusturuldu — {ipo.company_name} (ID: {ipo.id})")
        return RedirectResponse(url=f"/admin/ipo/{ipo.id}/edit?success=created", status_code=303)

    except Exception as e:
        logger.error(f"Admin: IPO olusturma hatasi — {e}")
        return templates.TemplateResponse("admin/ipo_form.html", {
            "request": request,
            "ipo": None,
            "is_new": True,
            "success": None,
            "error": str(e),
        })


# -------------------------------------------------------
# IPO DUZENLE
# -------------------------------------------------------

@router.get("/ipo/{ipo_id}/edit", response_class=HTMLResponse)
async def edit_ipo_form(
    request: Request,
    ipo_id: int,
    success: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """IPO duzenleme formu."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    result = await db.execute(
        select(IPO)
        .options(selectinload(IPO.allocations), selectinload(IPO.ceiling_tracks))
        .where(IPO.id == ipo_id)
    )
    ipo = result.scalar_one_or_none()
    if not ipo:
        return RedirectResponse(url="/admin/?error=not_found", status_code=303)

    success_msg = None
    if success == "created":
        success_msg = "IPO basariyla olusturuldu!"
    elif success == "updated":
        success_msg = "IPO basariyla guncellendi!"

    return templates.TemplateResponse("admin/ipo_form.html", {
        "request": request,
        "ipo": ipo,
        "is_new": False,
        "success": success_msg,
        "error": None,
    })


@router.post("/ipo/{ipo_id}/edit")
async def update_ipo(
    request: Request,
    ipo_id: int,
    db: AsyncSession = Depends(get_db),
):
    """IPO guncelle."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    result = await db.execute(select(IPO).where(IPO.id == ipo_id))
    ipo = result.scalar_one_or_none()
    if not ipo:
        return RedirectResponse(url="/admin/?error=not_found", status_code=303)

    form = await request.form()

    try:
        # Temel bilgiler
        ipo.company_name = form.get("company_name", ipo.company_name).strip()
        ipo.ticker = form.get("ticker", "").strip().upper() or None
        ipo.logo_url = form.get("logo_url", "").strip() or None
        ipo.status = form.get("status", ipo.status)

        # Fiyat & buyukluk
        ipo.ipo_price = parse_decimal(form.get("ipo_price"))
        ipo.total_lots = parse_int(form.get("total_lots"))
        ipo.offering_size_tl = parse_decimal(form.get("offering_size_tl"))
        ipo.capital_increase_lots = parse_int(form.get("capital_increase_lots"))
        ipo.partner_sale_lots = parse_int(form.get("partner_sale_lots"))

        # Tarihler
        ipo.subscription_start = parse_date(form.get("subscription_start"))
        ipo.subscription_end = parse_date(form.get("subscription_end"))

        # subscription_hours — iki ayrı select'ten birleştir (HH:MM-HH:MM)
        sub_open = form.get("subscription_hour_open", "").strip()
        sub_close = form.get("subscription_hour_close", "").strip()
        if sub_open and sub_close:
            ipo.subscription_hours = f"{sub_open}-{sub_close}"
        elif sub_open:
            ipo.subscription_hours = f"{sub_open}-17:00"
        elif sub_close:
            ipo.subscription_hours = f"09:00-{sub_close}"
        # else: degistirme (mevcut degeri koru)

        ipo.trading_start = parse_date(form.get("trading_start"))
        ipo.spk_approval_date = parse_date(form.get("spk_approval_date"))
        ipo.expected_trading_date = parse_date(form.get("expected_trading_date"))

        # SPK referans
        ipo.spk_bulletin_no = form.get("spk_bulletin_no", "").strip() or None

        # Dagitim & katilim
        ipo.distribution_completed = parse_bool(form.get("distribution_completed"))
        ipo.distribution_method = form.get("distribution_method", "").strip() or None
        ipo.distribution_description = form.get("distribution_description", "").strip() or None
        ipo.participation_method = form.get("participation_method", "").strip() or None
        ipo.participation_description = form.get("participation_description", "").strip() or None
        ipo.public_float_pct = parse_decimal(form.get("public_float_pct"))
        ipo.discount_pct = parse_decimal(form.get("discount_pct"))

        # Pazar & araci
        ipo.market_segment = form.get("market_segment", "").strip() or None
        ipo.lead_broker = form.get("lead_broker", "").strip() or None

        # Tahmini lot
        ipo.estimated_lots_per_person = parse_int(form.get("estimated_lots_per_person"))

        # Ek bilgiler
        ipo.lock_up_period_days = parse_int(form.get("lock_up_period_days"))
        ipo.price_stability_days = parse_int(form.get("price_stability_days"))
        ipo.min_application_lot = parse_int(form.get("min_application_lot")) or 1

        # Sirket
        ipo.company_description = form.get("company_description", "").strip() or None
        ipo.sector = form.get("sector", "").strip() or None
        ipo.fund_usage = form.get("fund_usage", "").strip() or None

        # Mali veriler
        ipo.revenue_current_year = parse_decimal(form.get("revenue_current_year"))
        ipo.revenue_previous_year = parse_decimal(form.get("revenue_previous_year"))
        ipo.gross_profit = parse_decimal(form.get("gross_profit"))

        # Linkler
        ipo.kap_notification_url = form.get("kap_notification_url", "").strip() or None
        ipo.prospectus_url = form.get("prospectus_url", "").strip() or None
        ipo.spk_bulletin_url = form.get("spk_bulletin_url", "").strip() or None

        # Tahsisat
        ipo.allocation_announced = parse_bool(form.get("allocation_announced"))
        ipo.total_applicants = parse_int(form.get("total_applicants"))

        # Tavan takip
        ipo.ceiling_tracking_active = parse_bool(form.get("ceiling_tracking_active"))
        ipo.ceiling_broken = parse_bool(form.get("ceiling_broken"))

        # Arsiv
        ipo.archived = parse_bool(form.get("archived"))
        ipo.trading_day_count = parse_int(form.get("trading_day_count")) or 0

        # --- Auto-lock: Admin doldurulan alanlari otomatik kilitle ---
        import json as _json
        lockable_fields = {
            "subscription_start": ipo.subscription_start,
            "subscription_end": ipo.subscription_end,
            "subscription_hours": ipo.subscription_hours,
            "trading_start": ipo.trading_start,
            "ipo_price": ipo.ipo_price,
            "total_lots": ipo.total_lots,
            "expected_trading_date": ipo.expected_trading_date,
        }

        existing_locks = set()
        if ipo.manual_fields:
            try:
                existing_locks = set(_json.loads(ipo.manual_fields))
            except Exception:
                pass

        for field_name, field_val in lockable_fields.items():
            if field_val is not None:
                existing_locks.add(field_name)

        ipo.manual_fields = _json.dumps(list(existing_locks))

        ipo.updated_at = datetime.utcnow()

        await db.flush()
        logger.info(f"Admin: IPO guncellendi — {ipo.company_name} (ID: {ipo.id}) [locks: {list(existing_locks)}]")
        return RedirectResponse(url=f"/admin/ipo/{ipo.id}/edit?success=updated", status_code=303)

    except Exception as e:
        logger.error(f"Admin: IPO guncelleme hatasi — {e}")
        return templates.TemplateResponse("admin/ipo_form.html", {
            "request": request,
            "ipo": ipo,
            "is_new": False,
            "success": None,
            "error": str(e),
        })


# -------------------------------------------------------
# IPO SIL
# -------------------------------------------------------

@router.post("/ipo/{ipo_id}/delete")
async def delete_ipo(
    request: Request,
    ipo_id: int,
    db: AsyncSession = Depends(get_db),
):
    """IPO sil (cascade ile iliskili kayitlar da silinir)."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    result = await db.execute(
        select(IPO)
        .options(
            selectinload(IPO.allocations),
            selectinload(IPO.ceiling_tracks),
            selectinload(IPO.brokers),
        )
        .where(IPO.id == ipo_id)
    )
    ipo = result.scalar_one_or_none()
    if not ipo:
        return RedirectResponse(url="/admin/?error=not_found", status_code=303)

    # Kara listeye ekle — scraper ayni sirketi tekrar eklemesin
    from app.models import DeletedIPO
    from app.models.spk_application import SPKApplication
    deleted_record = DeletedIPO(
        company_name=ipo.company_name,
        ticker=ipo.ticker,
    )
    db.add(deleted_record)

    # SPK tablosundaki kaydı da "deleted" yap — scraper tekrar pending yapmasın
    spk_result = await db.execute(
        select(SPKApplication).where(
            SPKApplication.company_name == ipo.company_name
        )
    )
    spk_app = spk_result.scalar_one_or_none()
    if spk_app:
        spk_app.status = "deleted"

    logger.info(f"Admin: IPO siliniyor — {ipo.company_name} (ID: {ipo.id}) → kara listeye + SPK deleted")
    await db.delete(ipo)
    await db.flush()

    return RedirectResponse(url="/admin/?success=deleted", status_code=303)


# -------------------------------------------------------
# DAGITIM SONUCLARI
# -------------------------------------------------------

@router.get("/ipo/{ipo_id}/allocations", response_class=HTMLResponse)
async def allocations_form(
    request: Request,
    ipo_id: int,
    success: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Dagitim sonuclari formu."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    result = await db.execute(
        select(IPO)
        .options(selectinload(IPO.allocations))
        .where(IPO.id == ipo_id)
    )
    ipo = result.scalar_one_or_none()
    if not ipo:
        return RedirectResponse(url="/admin/?error=not_found", status_code=303)

    # Mevcut allocation verisini gruplara ayir
    groups = ["bireysel", "yuksek_basvurulu", "kurumsal_yurtici", "kurumsal_yurtdisi"]
    alloc_map = {}
    for alloc in ipo.allocations:
        alloc_map[alloc.group_name] = alloc

    return templates.TemplateResponse("admin/allocations.html", {
        "request": request,
        "ipo": ipo,
        "groups": groups,
        "alloc_map": alloc_map,
        "success": "Dagitim sonuclari kaydedildi!" if success == "saved" else None,
        "error": None,
    })


@router.post("/ipo/{ipo_id}/allocations")
async def save_allocations(
    request: Request,
    ipo_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Dagitim sonuclarini kaydet."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    result = await db.execute(
        select(IPO)
        .options(selectinload(IPO.allocations))
        .where(IPO.id == ipo_id)
    )
    ipo = result.scalar_one_or_none()
    if not ipo:
        return RedirectResponse(url="/admin/?error=not_found", status_code=303)

    form = await request.form()

    try:
        # Mevcut allocation'lari sil
        for existing in ipo.allocations:
            await db.delete(existing)

        # Yeni allocation'lari ekle
        groups = ["bireysel", "yuksek_basvurulu", "kurumsal_yurtici", "kurumsal_yurtdisi"]
        for group in groups:
            pct = parse_decimal(form.get(f"{group}_pct"))
            lots = parse_int(form.get(f"{group}_lots"))
            participants = parse_int(form.get(f"{group}_participants"))
            avg_lot = parse_decimal(form.get(f"{group}_avg_lot"))

            # Ortalama lot otomatik hesapla (bos ise)
            if avg_lot is None and lots and participants and participants > 0:
                avg_lot = Decimal(str(lots)) / Decimal(str(participants))
                avg_lot = avg_lot.quantize(Decimal("0.01"))

            # En az bir alan dolu olmali
            if any(v is not None for v in [pct, lots, participants, avg_lot]):
                alloc = IPOAllocation(
                    ipo_id=ipo.id,
                    group_name=group,
                    allocation_pct=pct,
                    allocated_lots=lots,
                    participant_count=participants,
                    avg_lot_per_person=avg_lot,
                )
                db.add(alloc)

        # Toplam basvuran
        ipo.total_applicants = parse_int(form.get("total_applicants"))
        ipo.allocation_announced = parse_bool(form.get("allocation_announced"))
        ipo.updated_at = datetime.utcnow()

        await db.flush()
        logger.info(f"Admin: Dagitim sonuclari kaydedildi — {ipo.company_name} (ID: {ipo.id})")
        return RedirectResponse(
            url=f"/admin/ipo/{ipo.id}/allocations?success=saved",
            status_code=303,
        )

    except Exception as e:
        logger.error(f"Admin: Dagitim kaydetme hatasi — {e}")
        groups = ["bireysel", "yuksek_basvurulu", "kurumsal_yurtici", "kurumsal_yurtdisi"]
        alloc_map = {a.group_name: a for a in ipo.allocations}
        return templates.TemplateResponse("admin/allocations.html", {
            "request": request,
            "ipo": ipo,
            "groups": groups,
            "alloc_map": alloc_map,
            "success": None,
            "error": str(e),
        })


# -------------------------------------------------------
# SPK BASVURULARI
# -------------------------------------------------------

@router.get("/spk", response_class=HTMLResponse)
async def spk_list(
    request: Request,
    success: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """SPK basvurulari listesi."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    result = await db.execute(
        select(SPKApplication)
        .order_by(desc(SPKApplication.created_at))
    )
    applications = list(result.scalars().all())

    return templates.TemplateResponse("admin/spk_list.html", {
        "request": request,
        "applications": applications,
        "success": success,
    })


@router.post("/spk/{app_id}/status")
async def update_spk_status(
    request: Request,
    app_id: int,
    db: AsyncSession = Depends(get_db),
):
    """SPK basvuru durumunu guncelle."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    result = await db.execute(
        select(SPKApplication).where(SPKApplication.id == app_id)
    )
    app = result.scalar_one_or_none()
    if not app:
        return RedirectResponse(url="/admin/spk?error=not_found", status_code=303)

    form = await request.form()
    new_status = form.get("status", app.status)
    app.status = new_status

    await db.flush()
    logger.info(f"Admin: SPK basvuru status guncellendi — {app.company_name} -> {new_status}")

    return RedirectResponse(url="/admin/spk?success=updated", status_code=303)


@router.post("/spk/{app_id}/delete")
async def delete_spk_application(
    request: Request,
    app_id: int,
    db: AsyncSession = Depends(get_db),
):
    """SPK basvuruyu siler."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    result = await db.execute(
        select(SPKApplication).where(SPKApplication.id == app_id)
    )
    app = result.scalar_one_or_none()
    if not app:
        return RedirectResponse(url="/admin/spk?error=not_found", status_code=303)

    company_name = app.company_name
    # Status'u "deleted" yap — DB'den silme, yoksa scraper tekrar ekler
    app.status = "deleted"
    await db.flush()
    logger.info(f"Admin: SPK basvuru deleted yapildi — {company_name} (id={app_id})")

    return RedirectResponse(url="/admin/spk?success=deleted", status_code=303)


@router.post("/spk/cleanup-duplicates")
async def cleanup_spk_duplicates(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Tum SPK tablosunu siler ve SPK sitesinden taze veri ceker (full resync).

    1. Tum kayitlari sil
    2. SPK sitesinden guncel listeyi cek
    3. Benzersiz kayitlari ekle (duplike olmaz)
    """
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    try:
        # 1. IPO tablosundaki TUM sirketleri al (SPK'dan gecmis — tekrar eklenmemeli)
        ipo_result = await db.execute(select(IPO.company_name))
        ipo_names_normalized = set()
        for (name,) in ipo_result.all():
            if name:
                ipo_names_normalized.add(_normalize_company_name(name))

        # 2. Tum SPK kayitlarini sil
        result = await db.execute(select(SPKApplication))
        all_apps = list(result.scalars().all())
        old_count = len(all_apps)
        for app in all_apps:
            await db.delete(app)
        await db.flush()

        # 3. SPK sitesinden taze veri cek
        from app.scrapers.spk_scraper import SPKScraper
        scraper = SPKScraper()
        try:
            applications = await scraper.fetch_ipo_applications()
        finally:
            await scraper.close()

        # 4. Benzersiz kayitlari ekle (IPO'daki sirketleri atla)
        seen_names = set()
        new_count = 0
        skipped_ipo = 0
        for app_data in applications:
            name = app_data.get("company_name", "").strip()
            if not name or name in seen_names:
                continue
            seen_names.add(name)

            # IPO tablosunda zaten var — atla
            if _is_company_in_ipo(name, ipo_names_normalized):
                skipped_ipo += 1
                continue

            db.add(SPKApplication(
                company_name=name,
                application_date=app_data.get("application_date"),
                status="pending",
            ))
            new_count += 1

        await db.flush()
        msg = f"{old_count} eski silindi, {new_count} taze yuklendi, {skipped_ipo} zaten IPO'da"
        logger.info(f"Admin: SPK full resync — {msg}")

        return RedirectResponse(
            url=f"/admin/spk?success={msg}",
            status_code=303,
        )

    except Exception as e:
        logger.error(f"Admin: SPK resync hatasi — {e}")
        return RedirectResponse(
            url=f"/admin/spk?success=Hata: {str(e)[:80]}",
            status_code=303,
        )


# ============================================================
# SCRAPER TETIKLEME
# ============================================================

async def _scraper_ipo_report(db: AsyncSession, source: str) -> str:
    """Scraper sonrasi aktif IPO durum raporu — kisa format.

    Telegram: Ticker + dolu alan sayisi / eksik alanlar (max ~30 kar/satir).
    """
    result = await db.execute(
        select(IPO).where(
            and_(
                IPO.archived == False,
                IPO.status.in_(["newly_approved", "in_distribution", "awaiting_trading", "trading"]),
            )
        )
    )
    active_ipos = list(result.scalars().all())

    if not active_ipos:
        return f"{source}: Aktif IPO yok"

    # Status gruplari
    status_emojis = {
        "newly_approved": "🆕",
        "in_distribution": "📋",
        "awaiting_trading": "⏳",
        "trading": "📈",
    }
    status_groups: dict[str, list] = {}
    for ipo in active_ipos:
        status_groups.setdefault(ipo.status, []).append(ipo)

    # Kontrol edilecek onemli alanlar (kisa etiket)
    key_fields = {
        "ipo_price": "Fyat",
        "subscription_start": "Bşl",
        "subscription_end": "Bts",
        "trading_start": "İşl",
        "total_lots": "Lot",
        "lead_broker": "Arcı",
    }

    # Telegram rapor satirlari
    tg_lines = []
    # Header: kaynak + toplam
    status_counts = []
    for sk, ipos in status_groups.items():
        e = status_emojis.get(sk, "📌")
        status_counts.append(f"{e}{len(ipos)}")
    tg_lines.append(
        f"🔄 <b>{source}</b> ✓ {len(active_ipos)} IPO"
        f" | {' '.join(status_counts)}"
    )

    # Her grup icin kisa satirlar
    for status_key in ["trading", "in_distribution", "awaiting_trading", "newly_approved"]:
        ipos = status_groups.get(status_key)
        if not ipos:
            continue
        emoji = status_emojis.get(status_key, "📌")

        for ipo in ipos:
            ticker = ipo.ticker or "?"
            # Eksik alan kontrolu
            missing = []
            filled = 0
            for field, flabel in key_fields.items():
                val = getattr(ipo, field, None)
                if val is None or (isinstance(val, str) and not val.strip()):
                    missing.append(flabel)
                else:
                    filled += 1

            if missing:
                # Kisa eksik listesi: max 30 kar
                m_str = ",".join(missing)
                if len(m_str) > 20:
                    m_str = m_str[:18] + ".."
                tg_lines.append(f"{emoji}{ticker} ⚠ -{m_str}")
            else:
                tg_lines.append(f"{emoji}{ticker} ✅ {filled}/{len(key_fields)}")

    # Dashboard mesaji
    summary = f"{source} OK! {len(active_ipos)} IPO"

    # Telegram gonder
    try:
        from app.services.admin_telegram import send_admin_message
        import asyncio
        asyncio.ensure_future(send_admin_message("\n".join(tg_lines)))
    except Exception as tg_err:
        logger.warning("Scraper Telegram rapor hatasi: %s", tg_err)

    return summary


@router.post("/run-scraper/halkarz")
async def run_halkarz_scraper(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """HalkArz scraper'ini admin panelden manuel tetikle."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    try:
        from app.scrapers.halkarz_scraper import scrape_halkarz
        await scrape_halkarz()

        # Rapor olustur
        summary = await _scraper_ipo_report(db, "HalkArz")
        # URL icin cok uzunsa kirp
        if len(summary) > 500:
            summary = summary[:497] + "..."

        return RedirectResponse(url=f"/admin/?success={summary}", status_code=303)
    except Exception as e:
        logger.error(f"Admin: HalkArz scraper tetikleme hatasi — {e}")
        return RedirectResponse(url=f"/admin/?error=HalkArz hatasi: {str(e)[:100]}", status_code=303)


@router.post("/run-scraper/gedik")
async def run_gedik_scraper(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Gedik scraper'ini admin panelden manuel tetikle."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    try:
        from app.scrapers.gedik_scraper import scrape_gedik
        await scrape_gedik()

        # Rapor olustur
        summary = await _scraper_ipo_report(db, "Gedik")
        if len(summary) > 500:
            summary = summary[:497] + "..."

        return RedirectResponse(url=f"/admin/?success={summary}", status_code=303)
    except Exception as e:
        logger.error(f"Admin: Gedik scraper tetikleme hatasi — {e}")
        return RedirectResponse(url=f"/admin/?error=Gedik hatasi: {str(e)[:100]}", status_code=303)


# -------------------------------------------------------
# TWEET KUYRUGU — Bekleyen Tweetler
# -------------------------------------------------------

@router.get("/tweets", response_class=HTMLResponse)
async def tweets_page(
    request: Request,
    status: str = "pending",
    trigger_msg: Optional[str] = None,
    trigger_ok: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Bekleyen tweetleri listeler."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    from app.models.pending_tweet import PendingTweet

    # Pending sayisi (badge icin)
    pending_result = await db.execute(
        select(sa_func.count(PendingTweet.id)).where(PendingTweet.status == "pending")
    )
    pending_count = pending_result.scalar() or 0

    # Filtreli liste
    query = select(PendingTweet)
    if status != "all":
        query = query.where(PendingTweet.status == status)
    query = query.order_by(desc(PendingTweet.created_at)).limit(50)

    result = await db.execute(query)
    tweets = list(result.scalars().all())

    # Auto-send durumu — DB'den okunur (restart'a dayanıklı)
    from app.services.twitter_service import is_auto_send
    auto_send = is_auto_send()

    return templates.TemplateResponse("admin/tweets.html", {
        "request": request,
        "tweets": tweets,
        "pending_count": pending_count,
        "current_status": status,
        "auto_send": auto_send,
        "trigger_message": trigger_msg,
        "trigger_success": trigger_ok == "1",
    })


@router.post("/ipo/{ipo_id}/run-prospectus-analysis")
async def run_prospectus_analysis_admin(
    ipo_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """İzahname PDF'ini admin panelden manuel olarak AI ile analiz et."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    from sqlalchemy import select
    from app.models.ipo import IPO

    result = await db.execute(select(IPO).where(IPO.id == ipo_id))
    ipo = result.scalar_one_or_none()
    if not ipo:
        return RedirectResponse(url=f"/admin/?error=IPO bulunamadı: {ipo_id}", status_code=303)

    if not ipo.prospectus_url:
        return RedirectResponse(
            url=f"/admin/ipo/{ipo_id}/edit?error=Bu IPO için izahname URL'si yok",
            status_code=303,
        )

    # Mevcut analizi sıfırla (yeniden analiz için)
    ipo.prospectus_analysis = None
    ipo.prospectus_analyzed_at = None
    ipo.prospectus_tweeted = False
    await db.commit()

    try:
        from app.services.prospectus_analyzer import analyze_prospectus
        # GC korumalı arka plan görevi (_bg_tasks referans tutar)
        _fire_and_forget(analyze_prospectus(ipo_id, ipo.prospectus_url, delay_seconds=0))
        msg = f"İzahname analizi başlatıldı: {ipo.company_name} — birkaç dakika içinde tamamlanır"
        return RedirectResponse(url=f"/admin/?success={msg}", status_code=303)
    except Exception as e:
        logger.error("Admin prospectus analiz hatasi: %s", e)
        return RedirectResponse(
            url=f"/admin/?error=Analiz başlatılamadı: {str(e)[:100]}",
            status_code=303,
        )


@router.get("/ipo/{ipo_id}/prospectus-debug")
async def debug_prospectus_analysis(
    ipo_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """İzahname analizi adım adım teşhis — hangi adımda hata olduğunu gösterir."""
    from fastapi.responses import JSONResponse
    if not get_current_admin(request):
        return JSONResponse({"error": "Yetkisiz"}, status_code=401)

    result = {"ipo_id": ipo_id, "steps": {}}

    try:
        # Adım 1: IPO DB kaydı
        from app.models.ipo import IPO
        r = await db.execute(select(IPO).where(IPO.id == ipo_id))
        ipo = r.scalar_one_or_none()
        if not ipo:
            return JSONResponse({"error": f"IPO bulunamadı: {ipo_id}"})
        result["company"] = ipo.company_name
        result["prospectus_url"] = ipo.prospectus_url
        result["steps"]["1_ipo_found"] = True

        if not ipo.prospectus_url:
            result["steps"]["error"] = "prospectus_url yok"
            return JSONResponse(result)

        # Adım 2: PDF import
        try:
            import pdfplumber
            result["steps"]["2_pdfplumber_import"] = True
        except Exception as e:
            result["steps"]["2_pdfplumber_import"] = False
            result["steps"]["error"] = f"pdfplumber import hatası: {e}"
            return JSONResponse(result)

        # Adım 3: PDF indir
        try:
            import httpx, tempfile
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
                tmp_path = f.name
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                resp = await client.get(ipo.prospectus_url)
                with open(tmp_path, "wb") as f:
                    f.write(resp.content)
            import os
            size_kb = os.path.getsize(tmp_path) // 1024
            result["steps"]["3_pdf_download"] = f"OK ({size_kb} KB, status={resp.status_code})"
        except Exception as e:
            result["steps"]["3_pdf_download"] = False
            result["steps"]["error"] = f"PDF indirme hatası: {e}"
            return JSONResponse(result)

        # Adım 4a: pdfplumber metin çıkar
        try:
            text_plumber = ""
            with pdfplumber.open(tmp_path) as pdf:
                pages_read = min(5, len(pdf.pages))
                for page in pdf.pages[:pages_read]:
                    text_plumber += page.extract_text() or ""
            result["steps"]["4a_pdfplumber"] = f"OK ({len(text_plumber)} karakter, ilk 5 sayfa)"
        except Exception as e:
            result["steps"]["4a_pdfplumber"] = f"HATA: {e}"

        # Adım 4b: PyMuPDF metin çıkar
        try:
            import fitz  # PyMuPDF
            text_fitz = ""
            doc = fitz.open(tmp_path)
            pages_read = min(5, len(doc))
            for page in doc[:pages_read]:
                text_fitz += page.get_text("text") or ""
            doc.close()
            result["steps"]["4b_pymupdf"] = f"OK ({len(text_fitz)} karakter, ilk 5 sayfa)"
            result["text_sample"] = text_fitz[:300] if text_fitz else text_plumber[:300]
        except Exception as e:
            result["steps"]["4b_pymupdf"] = f"HATA: {e}"
            result["text_sample"] = text_plumber[:300]

        # Adım 5: Abacus API test
        try:
            from app.config import get_settings
            api_key = get_settings().ABACUS_API_KEY
            result["steps"]["5_api_key"] = "OK" if api_key else "YOK"
        except Exception as e:
            result["steps"]["5_api_key"] = f"Hata: {e}"

        result["steps"]["all_ok"] = True
        return JSONResponse(result)

    except Exception as e:
        result["steps"]["fatal_error"] = str(e)
        return JSONResponse(result)


@router.post("/tweets/trigger-snapshot")
async def trigger_snapshot_from_admin(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Admin panelden T15 ogle arasi market snapshot tweet'ini tetikler."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    try:
        from app.scheduler import market_snapshot_tweet
        result = await market_snapshot_tweet()
        if result and result.get("error"):
            logger.error("[ADMIN] T15 hatasi: %s", result["error"])
            from urllib.parse import quote
            msg = quote(f"T15 Hata: {result['error'][:150]}")
            return RedirectResponse(
                url=f"/admin/tweets?trigger_msg={msg}&trigger_ok=0",
                status_code=303,
            )
        msg_text = result.get("message", "Başarılı!") if result else "Başarılı!"
        from urllib.parse import quote
        msg = quote(f"T15: {msg_text}")
        logger.info("[ADMIN] T15 tetiklendi: %s", msg_text)
        return RedirectResponse(
            url=f"/admin/tweets?trigger_msg={msg}&trigger_ok=1",
            status_code=303,
        )
    except Exception as e:
        logger.error("[ADMIN] T15 tetikleme hatasi: %s", e)
        from urllib.parse import quote
        msg = quote(f"T15 Hata: {str(e)[:100]}")
        return RedirectResponse(
            url=f"/admin/tweets?trigger_msg={msg}&trigger_ok=0",
            status_code=303,
        )


@router.post("/tweets/trigger-opening")
async def trigger_opening_from_admin(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Admin panelden T16 acilis bilgileri tweet'ini tetikler.
    Scheduler'daki opening_summary_tweet fonksiyonunu cagirir.
    """
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    try:
        from app.scheduler import opening_summary_tweet

        result = await opening_summary_tweet()

        if result and result.get("error"):
            logger.error("[ADMIN] T16 hatasi: %s", result["error"])
            msg = quote(f"T16 Hata: {result['error'][:150]}")
            return RedirectResponse(url=f"/admin/tweets?trigger_msg={msg}&trigger_ok=0", status_code=303)

        msg_text = result.get("message", "T16 tetiklendi") if result else "T16 tetiklendi (sonuc yok)"
        msg = quote(f"T16: {msg_text}")
        logger.info("[ADMIN] T16 tetiklendi: %s", msg_text)
        return RedirectResponse(url=f"/admin/tweets?trigger_msg={msg}&trigger_ok=1", status_code=303)

    except Exception as e:
        logger.error("[ADMIN] T16 tetikleme hatasi: %s", e)
        msg = quote(f"T16 Hata: {str(e)[:100]}")
        return RedirectResponse(url=f"/admin/tweets?trigger_msg={msg}&trigger_ok=0", status_code=303)


@router.post("/tweets/trigger-opening-push")
async def trigger_opening_push_from_admin(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Admin panelden acilis push bildirimlerini tetikler.
    Trading durumundaki hisselerin son fiyat verisine bakarak
    her hisse icin gunluk_acilis_kapanis bildirimi gonderir.
    """
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    try:
        from sqlalchemy import select, and_
        from app.models.ipo import IPO, IPOCeilingTrack
        from app.services.notification import NotificationService

        # Trading durumundaki hisseleri bul
        result = await db.execute(
            select(IPO).where(
                and_(
                    IPO.status == "trading",
                    IPO.archived == False,
                    IPO.ticker.isnot(None),
                    IPO.ceiling_tracking_active == True,
                )
            )
        )
        trading_ipos = list(result.scalars().all())

        if not trading_ipos:
            msg = quote("Acilis Push: Trading durumunda hisse yok")
            return RedirectResponse(url=f"/admin/tweets?trigger_msg={msg}&trigger_ok=0", status_code=303)

        notif_service = NotificationService(db)
        from datetime import date as date_type
        today = date_type.today()
        sent_total = 0

        for ipo in trading_ipos:
            # Bugunun track verisini bul
            track_result = await db.execute(
                select(IPOCeilingTrack).where(
                    and_(
                        IPOCeilingTrack.ipo_id == ipo.id,
                        IPOCeilingTrack.trade_date == today,
                    )
                )
            )
            today_track = track_result.scalar_one_or_none()

            if today_track and today_track.close_price:
                hit_ceiling = today_track.hit_ceiling or False
                hit_floor = today_track.hit_floor or False
                pct_change = float(today_track.pct_change) if today_track.pct_change else 0.0
            else:
                hit_ceiling = False
                hit_floor = False
                pct_change = 0.0

            ticker = ipo.ticker
            if hit_ceiling:
                title = f"🚀 Seans Açılış: {ticker} Tavan Açtı!"
                body = f"{ticker} tavan fiyatından açıldı 🎯"
            elif hit_floor:
                title = f"📉 Seans Açılış: {ticker} Taban Açtı!"
                body = f"{ticker} taban fiyatından açıldı ⚠️"
            else:
                # %0.00 nötr — bildirim gönderme
                if abs(pct_change) < 0.005:
                    continue
                gap_str = f"%+{abs(pct_change):.2f}" if pct_change >= 0 else f"%-{abs(pct_change):.2f}"
                if pct_change >= 0:
                    title = f"🟢 Seans Açılış: {ticker} Alıcılı Açtı"
                    body = f"Açılış Gap: {gap_str}"
                else:
                    title = f"🔴 Seans Açılış: {ticker} Satıcılı Açtı"
                    body = f"Açılış Gap: {gap_str}"

            # Bu IPO icin aktif aboneleri bul ve bildirim gonder
            from app.models.user import StockNotificationSubscription, User
            stock_notif_result = await db.execute(
                select(StockNotificationSubscription).where(
                    and_(
                        or_(
                            and_(
                                StockNotificationSubscription.ipo_id == ipo.id,
                                StockNotificationSubscription.notification_type == "gunluk_acilis_kapanis",
                            ),
                            StockNotificationSubscription.is_annual_bundle == True,
                        ),
                        StockNotificationSubscription.is_active == True,
                        StockNotificationSubscription.muted == False,
                    )
                )
            )
            active_subs = list(stock_notif_result.scalars().all())

            notified_ids = set()
            for sub in active_subs:
                if sub.user_id in notified_ids:
                    continue
                user_result = await db.execute(select(User).where(User.id == sub.user_id))
                user = user_result.scalar_one_or_none()
                if not user:
                    continue
                fcm = (user.fcm_token or "").strip()
                expo = (user.expo_push_token or "").strip()
                if not fcm and not expo:
                    continue
                if not user.notifications_enabled:
                    continue

                try:
                    success = await notif_service._send_to_user(
                        user=user,
                        title=title,
                        body=body,
                        data={
                            "type": "stock_notification",
                            "notification_type": "gunluk_acilis_kapanis",
                            "ticker": ticker,
                            "ipo_id": str(ipo.id),
                        },
                        channel_id="ceiling_alerts_v2",
                    )
                    if success:
                        sent_total += 1
                        notified_ids.add(sub.user_id)
                except Exception:
                    pass

        tickers_str = ", ".join(ipo.ticker for ipo in trading_ipos)
        msg = quote(f"Açılış Push: {sent_total} bildirim gönderildi ({tickers_str})")
        logger.info("[ADMIN] Acilis push: %d bildirim — %s", sent_total, tickers_str)
        return RedirectResponse(url=f"/admin/tweets?trigger_msg={msg}&trigger_ok=1", status_code=303)

    except Exception as e:
        logger.error("[ADMIN] Acilis push hatasi: %s", e, exc_info=True)
        msg = quote(f"Push Hata: {str(e)[:100]}")
        return RedirectResponse(url=f"/admin/tweets?trigger_msg={msg}&trigger_ok=0", status_code=303)


@router.post("/reminders/trigger-now")
async def trigger_reminder_now(
    request: Request,
    reminder_type: str = "reminder_4h",
    ticker: str = "",
):
    """Admin panelden hatirlatma tweet + push bildirimini aninda tetikler.

    reminder_type: reminder_4h | reminder_30min | reminder_1h | reminder_2h
    ticker: sadece bu ticker icin tetikle (bos = bugun son gun olan tum IPO'lar)

    Dedup bypass edilir — zaten gonderilmis olsa bile tekrar gonderir.
    Zamanlama penceresi kontrolu atlanir.
    """
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    valid_types = ["reminder_4h", "reminder_30min", "reminder_1h", "reminder_2h"]
    if reminder_type not in valid_types:
        msg = quote(f"Gecersiz reminder_type: {reminder_type}. Gecerli: {', '.join(valid_types)}")
        return RedirectResponse(url=f"/admin/tweets?trigger_msg={msg}&trigger_ok=0", status_code=303)

    try:
        from app.scheduler import check_reminders
        logger.info(
            "[ADMIN] Hatirlatma force tetikleniyor: type=%s ticker=%s",
            reminder_type, ticker or "(hepsi)",
        )
        await check_reminders(
            force_reminder_type=reminder_type,
            force_ticker=ticker.strip().upper() if ticker.strip() else None,
        )
        ticker_info = f" ({ticker.upper()})" if ticker.strip() else ""
        msg = quote(f"Hatirlatma tetiklendi: {reminder_type}{ticker_info}")
        return RedirectResponse(url=f"/admin/tweets?trigger_msg={msg}&trigger_ok=1", status_code=303)

    except Exception as e:
        logger.error("[ADMIN] Hatirlatma tetikleme hatasi: %s", e, exc_info=True)
        msg = quote(f"Hatirlatma Hata: {str(e)[:100]}")
        return RedirectResponse(url=f"/admin/tweets?trigger_msg={msg}&trigger_ok=0", status_code=303)


@router.post("/tweets/{tweet_id}/approve")
async def approve_tweet(
    request: Request,
    tweet_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Tweet'i onayla ve X'e gonder."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    from datetime import datetime, timezone
    from app.models.pending_tweet import PendingTweet

    result = await db.execute(
        select(PendingTweet).where(PendingTweet.id == tweet_id)
    )
    tweet = result.scalar_one_or_none()
    if not tweet or tweet.status != "pending":
        return RedirectResponse(url="/admin/tweets", status_code=303)

    # Tweet'i gercekten at — force_send=True ile auto_send kontrolunu atla
    from app.services.twitter_service import _safe_tweet as real_tweet, _safe_tweet_with_media as real_tweet_media

    try:
        if tweet.image_path:
            success = real_tweet_media(tweet.text, tweet.image_path, source="admin_approve", force_send=True)
        else:
            success = real_tweet(tweet.text, source="admin_approve", force_send=True)

        if success:
            tweet.status = "sent"
            tweet.sent_at = datetime.now(timezone.utc)
            # Basarili gonderim sonrasi temp gorsel dosyasini temizle
            if tweet.image_path:
                try:
                    import os
                    if os.path.exists(tweet.image_path):
                        os.remove(tweet.image_path)
                except OSError:
                    pass
        else:
            tweet.status = "failed"
            tweet.error_message = "Tweet gonderilemedi"
    except Exception as e:
        tweet.status = "failed"
        tweet.error_message = str(e)[:500]

    tweet.reviewed_at = datetime.now(timezone.utc)
    await db.commit()

    return RedirectResponse(url="/admin/tweets", status_code=303)


@router.post("/tweets/{tweet_id}/reject")
async def reject_tweet(
    request: Request,
    tweet_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Tweet'i reddet."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    from datetime import datetime, timezone
    from app.models.pending_tweet import PendingTweet

    result = await db.execute(
        select(PendingTweet).where(PendingTweet.id == tweet_id)
    )
    tweet = result.scalar_one_or_none()
    if not tweet or tweet.status != "pending":
        return RedirectResponse(url="/admin/tweets", status_code=303)

    tweet.status = "rejected"
    tweet.reviewed_at = datetime.now(timezone.utc)
    await db.commit()

    return RedirectResponse(url="/admin/tweets", status_code=303)


@router.post("/tweets/trigger-spk-analysis")
async def trigger_spk_analysis_from_admin(
    request: Request,
):
    """Admin panelden veya API key ile SPK bülten analizini tetikler."""
    # Session auth VEYA API key
    api_key = request.headers.get("X-Admin-Key", "")
    is_api = api_key == os.getenv("ADMIN_PASSWORD", "SzBist2026Admin!")
    is_session = bool(get_current_admin(request))

    if not is_api and not is_session:
        from fastapi.responses import JSONResponse as _JR
        if "application/json" in request.headers.get("accept", ""):
            return _JR({"error": "Unauthorized"}, status_code=401)
        return RedirectResponse(url="/admin/login", status_code=303)

    from fastapi.responses import JSONResponse
    from urllib.parse import quote
    try:
        import httpx as _hx
        import pdfplumber
        import io
        from app.scrapers.spk_bulletin_scraper import format_tables_for_analysis
        from app.services.twitter_service import tweet_spk_bulletin_analysis

        # Body'den veya query'den URL/bulletin_no alabilir
        body = {}
        try:
            body = await request.json()
        except Exception:
            pass
        pdf_url = body.get("pdf_url") or request.query_params.get("pdf_url")
        bulletin_no = body.get("bulletin_no") or request.query_params.get("bulletin_no", "2026/10")

        if not pdf_url:
            # Scraper ile bul
            from app.scrapers.spk_bulletin_scraper import SPKBulletinScraper, bulletin_no_str
            scraper = SPKBulletinScraper()
            try:
                bulletins = await scraper.fetch_bulletin_list(year=2026)
                target = bulletins[-1] if bulletins else None
                for b in reversed(bulletins):
                    yr, no = bulletin_no.split("/") if "/" in bulletin_no else (2026, 10)
                    if b["bulletin_no"] == (int(yr), int(no)):
                        target = b
                        break
                if target:
                    pdf_url = target["pdf_url"]
                    bulletin_no = bulletin_no_str(*target["bulletin_no"])
            finally:
                await scraper.close()

        if not pdf_url:
            err = "PDF URL bulunamadı — pdf_url parametresi verin"
            if is_api:
                return JSONResponse({"ok": False, "error": err})
            return RedirectResponse(url=f"/admin/tweets?trigger_msg={quote(err)}&trigger_ok=0", status_code=303)

        # PDF indir (sync httpx — verify=False)
        logger.info("[ADMIN] SPK PDF indiriliyor: %s", pdf_url)
        async with _hx.AsyncClient(timeout=30, verify=False, follow_redirects=True) as client:
            r = await client.get(pdf_url)
        if r.status_code != 200:
            err = f"SPK PDF HTTP {r.status_code}"
            if is_api:
                return JSONResponse({"ok": False, "error": err})
            return RedirectResponse(url=f"/admin/tweets?trigger_msg={quote(err)}&trigger_ok=0", status_code=303)

        logger.info("[ADMIN] SPK PDF indirildi: %d bytes", len(r.content))

        # Parse PDF
        pdf = pdfplumber.open(io.BytesIO(r.content))
        full_text = ""
        tables = []
        for page in pdf.pages:
            full_text += (page.extract_text() or "") + "\n"
            for t in (page.extract_tables() or []):
                tables.append(t)

        analysis_text = format_tables_for_analysis(tables, full_text)
        if len(analysis_text) < 50:
            analysis_text = full_text  # Fallback: tüm text

        if len(analysis_text) < 50:
            err = f"SPK bülten içeriği çok kısa ({len(analysis_text)} kar)"
            if is_api:
                return JSONResponse({"ok": False, "error": err})
            return RedirectResponse(url=f"/admin/tweets?trigger_msg={quote(err)}&trigger_ok=0", status_code=303)

        logger.info("[ADMIN] SPK analiz text: %d kar, tweet atılıyor...", len(analysis_text))
        result = tweet_spk_bulletin_analysis(analysis_text, bulletin_no)
        if is_api:
            return JSONResponse({"ok": bool(result), "bulletin": bulletin_no, "text_len": len(analysis_text)})
        msg = quote(f"SPK Bülten {bulletin_no}: {'Başarılı' if result else 'Başarısız'}")
        return RedirectResponse(
            url=f"/admin/tweets?trigger_msg={msg}&trigger_ok={'1' if result else '0'}",
            status_code=303,
        )
    except Exception as e:
        logger.error("[ADMIN] SPK bülten analiz retry hatası: %s", e, exc_info=True)
        if is_api:
            return JSONResponse({"ok": False, "error": str(e)[:200]})
        msg = quote(f"SPK Hata: {str(e)[:150]}")
        return RedirectResponse(url=f"/admin/tweets?trigger_msg={msg}&trigger_ok=0", status_code=303)


@router.post("/tweets/toggle-auto-send")
async def toggle_auto_send(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """TWITTER_AUTO_SEND toggle — DB'ye kaydeder, restart'a dayanıklı.

    True  → Otomatik mod (tweetler direkt X'e atilir)
    False → Onay modu (tweetler kuyruğa düşer, admin onaylar)

    Değer app_settings tablosunda saklanır — Render restart olsa bile korunur.
    """
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    from app.models.app_setting import AppSetting
    from app.services.twitter_service import clear_settings_cache, is_auto_send

    # Mevcut durumu DB'den oku
    current = is_auto_send()
    new_val = "false" if current else "true"

    # DB'ye yaz (upsert)
    result = await db.execute(
        select(AppSetting).where(AppSetting.key == "TWITTER_AUTO_SEND")
    )
    setting = result.scalar_one_or_none()
    if setting:
        setting.value = new_val
    else:
        db.add(AppSetting(key="TWITTER_AUTO_SEND", value=new_val))
    await db.commit()

    # Cache'i hemen sıfırla ki değişiklik anında yansısın
    clear_settings_cache()

    logger.info(
        "[ADMIN] TWITTER_AUTO_SEND -> %s (DB'ye kaydedildi, restart'a dayanıklı)",
        new_val,
    )

    return RedirectResponse(url="/admin/tweets", status_code=303)


# -------------------------------------------------------
# TWEET ONIZLEME — Sadece goruntuleme
# -------------------------------------------------------

@router.get("/tweet-preview", response_class=HTMLResponse)
async def tweet_preview_page(request: Request):
    """Tweet şablonları önizleme — ATATR + EREGL örnekleri."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)
    return templates.TemplateResponse("admin/tweet_preview.html", {"request": request})


# Not: _TWEET_TEMPLATES ve _TWEET_EXAMPLES artık kullanılmıyor — /admin/tweet-preview sayfasında
# statik HTML olarak gösteriliyor. Aşağıda sadece referans olarak bırakıldı.
_TWEET_TEMPLATES = [
    # 1. Yeni Halka Arz
    ("T1_BASLIK", "Başlık", "\U0001F6A8 SPK Bülteni Yayımlandı!", "1", "Yeni Halka Arz (SPK Onayı)"),
    ("T1_ACIKLAMA", "Açıklama", "için halka arz başvurusu SPK tarafından onaylandı.", "1", None),
    ("T1_CTA", "CTA (çağrı)", "\U0001F4F2 Bilgiler geldikçe bildirim göndereceğiz.", "1", None),
    # 2. Dağıtıma Çıkış
    ("T2_BASLIK", "Başlık", "\U0001F4CB Halka Arz Başvuruları Başladı!", "2", "Dağıtıma Çıkış"),
    ("T2_ACIKLAMA", "Açıklama", "için talep toplama süreci başlamıştır.", "2", None),
    # 3. Dağıtım Sonuçları
    ("T3_BASLIK", "Başlık", "✅ Kesinleşen Dağıtım Sonuçları", "3", "Kesinleşen Dağıtım Sonuçları"),
    # 4. Son 4 Saat
    ("T4_BASLIK", "Başlık", "\u23F0 Son 4 Saat!", "4", "Son 4 Saat Hatırlatma"),
    ("T4_ACIKLAMA", "Açıklama", "halka arz başvurusu için kapanışa son 4 saat kaldı!", "4", None),
    # 5. Son 30 Dakika
    ("T5_BASLIK", "Başlık", "\U0001F6A8 Son 30 Dakika!", "5", "Son 30 Dakika Hatırlatma"),
    ("T5_ACIKLAMA", "Açıklama", "halka arz başvurusu kapanmak üzere!", "5", None),
    # 6. İlk İşlem Günü
    ("T6_BASLIK", "Başlık", "\U0001F514 Gong Çalıyor!", "6", "İlk İşlem Günü (Gong)"),
    ("T6_ACIKLAMA", "Açıklama", "bugün borsada işleme başlıyor!", "6", None),
    ("T6_CTA", "CTA", "25 günlük tavan/taban takibini uygulamamızdan yapabilirsiniz.", "6", None),
    # 7. Açılış Fiyatı
    ("T7_BASLIK", "Başlık", "\U0001F4C8 Açılış Fiyatı Belli Oldu!", "7", "Açılış Fiyatı"),
    # 8. Günlük Takip (çoğunlukla dinamik)
    ("T8_INFO", "_info", "Tamamı dinamik — düzenlenebilir alan yok", "8", "Günlük Takip (18:20)"),
    # 9. 25 Gün Performans (çoğunlukla dinamik)
    ("T9_INFO", "_info", "Tamamı dinamik — düzenlenebilir alan yok", "9", "25 Gün Performans Özeti"),
    # 10. Ay Sonu Rapor (çoğunlukla dinamik)
    ("T10_INFO", "_info", "Tamamı dinamik — düzenlenebilir alan yok", "10", "Ay Sonu Halka Arz Raporu"),
    # 11. BIST50 KAP
    ("T11_TANITIM", "Tanıtım Metni", "350+ hisse senedini tarayan sistemimiz çok yakında AppStore ve GoogleStore'da!", "11", "BIST50 KAP Haberi"),
    ("T11_CTA", "CTA", "Ücretsiz BIST 50 bildirimleri için:", "11", None),
    # 12. Son Gün Sabah
    ("T12_BASLIK", "Başlık", "\U0001F4E2 Son Başvuru Günü!", "12", "Son Gün Sabah"),
    ("T12_CTA", "CTA", "\u23F0 Son anlara kadar hatırlatma yapacağız.", "12", None),
    # 13. Şirket Tanıtım
    ("T13_BASLIK", "Başlık", "\U0001F4CB Halka Arz Hakkında", "13", "Şirket Tanıtım"),
    # 14. SPK Bekleyenler
    ("T14_ACIKLAMA", "Açıklama", "Güncel listeyi uygulamamızdan takip edebilirsiniz.", "14", "SPK Bekleyenler (Aylık)"),
    # 15. Öğle Arası Market Snapshot
    ("T15_BASLIK", "Başlık", "\U0001F4CA Öğle Arası", "15", "Öğle Arası Market Snapshot"),
    # 16. Yeni Halka Arzlar Açılış Bilgileri
    ("T16_BASLIK", "Başlık", "\U0001F4CA Yeni Halka Arzlar — Açılış Bilgileri", "16", "Açılış Bilgileri (İlk 5 Gün)"),
]

# Her tweet grubunun örnek formatı (★ = admin'den düzenlenebilir)
_TWEET_EXAMPLES = {
    "1": (
        "★{T1_BASLIK}\n\n"
        "{şirket_adı} (#{ticker}) ★{T1_ACIKLAMA}\n"
        "Fiyat: {fiyat} TL\n\n"
        "★{T1_CTA}\n"
        "Detaylar için: ★{APP_LINK}\n\n"
        "#HalkaArz #BIST #Borsa"
    ),
    "2": (
        "★{T2_BASLIK}\n\n"
        "{şirket_adı} (#{ticker}) ★{T2_ACIKLAMA}\n"
        "Fiyat: {fiyat} TL\n"
        "Son başvuru: {tarih}\n"
        "Tahmini: ~{lot} lot/kişi (★{LOT_DISCLAIMER})\n\n"
        "📲 ★{APP_LINK}\n\n"
        "#HalkaArz #BIST #{ticker}"
    ),
    "3": (
        "★{T3_BASLIK}\n\n"
        "{şirket_adı} (#{ticker})\n\n"
        "Bireysel: {lot} lot | {başvuru_sayısı} kişi\n\n"
        "📲 ★{APP_LINK}\n\n"
        "#HalkaArz #{ticker}"
    ),
    "4": (
        "★{T4_BASLIK}\n\n"
        "{şirket_adı} (#{ticker}) ★{T4_ACIKLAMA}\n"
        "📊 Tahmini: ~{lot} lot/kişi (★{LOT_DISCLAIMER})\n\n"
        "⏳ Başvurular saat {saat}'a kadar devam ediyor.\n\n"
        "📲 ★{APP_LINK}\n\n"
        "#HalkaArz #SonGün #{ticker}"
    ),
    "5": (
        "★{T5_BASLIK}\n\n"
        "{şirket_adı} (#{ticker}) ★{T5_ACIKLAMA}\n"
        "📊 Tahmini: ~{lot} lot/kişi (★{LOT_DISCLAIMER})\n\n"
        "Saat {saat}'da başvurular kapanıyor, acele edin!\n\n"
        "📲 ★{APP_LINK}\n\n"
        "#HalkaArz #SonDakika #{ticker}"
    ),
    "6": (
        "★{T6_BASLIK}\n\n"
        "{şirket_adı} (#{ticker}) ★{T6_ACIKLAMA}\n"
        "Halka arz fiyatı: {fiyat} TL\n\n"
        "★{T6_CTA}\n\n"
        "📲 ★{APP_LINK}\n\n"
        "#HalkaArz #BIST #{ticker}"
    ),
    "7": (
        "★{T7_BASLIK}\n\n"
        "{şirket_adı} (#{ticker})\n\n"
        "• Halka arz fiyatı: {fiyat} TL\n"
        "• Açılış fiyatı: {açılış_fiyatı} TL\n"
        "• Durum: {durum}\n\n"
        "📲 ★{APP_LINK}\n\n"
        "#HalkaArz #{ticker}"
    ),
    "8": (
        "📊 #{ticker} — {gün}/25 Gün Sonu\n\n"
        "Halka Arz: {fiyat} TL\n"
        "Kapanış: {kapanış} TL | %{değişim} | {durum}\n"
        "Kümülatif: %{kümülatif}\n\n"
        "Tavan: {tavan_gün} | Taban: {taban_gün} | Normal: {normal}\n\n"
        "📲 ★{APP_LINK}\n"
        "#HalkaArz #{ticker}\n\n"
        "⚠️ T8 çoğunlukla dinamik — düzenlenebilir alan yok"
    ),
    "9": (
        "📋 #{ticker} — 25 Günü Bitirdi\n\n"
        "Halka Arz: {fiyat} TL\n"
        "Kişi Başı Ort Lot: {lot}\n\n"
        "Tavan: {tavan_gün} | Taban: {taban_gün} | Normal: {normal}\n\n"
        "📲 ★{APP_LINK}\n"
        "#HalkaArz #BIST #{ticker}\n\n"
        "⚠️ T9 çoğunlukla dinamik — düzenlenebilir alan yok"
    ),
    "10": (
        "📊 {yıl} Halka Arz — {ay} Sonu Raporu\n\n"
        "• Toplam halka arz: {toplam}\n"
        "• 25 günü doldu: {tamamlanan}\n"
        "• Ort. getiri: %{getiri}\n"
        "• En iyi: #{en_iyi} (%{en_iyi_getiri})\n\n"
        "📲 ★{APP_LINK}\n"
        "#HalkaArz #BIST #AySonuRaporu\n\n"
        "⚠️ T10 çoğunlukla dinamik — düzenlenebilir alan yok"
    ),
    "11": (
        "{emoji} #{ticker} — Haber Bildirimi\n\n"
        "Anlık Haber Yakalandı {tarih_saat}\n\n"
        "İlişkili Kelime: {anahtar_kelime}\n\n"
        "★{T11_TANITIM}\n\n"
        "★{T11_CTA}\n"
        "📲 ★{APP_LINK}\n\n"
        "#BIST50 #{ticker} #KAP #Borsa"
    ),
    "12": (
        "★{T12_BASLIK}\n\n"
        "{şirket_adı} (#{ticker}) için halka arz başvuruları\n"
        "bugün saat {saat}'a kadar devam ediyor.\n"
        "Fiyat: {fiyat} TL\n\n"
        "★{T12_CTA}\n\n"
        "📲 ★{APP_LINK}\n\n"
        "#HalkaArz #{ticker}"
    ),
    "13": (
        "★{T13_BASLIK}\n\n"
        "{şirket_adı} (#{ticker})\n"
        "SPK Onay: {spk_tarih}\n"
        "Sektör: {sektör}\n"
        "Fiyat: {fiyat} TL\n"
        "{açıklama_metni}\n\n"
        "📲 Detaylar: ★{APP_LINK}\n\n"
        "#HalkaArz #{ticker}"
    ),
    "14": (
        "📊 SPK Onay Bekleyenler\n\n"
        "Şu an {adet} şirket SPK onayı beklemektedir.\n\n"
        "★{T14_ACIKLAMA}\n\n"
        "📲 ★{APP_LINK}\n\n"
        "#HalkaArz #SPK #BIST #Borsa"
    ),
    "15": (
        "★{T15_BASLIK} — {hisse_sayısı} Hisse\n\n"
        "🟢 #ASELS 5/25 %+2.3\n"
        "🔴 #SZALG 12/25 %-1.1\n"
        "...\n\n"
        "Tavan: {tavan} | Taban: {taban}\n\n"
        "📲 ★{APP_LINK}\n"
        "#HalkaArz #BIST #Borsa"
    ),
    "16": (
        "★{T16_BASLIK}\n\n"
        "🟢 #AKHAN 2. Gün | Açılış: 28.86 TL | %+9.5 (Tavan)\n"
        "🟢 #NETCD 3. Gün | Açılış: 108.50 TL | %+5.2\n"
        "🔴 #BESTE 1. Gün | Açılış: 25.80 TL | %-0.8\n\n"
        "📲 ★{APP_LINK}\n"
        "#HalkaArz #BIST #Borsa\n\n"
        "📷 Yatay sütunlu görsel otomatik eklenir\n"
        "(HA fiyat, Açılış, %, Durum, T/Tb/N istatistikleri)"
    ),
}



# -------------------------------------------------------
# TWEET GORSEL ONIZLEME — Admin panelde gorsel goster
# -------------------------------------------------------

@router.get("/tweet-image/{tweet_id}")
async def tweet_image(
    request: Request,
    tweet_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Tweet görseli serve eder (admin panel önizleme)."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    from app.models.pending_tweet import PendingTweet
    from fastapi.responses import FileResponse

    result = await db.execute(select(PendingTweet).where(PendingTweet.id == tweet_id))
    tweet = result.scalar_one_or_none()

    if not tweet or not tweet.image_path:
        from fastapi.responses import Response
        return Response(status_code=404)

    image_path = tweet.image_path

    if not os.path.exists(image_path):
        from fastapi.responses import Response
        return Response(status_code=404)

    # MIME type belirle
    ext = os.path.splitext(image_path)[1].lower()
    media_types = {".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg"}
    media_type = media_types.get(ext, "image/png")

    return FileResponse(image_path, media_type=media_type)


# -------------------------------------------------------
# BROADCAST — Toplu Bildirim Gonderimi
# -------------------------------------------------------

@router.get("/broadcast", response_class=HTMLResponse)
async def broadcast_page(
    request: Request,
    success: Optional[str] = None,
    error: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """Broadcast bildirim gonderim sayfasi."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    from app.services.broadcast import can_broadcast

    can_send, cooldown_remaining = can_broadcast()

    return templates.TemplateResponse("admin/broadcast.html", {
        "request": request,
        "success": success,
        "error": error,
        "can_send": can_send,
        "cooldown_remaining": cooldown_remaining,
    })


@router.post("/broadcast/preview")
async def broadcast_preview(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Broadcast onizleme — hedef kitle sayisini dondurur (AJAX)."""
    if not get_current_admin(request):
        from fastapi.responses import JSONResponse
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    form = await request.form()
    audience = form.get("audience", "all")

    from app.services.broadcast import count_recipients
    count = await count_recipients(db, audience)

    from fastapi.responses import JSONResponse
    return JSONResponse({"count": count, "audience": audience})


@router.post("/broadcast/send")
async def broadcast_send(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Broadcast bildirim gonder — SENKRON (request icinde).

    Background task sorunlari (event loop blocking, task GC, session)
    yuzunden dogrudan request icinde gonderim yapar.
    18 kullanici icin ~40 saniye surer (2sn throttle).
    """
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    form = await request.form()
    title = (form.get("title") or "").strip()
    body = (form.get("body") or "").strip()
    audience = form.get("audience", "all")
    deep_link_target = form.get("deep_link_target", "none")

    # Validasyon
    if not title or len(title) > 100:
        return RedirectResponse(
            url="/admin/broadcast?error=Baslik 1-100 karakter olmali",
            status_code=303,
        )
    if not body or len(body) > 500:
        return RedirectResponse(
            url="/admin/broadcast?error=Mesaj 1-500 karakter olmali",
            status_code=303,
        )
    if audience not in ("all", "paid", "free"):
        return RedirectResponse(
            url="/admin/broadcast?error=Gecersiz hedef kitle",
            status_code=303,
        )
    if deep_link_target not in ("none", "halka-arz", "ai-haberler", "ayarlar"):
        deep_link_target = "none"

    from app.services.broadcast import can_broadcast, mark_broadcast_sent, count_recipients

    can_send, cooldown_remaining = can_broadcast()
    if not can_send:
        return RedirectResponse(
            url=f"/admin/broadcast?error=Rate limit aktif — {cooldown_remaining} saniye bekleyin",
            status_code=303,
        )

    # Cooldown baslat
    mark_broadcast_sent()

    # --- SENKRON GONDERIM (request icinde) ---
    import asyncio

    from app.services.notification import _init_firebase, is_firebase_initialized, NotificationService
    _init_firebase()

    if not is_firebase_initialized():
        return RedirectResponse(
            url="/admin/broadcast?error=Firebase baslatılamadı — bildirim gonderilemez",
            status_code=303,
        )

    from app.services.broadcast import _get_target_users
    users = await _get_target_users(db, audience)
    total = len(users)

    if total == 0:
        return RedirectResponse(
            url="/admin/broadcast?error=Hedef kitle bos — bildirim gonderilecek kullanici yok",
            status_code=303,
        )

    safe_data = {
        "type": "announcement",
        "target": str(deep_link_target),
    }

    sent = 0
    failed = 0
    error_details: list[str] = []

    notif_service = NotificationService(db)

    for user in users:
        fcm = (user.fcm_token or "").strip()
        expo = (user.expo_push_token or "").strip()
        if not fcm and not expo:
            failed += 1
            error_details.append(f"User {user.id}: token bos")
            continue

        try:
            success = await notif_service._send_to_user(
                user=user,
                title=title,
                body=body,
                data=safe_data,
                channel_id="default_v2",
                delay=False,
            )
            if success:
                sent += 1
                logger.info("Broadcast: User %d OK", user.id)
            else:
                failed += 1
                error_details.append(f"User {user.id}: gonderim basarisiz")

            # Throttle
            await asyncio.sleep(1)

        except Exception as e:
            error_name = type(e).__name__
            error_msg = str(e)[:120]
            failed += 1
            error_details.append(f"User {user.id} ({error_name}): {error_msg}")
            logger.warning(
                "Broadcast: User %d FAILED (%s): %s",
                user.id, error_name, error_msg,
            )

    # Telegram rapor
    from app.services.broadcast import _send_telegram_report
    try:
        await _send_telegram_report(
            title, audience, deep_link_target, total, sent, failed, error_details,
        )
    except Exception:
        pass

    if sent > 0:
        return RedirectResponse(
            url=f"/admin/broadcast?success=Broadcast tamamlandi! {sent}/{total} basarili, {failed} basarisiz.",
            status_code=303,
        )
    else:
        err_summary = "; ".join(error_details[:3]) if error_details else "Bilinmeyen hata"
        return RedirectResponse(
            url=f"/admin/broadcast?error=Broadcast basarisiz: 0/{total}. Hata: {err_summary[:200]}",
            status_code=303,
        )


# -------------------------------------------------------
# DEBUG: FCM Token Durumu
# -------------------------------------------------------

@router.get("/debug/tokens")
async def debug_tokens(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Kullanicilarin FCM token durumlarini kontrol et (JSON)."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    from app.models.user import User

    result = await db.execute(
        select(User).where(
            and_(
                User.notifications_enabled == True,
                User.deleted == False,
            )
        )
    )
    users = list(result.scalars().all())

    user_data = []
    for u in users:
        fcm = (u.fcm_token or "").strip()
        expo = (u.expo_push_token or "").strip()
        user_data.append({
            "id": u.id,
            "device_id": u.device_id[:12] + "..." if u.device_id else None,
            "platform": u.platform,
            "fcm_token": f"{fcm[:20]}...{fcm[-8:]}" if len(fcm) > 30 else fcm,
            "fcm_len": len(fcm),
            "fcm_prefix": fcm[:20] if fcm else None,
            "expo_token": expo[:30] + "..." if len(expo) > 30 else expo,
            "expo_len": len(expo),
            "notifications_enabled": u.notifications_enabled,
            "app_version": u.app_version,
        })

    from starlette.responses import JSONResponse
    return JSONResponse({
        "total_users": len(users),
        "with_fcm": sum(1 for u in user_data if u["fcm_len"] > 0),
        "with_expo": sum(1 for u in user_data if u["expo_len"] > 0),
        "users": user_data,
    })


@router.post("/debug/test-push/{user_id}")
async def debug_test_push(
    request: Request,
    user_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Belirli bir kullaniciya test bildirimi gonder (debug). FCM veya Expo token kullanir."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    from app.models.user import User
    from app.services.notification import NotificationService

    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        from starlette.responses import JSONResponse
        return JSONResponse({"error": "Kullanici bulunamadi"}, status_code=404)

    fcm = (user.fcm_token or "").strip()
    expo = (user.expo_push_token or "").strip()
    if not fcm and not expo:
        from starlette.responses import JSONResponse
        return JSONResponse({
            "error": "FCM ve Expo token bos — bildirim gonderilemez",
            "user_id": user.id,
        })

    notif_service = NotificationService(db)

    try:
        success = await notif_service._send_to_user(
            user=user,
            title="Test Bildirimi",
            body="Bu bir admin debug test bildirimidir.",
            data={"type": "test"},
            channel_id="default_v2",
            delay=False,
        )
        method = "FCM" if fcm else "Expo"
        from starlette.responses import JSONResponse
        return JSONResponse({
            "success": success,
            "user_id": user.id,
            "method": method,
            "token_prefix": (fcm or expo)[:20],
        })
    except Exception as e:
        from starlette.responses import JSONResponse
        return JSONResponse({
            "success": False,
            "user_id": user.id,
            "error": f"{type(e).__name__}: {str(e)[:200]}",
            "token_prefix": (fcm or expo)[:20],
        })


# -------------------------------------------------------
# KUPON YONETIMI
# -------------------------------------------------------

def _generate_coupon_code() -> str:
    """SZ + 6 random alfanumerik (buyuk harf + rakam) = SZAB12XY."""
    chars = string.ascii_uppercase + string.digits
    return "SZ" + "".join(secrets.choice(chars) for _ in range(6))


@router.get("/coupons", response_class=HTMLResponse)
async def admin_coupons(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Kupon yonetim sayfasi — listele + olustur formu."""
    admin = get_current_admin(request)
    if not admin:
        return RedirectResponse("/admin/login", status_code=302)

    result = await db.execute(
        select(Coupon).order_by(desc(Coupon.created_at))
    )
    coupons = result.scalars().all()

    # Durum hesapla
    now = datetime.now(timezone.utc)
    coupon_list = []
    for c in coupons:
        if not c.is_active:
            status = "deaktif"
        elif c.expires_at and c.expires_at < now:
            status = "suresi_dolmus"
        elif c.uses_count >= c.max_uses:
            status = "tukendi"
        else:
            status = "aktif"
        coupon_list.append({"coupon": c, "status": status})

    return templates.TemplateResponse("admin/coupons.html", {
        "request": request,
        "coupon_list": coupon_list,
        "success": request.query_params.get("success"),
        "error": request.query_params.get("error"),
    })


@router.post("/coupons/create")
async def admin_create_coupon(
    request: Request,
    amount: float = Form(...),
    max_uses: int = Form(1),
    expires_at: Optional[str] = Form(None),
    db: AsyncSession = Depends(get_db),
):
    """Yeni kupon olustur — SZ-prefix unique kod uret."""
    admin = get_current_admin(request)
    if not admin:
        return RedirectResponse("/admin/login", status_code=302)

    if amount <= 0 or amount > 50000:
        return RedirectResponse("/admin/coupons?error=Puan+miktari+0-50000+arasinda+olmali", status_code=302)
    if max_uses < 1 or max_uses > 10000:
        return RedirectResponse("/admin/coupons?error=Kullanim+limiti+1-10000+arasinda+olmali", status_code=302)

    # SKT parse
    expire_dt = None
    if expires_at and expires_at.strip():
        try:
            expire_dt = datetime.strptime(expires_at.strip(), "%Y-%m-%d").replace(
                hour=23, minute=59, second=59, tzinfo=timezone.utc
            )
        except ValueError:
            return RedirectResponse("/admin/coupons?error=Gecersiz+tarih+formati", status_code=302)

    # Unique kod uret (collision check)
    for _ in range(10):
        code = _generate_coupon_code()
        existing = await db.execute(
            select(Coupon).where(Coupon.code == code)
        )
        if not existing.scalar_one_or_none():
            break
    else:
        return RedirectResponse("/admin/coupons?error=Kod+uretilemedi+tekrar+deneyin", status_code=302)

    coupon = Coupon(
        code=code,
        amount=amount,
        max_uses=max_uses,
        uses_count=0,
        expires_at=expire_dt,
        is_active=True,
    )
    db.add(coupon)
    await db.flush()

    return RedirectResponse(f"/admin/coupons?success=Kupon+olusturuldu:+{code}", status_code=302)


@router.post("/coupons/{coupon_id}/delete")
async def admin_delete_coupon(
    request: Request,
    coupon_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Kuponu deaktive et (soft delete)."""
    admin = get_current_admin(request)
    if not admin:
        return RedirectResponse("/admin/login", status_code=302)

    result = await db.execute(
        select(Coupon).where(Coupon.id == coupon_id)
    )
    coupon = result.scalar_one_or_none()
    if not coupon:
        return RedirectResponse("/admin/coupons?error=Kupon+bulunamadi", status_code=302)

    coupon.is_active = False
    await db.flush()

    return RedirectResponse(f"/admin/coupons?success=Kupon+{coupon.code}+deaktive+edildi", status_code=302)


# -------------------------------------------------------
# REPLY MODULE — AI destekli tweet reply
# -------------------------------------------------------

@router.get("/replies", response_class=HTMLResponse)
async def admin_replies_page(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """AI Reply modülü sayfası — manuel + otomatik + hedefler."""
    admin = get_current_admin(request)
    if not admin:
        return RedirectResponse("/admin/login", status_code=302)

    # Takip edilen hesaplar
    targets_result = await db.execute(
        select(ReplyTarget).order_by(desc(ReplyTarget.created_at))
    )
    targets = targets_result.scalars().all()

    # Son 50 otomatik reply logu
    log_result = await db.execute(
        select(AutoReply).order_by(desc(AutoReply.created_at)).limit(50)
    )
    reply_log = log_result.scalars().all()

    # Auto-reply toggle durumu
    from app.models.app_setting import AppSetting
    toggle_result = await db.execute(
        select(AppSetting).where(AppSetting.key == "AUTO_REPLY_ENABLED")
    )
    toggle_setting = toggle_result.scalar_one_or_none()
    auto_reply_on = True  # Default: açık
    if toggle_setting:
        auto_reply_on = toggle_setting.value.lower() in ("true", "1", "yes")

    # Mentions auto-reply toggle durumu
    mentions_result = await db.execute(
        select(AppSetting).where(AppSetting.key == "MENTIONS_REPLY_ENABLED")
    )
    mentions_setting = mentions_result.scalar_one_or_none()
    mentions_reply_on = False  # Default: kapalı
    if mentions_setting:
        mentions_reply_on = mentions_setting.value.lower() in ("true", "1", "yes")

    # Bugün kaç reply atıldı
    from sqlalchemy import func as sa_func_inner
    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    count_result = await db.execute(
        select(sa_func.count(AutoReply.id)).where(
            AutoReply.status == "replied",
            AutoReply.created_at >= today_start,
        )
    )
    today_count = count_result.scalar() or 0

    # Günlük limit (DB'den okur, default 20)
    limit_result = await db.execute(
        select(AppSetting).where(AppSetting.key == "AUTO_REPLY_DAILY_LIMIT")
    )
    limit_setting = limit_result.scalar_one_or_none()
    daily_limit = int(limit_setting.value) if limit_setting else 20

    return templates.TemplateResponse("admin/replies.html", {
        "request": request,
        "targets": targets,
        "reply_log": reply_log,
        "auto_reply_on": auto_reply_on,
        "mentions_reply_on": mentions_reply_on,
        "today_count": today_count,
        "daily_limit": daily_limit,
        "success": request.query_params.get("success"),
        "error": request.query_params.get("error"),
    })


@router.post("/replies/generate")
async def admin_replies_generate(request: Request):
    """Tweet URL al → tweet çek + AI reply üret → JSON dön."""
    from fastapi.responses import JSONResponse
    from app.services.twitter_reply_service import (
        fetch_tweet_by_url,
        generate_reply_suggestions,
    )

    admin = get_current_admin(request)
    if not admin:
        return JSONResponse({"error": "Yetkisiz erişim"}, status_code=401)

    form = await request.form()
    tweet_url = form.get("tweet_url", "").strip()

    if not tweet_url:
        return JSONResponse({"error": "Tweet URL'si gerekli."}, status_code=400)

    # 1. Tweet'i çek
    tweet_result = await fetch_tweet_by_url(tweet_url)
    if not tweet_result.get("success"):
        return JSONResponse({
            "error": tweet_result.get("error", "Tweet çekilemedi.")
        }, status_code=400)

    # 2. AI reply önerisi üret
    ai_result = await generate_reply_suggestions(tweet_result["text"])
    if not ai_result.get("success"):
        return JSONResponse({
            "error": ai_result.get("error", "AI reply üretilemedi.")
        }, status_code=500)

    return JSONResponse({
        "tweet": {
            "id": tweet_result["tweet_id"],
            "text": tweet_result["text"],
            "author_username": tweet_result["author_username"],
            "author_name": tweet_result["author_name"],
            "likes": tweet_result["likes"],
            "retweets": tweet_result["retweets"],
        },
        "ai_result": {
            "is_safe": ai_result["is_safe"],
            "reason": ai_result.get("reason", ""),
            "replies": ai_result.get("replies", []),
        },
    })


@router.post("/replies/send")
async def admin_replies_send(request: Request):
    """Onaylanan reply'ı X'te yayınla."""
    from fastapi.responses import JSONResponse
    from app.services.twitter_reply_service import send_reply

    admin = get_current_admin(request)
    if not admin:
        return JSONResponse({"error": "Yetkisiz erişim"}, status_code=401)

    form = await request.form()
    tweet_id = form.get("tweet_id", "").strip()
    reply_text = form.get("reply_text", "").strip()

    if not tweet_id or not reply_text:
        return JSONResponse({
            "error": "Tweet ID ve reply metni gerekli."
        }, status_code=400)

    result = await send_reply(tweet_id, reply_text)

    if result.get("success"):
        return JSONResponse({
            "success": True,
            "reply_tweet_id": result["reply_tweet_id"],
        })
    else:
        return JSONResponse({
            "success": False,
            "error": result.get("error", "Reply gönderilemedi."),
        }, status_code=500)


# -------------------------------------------------------
# QUOTE TWEET + ANALİZ — Manuel alıntı tweet özelliği
# -------------------------------------------------------

@router.post("/replies/quote-analyze")
async def admin_quote_analyze(request: Request):
    """Tweet URL'sinden 2 farklı AI quote analizi üretir."""
    from fastapi.responses import JSONResponse
    from app.services.twitter_reply_service import fetch_tweet_by_url, generate_quote_analysis

    admin = get_current_admin(request)
    if not admin:
        return JSONResponse({"error": "Yetkisiz erişim"}, status_code=401)

    form = await request.form()
    tweet_url = form.get("tweet_url", "").strip()

    if not tweet_url:
        return JSONResponse({"error": "Tweet URL'si gerekli."}, status_code=400)

    # Tweet'i çek
    tweet_result = await fetch_tweet_by_url(tweet_url)
    if not tweet_result.get("success"):
        return JSONResponse({
            "error": tweet_result.get("error", "Tweet çekilemedi.")
        }, status_code=400)

    # AI analiz üret (2 seçenek)
    ai_result = await generate_quote_analysis(
        tweet_result["text"],
        tweet_result["author_username"],
    )
    if not ai_result.get("success"):
        return JSONResponse({
            "error": ai_result.get("error", "AI analiz üretilemedi.")
        }, status_code=500)

    return JSONResponse({
        "tweet": {
            "id": tweet_result["tweet_id"],
            "url": tweet_url,
            "text": tweet_result["text"],
            "author_username": tweet_result["author_username"],
            "author_name": tweet_result["author_name"],
            "likes": tweet_result["likes"],
            "retweets": tweet_result["retweets"],
        },
        "is_safe": ai_result["is_safe"],
        "analyses": ai_result.get("analyses", []),
    })


@router.post("/replies/send-quote")
async def admin_send_quote(request: Request):
    """Seçilen AI analizini quote tweet olarak yayınlar."""
    from fastapi.responses import JSONResponse
    from app.services.twitter_reply_service import send_quote_analysis_tweet

    admin = get_current_admin(request)
    if not admin:
        return JSONResponse({"error": "Yetkisiz erişim"}, status_code=401)

    form = await request.form()
    tweet_url = form.get("tweet_url", "").strip()
    analysis_text = form.get("analysis_text", "").strip()

    if not tweet_url or not analysis_text:
        return JSONResponse({
            "error": "Tweet URL ve analiz metni gerekli."
        }, status_code=400)

    result = await send_quote_analysis_tweet(tweet_url, analysis_text)

    if result.get("success"):
        return JSONResponse({
            "success": True,
            "quote_tweet_id": result["quote_tweet_id"],
            "tweet_url": result["tweet_url"],
        })
    else:
        return JSONResponse({
            "success": False,
            "error": result.get("error", "Quote tweet gönderilemedi."),
        }, status_code=500)


# -------------------------------------------------------
# REPLY DEBUG — Son hata mesajlarini goster
# -------------------------------------------------------

@router.get("/replies/debug")
async def admin_replies_debug(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Son 20 reply kaydinin TAM hata mesajlarini dondurur (JSON)."""
    from fastapi.responses import JSONResponse
    from app.models.auto_reply import AutoReply

    result = await db.execute(
        select(AutoReply).order_by(desc(AutoReply.created_at)).limit(20)
    )
    entries = result.scalars().all()

    data = []
    for e in entries:
        data.append({
            "id": e.id,
            "target_username": e.target_username,
            "status": e.status,
            "error_message": e.error_message,
            "created_at": e.created_at.isoformat() if e.created_at else None,
            "target_tweet_id": e.target_tweet_id,
            "reply_text": (e.reply_text or "")[:60],
        })

    return JSONResponse(data)


# -------------------------------------------------------
# REPLY TARGETS — Hesap Ekleme/Silme + Toggle
# -------------------------------------------------------

@router.post("/replies/targets/add")
async def admin_reply_target_add(
    request: Request,
    username: str = Form(...),
    db: AsyncSession = Depends(get_db),
):
    """Yeni reply hedefi ekle."""
    admin = get_current_admin(request)
    if not admin:
        return RedirectResponse("/admin/login", status_code=302)

    # @ işaretini temizle
    clean_username = username.strip().lstrip("@")
    if not clean_username:
        return RedirectResponse("/admin/replies?error=Kullanici+adi+bos", status_code=302)

    # Zaten var mı?
    existing = await db.execute(
        select(ReplyTarget).where(ReplyTarget.username == clean_username)
    )
    if existing.scalar_one_or_none():
        return RedirectResponse(
            f"/admin/replies?error=@{clean_username}+zaten+mevcut", status_code=302
        )

    db.add(ReplyTarget(username=clean_username, is_active=True))
    await db.flush()

    return RedirectResponse(
        f"/admin/replies?success=@{clean_username}+eklendi", status_code=302
    )


@router.post("/replies/targets/{target_id}/delete")
async def admin_reply_target_delete(
    request: Request,
    target_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Reply hedefi sil (DB'den kaldir)."""
    admin = get_current_admin(request)
    if not admin:
        return RedirectResponse("/admin/login", status_code=302)

    result = await db.execute(
        select(ReplyTarget).where(ReplyTarget.id == target_id)
    )
    target = result.scalar_one_or_none()
    if not target:
        return RedirectResponse("/admin/replies?error=Hedef+bulunamadi", status_code=302)

    username = target.username
    await db.delete(target)
    await db.flush()

    return RedirectResponse(
        f"/admin/replies?success=@{username}+silindi", status_code=302
    )


@router.post("/replies/toggle-auto")
async def admin_reply_toggle_auto(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Otomatik reply toggle (açık/kapalı)."""
    from app.models.app_setting import AppSetting

    admin = get_current_admin(request)
    if not admin:
        return RedirectResponse("/admin/login", status_code=302)

    # Mevcut durumu kontrol et
    result = await db.execute(
        select(AppSetting).where(AppSetting.key == "AUTO_REPLY_ENABLED")
    )
    setting = result.scalar_one_or_none()

    if setting:
        current = setting.value.lower() in ("true", "1", "yes")
        setting.value = "false" if current else "true"
        new_state = "kapalı" if current else "açık"
    else:
        # İlk kez — default açık, toggle kapalıya çevir
        db.add(AppSetting(key="AUTO_REPLY_ENABLED", value="false"))
        new_state = "kapalı"

    await db.flush()

    return RedirectResponse(
        f"/admin/replies?success=Otomatik+reply+{new_state}", status_code=302
    )


@router.post("/replies/toggle-mentions")
async def admin_toggle_mentions_reply(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Mentions auto-reply sistemini aç/kapat."""
    from app.models.app_setting import AppSetting

    admin = get_current_admin(request)
    if not admin:
        return RedirectResponse("/admin/login", status_code=302)

    result = await db.execute(
        select(AppSetting).where(AppSetting.key == "MENTIONS_REPLY_ENABLED")
    )
    setting = result.scalar_one_or_none()

    if setting:
        current = setting.value.lower() in ("true", "1", "yes")
        setting.value = "false" if current else "true"
        new_state = "kapalı" if current else "açık"
    else:
        db.add(AppSetting(key="MENTIONS_REPLY_ENABLED", value="true"))
        new_state = "açık"

    await db.flush()

    return RedirectResponse(
        f"/admin/replies?success=Mentions+reply+{new_state}", status_code=302
    )


@router.post("/replies/set-limit")
async def admin_reply_set_limit(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    """Günlük reply limitini ayarla (admin panelden)."""
    from app.models.app_setting import AppSetting

    admin = get_current_admin(request)
    if not admin:
        return RedirectResponse("/admin/login", status_code=302)

    form = await request.form()
    new_limit = form.get("daily_limit", "20")
    try:
        limit_val = max(1, min(100, int(new_limit)))  # 1-100 arası
    except (ValueError, TypeError):
        limit_val = 20

    result = await db.execute(
        select(AppSetting).where(AppSetting.key == "AUTO_REPLY_DAILY_LIMIT")
    )
    setting = result.scalar_one_or_none()

    if setting:
        setting.value = str(limit_val)
    else:
        db.add(AppSetting(key="AUTO_REPLY_DAILY_LIMIT", value=str(limit_val)))

    await db.flush()

    return RedirectResponse(
        f"/admin/replies?success=Günlük+limit+{limit_val}+yapıldı", status_code=302
    )
