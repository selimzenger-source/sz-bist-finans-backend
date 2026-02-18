"""Admin panel route'lari — IPO CRUD + Dagitim Sonuclari + SPK Yonetimi."""

import logging
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

from fastapi import APIRouter, Request, Depends, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, desc, and_, func as sa_func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.database import get_db
from app.models.ipo import IPO, IPOAllocation, IPOCeilingTrack
from app.models.spk_application import SPKApplication
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
    deleted_record = DeletedIPO(
        company_name=ipo.company_name,
        ticker=ipo.ticker,
    )
    db.add(deleted_record)

    logger.info(f"Admin: IPO siliniyor — {ipo.company_name} (ID: {ipo.id}) → kara listeye eklendi")
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
    await db.delete(app)
    await db.flush()
    logger.info(f"Admin: SPK basvuru silindi — {company_name} (id={app_id})")

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

@router.post("/run-scraper/halkarz")
async def run_halkarz_scraper(request: Request):
    """HalkArz scraper'ini admin panelden manuel tetikle."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    try:
        from app.scrapers.halkarz_scraper import scrape_halkarz
        await scrape_halkarz()
        return RedirectResponse(url="/admin/?success=HalkArz scraper basariyla calisti!", status_code=303)
    except Exception as e:
        logger.error(f"Admin: HalkArz scraper tetikleme hatasi — {e}")
        return RedirectResponse(url=f"/admin/?success=Scraper hatasi: {str(e)[:80]}", status_code=303)


# -------------------------------------------------------
# TWEET KUYRUGU — Bekleyen Tweetler
# -------------------------------------------------------

@router.get("/tweets", response_class=HTMLResponse)
async def tweets_page(
    request: Request,
    status: str = "pending",
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

    # Auto-send durumu
    from app.config import get_settings
    auto_send = get_settings().TWITTER_AUTO_SEND

    return templates.TemplateResponse("admin/tweets.html", {
        "request": request,
        "tweets": tweets,
        "pending_count": pending_count,
        "current_status": status,
        "auto_send": auto_send,
    })


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

    # Tweet'i gercekten at
    from app.services.twitter_service import _safe_tweet as real_tweet, _safe_tweet_with_media as real_tweet_media
    from app.config import get_settings

    # Gecici olarak auto_send'i True yap (bu tek tweet icin)
    settings = get_settings()
    original_val = settings.TWITTER_AUTO_SEND
    try:
        settings.TWITTER_AUTO_SEND = True

        if tweet.image_path:
            success = real_tweet_media(tweet.text, tweet.image_path, source="admin_approve")
        else:
            success = real_tweet(tweet.text, source="admin_approve")

        if success:
            tweet.status = "sent"
            tweet.sent_at = datetime.now(timezone.utc)
            # Basarili gonderim sonrasi temp gorsel dosyasini temizle
            if tweet.image_path and tweet.image_path.startswith(("/tmp", "C:\\Users")):
                try:
                    import os
                    os.remove(tweet.image_path)
                except OSError:
                    pass
        else:
            tweet.status = "failed"
            tweet.error_message = "Tweet gonderilemedi"
    except Exception as e:
        tweet.status = "failed"
        tweet.error_message = str(e)[:500]
    finally:
        settings.TWITTER_AUTO_SEND = original_val

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


@router.post("/tweets/toggle-auto-send")
async def toggle_auto_send(request: Request):
    """TWITTER_AUTO_SEND toggle — runtime'da degistirir.

    True  → Otomatik mod (tweetler direkt X'e atilir)
    False → Onay modu (tweetler kuyruğa düşer, admin onaylar)

    NOT: Render restart olunca .env'deki değere döner.
    """
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    from app.config import get_settings
    settings = get_settings()
    settings.TWITTER_AUTO_SEND = not settings.TWITTER_AUTO_SEND

    logger.info(
        "[ADMIN] TWITTER_AUTO_SEND -> %s (admin tarafından değiştirildi)",
        settings.TWITTER_AUTO_SEND,
    )

    return RedirectResponse(url="/admin/tweets", status_code=303)


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
    import functools

    from app.services.notification import _init_firebase, is_firebase_initialized
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

    from firebase_admin import messaging

    safe_data = {
        "type": "announcement",
        "target": str(deep_link_target),
    }

    sent = 0
    failed = 0
    error_details: list[str] = []

    for user in users:
        try:
            token = (user.fcm_token or "").strip()
            if not token:
                failed += 1
                error_details.append(f"User {user.id}: token bos")
                continue

            message = messaging.Message(
                notification=messaging.Notification(
                    title=title,
                    body=body,
                ),
                data=safe_data,
                token=token,
                android=messaging.AndroidConfig(
                    priority="high",
                    notification=messaging.AndroidNotification(
                        sound="default",
                        channel_id="default_v2",
                        default_vibrate_timings=True,
                        notification_priority="PRIORITY_MAX",
                        visibility="PUBLIC",
                    ),
                ),
                apns=messaging.APNSConfig(
                    payload=messaging.APNSPayload(
                        aps=messaging.Aps(
                            sound="default",
                            badge=1,
                        ),
                    ),
                ),
            )

            # Thread pool'da calistir — async event loop'u bloke etmesin
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, functools.partial(messaging.send, message)
            )
            sent += 1
            logger.info("Broadcast: User %d OK — %s", user.id, response)

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
    """Belirli bir kullaniciya test bildirimi gonder (debug)."""
    if not get_current_admin(request):
        return RedirectResponse(url="/admin/login", status_code=303)

    from app.models.user import User
    from app.services.notification import _init_firebase, is_firebase_initialized

    result = await db.execute(select(User).where(User.id == user_id))
    user = result.scalar_one_or_none()
    if not user:
        from starlette.responses import JSONResponse
        return JSONResponse({"error": "Kullanici bulunamadi"}, status_code=404)

    _init_firebase()
    if not is_firebase_initialized():
        from starlette.responses import JSONResponse
        return JSONResponse({"error": "Firebase baslatılamadı"}, status_code=500)

    token = (user.fcm_token or "").strip()
    if not token:
        from starlette.responses import JSONResponse
        return JSONResponse({
            "error": "FCM token bos",
            "user_id": user.id,
            "expo_token": (user.expo_push_token or "")[:30],
        })

    from firebase_admin import messaging

    try:
        message = messaging.Message(
            notification=messaging.Notification(
                title="Test Bildirimi",
                body="Bu bir admin debug test bildirimidir.",
            ),
            data={"type": "test"},
            token=token,
            android=messaging.AndroidConfig(
                priority="high",
                notification=messaging.AndroidNotification(
                    sound="default",
                    channel_id="default_v2",
                ),
            ),
        )
        response = messaging.send(message)
        from starlette.responses import JSONResponse
        return JSONResponse({
            "success": True,
            "user_id": user.id,
            "fcm_response": response,
            "token_prefix": token[:20],
        })
    except Exception as e:
        from starlette.responses import JSONResponse
        return JSONResponse({
            "success": False,
            "user_id": user.id,
            "error": f"{type(e).__name__}: {str(e)[:200]}",
            "token_prefix": token[:20],
            "token_len": len(token),
        })
