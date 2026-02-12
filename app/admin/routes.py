"""Admin panel route'lari — IPO CRUD + Dagitim Sonuclari + SPK Yonetimi."""

import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

from fastapi import APIRouter, Request, Depends, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, desc, func as sa_func
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

    # Islem tarihine gore: en yakin tarih en ustte, belli olmayanlar en sonda
    query = select(IPO).order_by(
        IPO.trading_start.is_(None),  # tarihi belli olmayanlar en sona
        desc(IPO.trading_start),      # en yakin islem tarihi en ustte
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
            subscription_hours=form.get("subscription_hours", "").strip() or None,
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
        ipo.subscription_hours = form.get("subscription_hours", "").strip() or None
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

        ipo.updated_at = datetime.utcnow()

        await db.flush()
        logger.info(f"Admin: IPO guncellendi — {ipo.company_name} (ID: {ipo.id})")
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

    logger.info(f"Admin: IPO siliniyor — {ipo.company_name} (ID: {ipo.id})")
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
        select(SPKApplication).order_by(desc(SPKApplication.created_at))
    )
    applications = list(result.scalars().all())

    return templates.TemplateResponse("admin/spk_list.html", {
        "request": request,
        "applications": applications,
        "success": "SPK basvurusu guncellendi!" if success == "updated" else None,
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
