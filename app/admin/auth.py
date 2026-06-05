"""Admin kimlik dogrulama — STATELESS imzali cookie tabanli.

ESKI: aktif session token'lari BELLEKTE (set) tutuluyordu → sunucu her restart'ta
(512MB OOM ile sik sik) bu set siliniyor, admin aninda logout oluyor, surekli sifre
isteniyordu. ARTIK: token imzali (HMAC) ve kendi son-kullanma tarihini tasiyor;
sunucuda hicbir sey saklanmaz → restart'tan ETKILENMEZ, admin 30 gun girili kalir.
"""

import hashlib
import hmac
import time
from typing import Optional

from fastapi import Request, HTTPException

from app.config import get_settings

settings = get_settings()

SESSION_COOKIE_NAME = "admin_session"
SESSION_TTL_SECONDS = 60 * 60 * 24 * 30  # 30 gun


def _secret() -> bytes:
    """HMAC imza anahtari — ADMIN_PASSWORD'dan turetilir (paylasilan gizli).

    Sifre degisirse eski tum oturumlar otomatik gecersiz olur (guvenli yan etki).
    """
    base = f"{settings.ADMIN_PASSWORD or 'fallback'}::admin-session-v1"
    return hashlib.sha256(base.encode("utf-8")).digest()


def _sign(payload: str) -> str:
    return hmac.new(_secret(), payload.encode("utf-8"), hashlib.sha256).hexdigest()


def verify_password(password: str) -> bool:
    """Admin sifresini dogrular — timing-safe karsilastirma."""
    admin_pw = settings.ADMIN_PASSWORD
    if not admin_pw or not password:
        return False
    return hmac.compare_digest(password.encode("utf-8"), admin_pw.encode("utf-8"))


def create_session() -> str:
    """Stateless imzali token: '<expiry_ts>.<hmac_sig>'. Sunucuda saklanmaz."""
    exp = int(time.time()) + SESSION_TTL_SECONDS
    return f"{exp}.{_sign(str(exp))}"


def validate_session(token: Optional[str]) -> bool:
    """Imzayi ve son-kullanma tarihini dogrular (sunucu durumundan bagimsiz)."""
    if not token or "." not in token:
        return False
    try:
        exp_str, sig = token.rsplit(".", 1)
        exp = int(exp_str)
    except (ValueError, TypeError):
        return False
    if exp < int(time.time()):
        return False  # suresi dolmus
    return hmac.compare_digest(sig, _sign(exp_str))


def destroy_session(token: str):
    """Stateless — sunucu tarafi kayit yok; logout cookie silinerek yapilir (route)."""
    return None


def get_current_admin(request: Request) -> bool:
    """Request'ten admin oturumunu dogrular. False donerse login'e yonlendir."""
    token = request.cookies.get(SESSION_COOKIE_NAME)
    return validate_session(token)


def require_admin(request: Request):
    """Admin oturumu yoksa RedirectResponse dondurur."""
    if not get_current_admin(request):
        raise HTTPException(status_code=303, headers={"Location": "/admin/login"})
