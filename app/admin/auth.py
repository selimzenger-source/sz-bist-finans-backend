"""Admin kimlik dogrulama — basit session-cookie tabanli."""

import hmac
import secrets
from functools import wraps
from typing import Optional

from fastapi import Request, HTTPException
from fastapi.responses import RedirectResponse

from app.config import get_settings

settings = get_settings()

# Bellekte tutulan aktif session token'lari
_active_sessions: set[str] = set()

SESSION_COOKIE_NAME = "admin_session"


def verify_password(password: str) -> bool:
    """Admin sifresini dogrular — timing-safe karsilastirma."""
    admin_pw = settings.ADMIN_PASSWORD
    if not admin_pw or not password:
        return False
    return hmac.compare_digest(password.encode("utf-8"), admin_pw.encode("utf-8"))


def create_session() -> str:
    """Yeni session token olusturur."""
    token = secrets.token_urlsafe(32)
    _active_sessions.add(token)
    return token


def validate_session(token: Optional[str]) -> bool:
    """Session token'in gecerli olup olmadigini kontrol eder."""
    if not token:
        return False
    return token in _active_sessions


def destroy_session(token: str):
    """Session'i sonlandirir."""
    _active_sessions.discard(token)


def get_current_admin(request: Request) -> bool:
    """Request'ten admin oturumunu dogrular. False donerse login'e yonlendir."""
    token = request.cookies.get(SESSION_COOKIE_NAME)
    return validate_session(token)


def require_admin(request: Request):
    """Admin oturumu yoksa RedirectResponse dondurur."""
    if not get_current_admin(request):
        raise HTTPException(status_code=303, headers={"Location": "/admin/login"})
