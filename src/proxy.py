"""Single proxy configuration for all outbound traffic.

Set ``PROXY_URL`` env to ``http://user:pass@host:port`` (scheme optional,
defaults to http). Empty/missing = direct connection.

Used by:
- ``src/http.py`` — pooled httpx client (parser, chat API, accounts/check,
  translate);
- Playwright ``browser.new_context(proxy=...)`` in ``chat.py``
  (send_with_photos), ``sender.py`` and ``inbox.py`` (phase-2) workers.
"""
import os
from urllib.parse import urlparse, unquote


def get_proxy_url() -> str:
    """Raw proxy URL from env ('' = no proxy). Never logged verbatim."""
    return os.environ.get("PROXY_URL", "").strip()


def _parsed():
    url = get_proxy_url()
    if not url:
        return None
    if "://" not in url:
        url = "http://" + url
    try:
        p = urlparse(url)
    except ValueError:
        return None
    if not p.hostname or not p.port:
        return None
    return p


def playwright_proxy() -> dict | None:
    """Proxy dict for Playwright ``new_context(proxy=...)`` or None."""
    p = _parsed()
    if p is None:
        return None
    proxy = {"server": f"{p.scheme or 'http'}://{p.hostname}:{p.port}"}
    if p.username:
        proxy["username"] = unquote(p.username)
    if p.password:
        proxy["password"] = unquote(p.password)
    return proxy


def proxy_host_for_log() -> str:
    """Proxy host (no credentials) for log lines, '' when direct."""
    p = _parsed()
    return f"{p.hostname}:{p.port}" if p else ""


def is_ip_blocked(resp) -> bool:
    """True when the egress IP is WAF-blocked (HTTP 403 on any page).

    nebenan.de answers 403 with the original URL (no /login redirect), so
    URL-based session checks pass while the page contains no forms.
    Never raises; None response = not blocked.
    """
    try:
        return resp is not None and getattr(resp, "status", 0) == 403
    except Exception:
        return False


async def ensure_logged_in(page, account_name: str = "?", where: str = "") -> bool:
    """Fail fast when nebenan.de redirected to /login (dead session cookies).

    Returns True when the page looks authenticated, False otherwise (with
    a clear log line telling the operator to refresh cookies).
    """
    import logging as _logging

    url = page.url or ""
    if "/login" in url:
        _logging.getLogger("nebena").warning(
            f"[{account_name}] сессия истекла (редирект на /login{where}) — "
            f"обновите cookies: python save_cookies.py"
        )
        return False
    # Fail-open positive signal: the `s` session cookie should exist when we
    # are logged in. A missing cookie on a non-login URL is contradictory, so
    # log it for diagnostics — but NEVER fail on it (cookie edge cases must
    # not block a working session).
    try:
        cookies = await page.context.cookies()
        if not any(c.get("name") == "s" and "nebenan" in c.get("domain", "")
                   for c in cookies):
            _logging.getLogger("nebena").debug(
                f"[{account_name}] нет cookie 's'{where} — сессия может быть несвежей"
            )
    except Exception:
        pass
    return True
