"""Shared async HTTP client (httpx) for nebenan.de API calls.

Replaces per-call Playwright ``request.new_context()``: one pooled
``AsyncClient`` per process — no Chromium-driver startup cost, connection
reuse, sane default timeouts. Playwright stays only for real browser flows
(send_with_photos, sender/inbox workers).

Optional health recorder: ``set_health_recorder(callable)`` receives
``(account_name: str, method: str, url: str, status: int, body: str)``
on every response (2xx included, so account health can heal back to ok).
"""
import logging
import typing

import httpx

from src.proxy import get_proxy_url, proxy_host_for_log

log = logging.getLogger("nebena")

_TIMEOUT = httpx.Timeout(15.0, connect=10.0)

_client: httpx.AsyncClient | None = None

# Health recorder: set by webapi at startup. (account, method, url, status, body)
_health_recorder: typing.Callable[[str, str, str, int, str], typing.Any] | None = None


def set_health_recorder(
    cb: typing.Callable[[str, str, str, int, str], typing.Any],
) -> None:
    """Register a callback invoked on every non-2xx API response."""
    global _health_recorder
    _health_recorder = cb


def _record(account_name: str, method: str, url: str, status: int, body: str) -> None:
    if _health_recorder is not None:
        try:
            _health_recorder(account_name, method, url, status, body)
        except Exception:
            pass


def _make_client(proxy_url: str | None) -> httpx.AsyncClient:
    # `proxies=` (plural) was removed in httpx 0.28 in favour of `proxy=`.
    # Try the modern spelling first, fall back to the pinned-0.27 one.
    try:
        return httpx.AsyncClient(timeout=_TIMEOUT, proxy=proxy_url)
    except TypeError:
        return httpx.AsyncClient(timeout=_TIMEOUT, proxies=proxy_url)


def get_client() -> httpx.AsyncClient:
    """Process-wide pooled client (lazy init, re-created if closed).

    Picks up PROXY_URL at creation; restart the process to change proxy.
    """
    global _client
    if _client is None or _client.is_closed:
        proxy_url = get_proxy_url() or None
        _client = _make_client(proxy_url)
        host = proxy_host_for_log()
        log.info(f"HTTP client init (proxy: {host or 'direct'})")
    return _client


async def close_client() -> None:
    """Close the shared client (call on app shutdown)."""
    global _client
    if _client is not None and not _client.is_closed:
        try:
            await _client.aclose()
        except Exception:
            pass
    _client = None


async def api_request(
    method: str,
    url: str,
    token: str,
    body: dict | None = None,
    extra_headers: dict | None = None,
    account_name: str = "",
) -> dict | list | None:
    """GET/POST JSON to the nebenan API. Returns parsed JSON or None.

    On non-2xx the full response text is forwarded to the health recorder
    (if registered) so ban/expiry detection can work off the raw body.
    """
    if not token:
        log.warning(f"API {method} {url} → skipped (no token)")
        return None
    headers = {"x-auth-token": token, **(extra_headers or {})}
    try:
        client = get_client()
        if method == "GET":
            r = await client.get(url, headers=headers)
        else:
            r = await client.post(url, headers=headers, json=body)
    except httpx.TimeoutException:
        log.warning(f"API {method} {url} → timeout")
        _record(account_name, method, url, 0, "timeout")
        return None
    except httpx.HTTPError as e:
        log.warning(f"API {method} {url} → network error ({e})")
        _record(account_name, method, url, 0, "network_error")
        return None
    if r.status_code in (200, 201):
        _record(account_name, method, url, r.status_code, "")
        try:
            return r.json()
        except Exception as e:
            log.warning(f"API {method} {url} → {r.status_code}: bad JSON ({e})")
            return None
    log.warning(f"API {method} {url} → {r.status_code}: {r.text[:200]}")
    _record(account_name, method, url, r.status_code, r.text)
    return None


async def api_request_ex(
    method: str,
    url: str,
    token: str,
    account_name: str = "",
    body: dict | None = None,
    extra_headers: dict | None = None,
) -> tuple[dict | list | None, int, str]:
    """Like :func:`api_request` but returns ``(data, status_code, raw_text)``.

    ``status_code`` is 0 for network/timeout errors. ``raw_text`` is the
    truncated server body (or ``""`` on network failure).
    """
    if not token:
        log.warning(f"API {method} {url} → skipped (no token)")
        _record(account_name, method, url, 0, "no_token")
        return None, 0, ""
    headers = {"x-auth-token": token, **(extra_headers or {})}
    try:
        client = get_client()
        if method == "GET":
            r = await client.get(url, headers=headers)
        else:
            r = await client.post(url, headers=headers, json=body)
    except httpx.TimeoutException:
        log.warning(f"API {method} {url} → timeout")
        _record(account_name, method, url, 0, "timeout")
        return None, 0, "timeout"
    except httpx.HTTPError as e:
        log.warning(f"API {method} {url} → network error ({e})")
        _record(account_name, method, url, 0, "network_error")
        return None, 0, "network_error"
    text = r.text
    if r.status_code in (200, 201):
        _record(account_name, method, url, r.status_code, "")
        try:
            return r.json(), r.status_code, text
        except Exception as e:
            log.warning(f"API {method} {url} → {r.status_code}: bad JSON ({e})")
            return None, r.status_code, text
    log.warning(f"API {method} {url} → {r.status_code}: {text[:200]}")
    _record(account_name, method, url, r.status_code, text)
    return None, r.status_code, text
