"""Link generation via Lumma + Shlepock APIs.

Ported from bot2/Combo.py (dual generation flow) to async httpx on the
shared pooled client — PROXY_URL routing applies automatically.

Config comes ONLY from env (no secrets in repo):
  LUMMA_API_DOMAIN, LUMMA_API_KEY, LUMMA_WORKER_ID
  SHLEPOCK_API_DOMAIN, SHLEPOCK_API_KEY, SHLEPOCK_PROFILE_ID

Note: neither API supports nebenan.de sources — Lumma accepts
kleinanzeigen/markt/leboncoin, Shlepock markt2/kleinanzeigen2/vinted/wallapop.
"""
import asyncio
import logging
import os
import re
from urllib.parse import urlparse

import httpx

from src.http import get_client

log = logging.getLogger("nebena")

URL_RE = re.compile(r"^https?://\S+$", re.IGNORECASE)

# Domain substring -> [(api, service_code, label), ...]
DUAL_MAP: dict[str, list[tuple[str, str, str]]] = {
    "markt.de": [
        ("lumma", "markt", "Lumma · Markt"),
        ("shlepock", "markt2_de", "Shlepock · Markt 2.0"),
    ],
    "kleinanzeigen.de": [
        ("lumma", "kleinanzeigen", "Lumma · Kleinanzeigen"),
        ("shlepock", "kleinanzeigen2_de", "Shlepock · Kleinanzeigen 2.0"),
    ],
    "vinted.": [
        ("shlepock", "vinted2_de", "Shlepock · Vinted DE 2.0"),
    ],
    "wallapop.": [
        ("shlepock", "wallapop2_es", "Shlepock · Wallapop 2.0"),
    ],
}

_TIMEOUT = 30.0


def _env(name: str) -> str:
    return os.environ.get(name, "").strip()


def is_configured(api: str) -> bool:
    if api == "lumma":
        return bool(_env("LUMMA_API_DOMAIN") and _env("LUMMA_API_KEY") and _env("LUMMA_WORKER_ID"))
    if api == "shlepock":
        return bool(_env("SHLEPOCK_API_DOMAIN") and _env("SHLEPOCK_API_KEY") and _env("SHLEPOCK_PROFILE_ID"))
    return False


def detect_services(source_url: str) -> list[tuple[str, str, str]]:
    """Map a source URL to [(api, service_code, label)] by domain."""
    try:
        host = (urlparse((source_url or "").strip()).hostname or "").lower()
    except Exception:
        return []
    for domain_part, targets in DUAL_MAP.items():
        if domain_part.endswith("."):
            # TLD-agnostic service key ("vinted." matches www.vinted.fr):
            # the base must appear as a full DNS label.
            if domain_part[:-1] in host.split("."):
                return list(targets)
            continue
        # Full-domain key: exact-or-subdomain match to avoid
        # over-matching evilmarkt.de.evil.com.
        base = domain_part
        if host == base or host.endswith("." + base):
            return list(targets)
    return []


def _normalize_source_url(url: str) -> str:
    """Strip query/fragment on markt.de links (parser chokes on ?geoUrlId=...)."""
    try:
        parts = urlparse(url.strip())
    except Exception:
        return url
    if "markt.de" in parts.netloc.lower():
        return parts._replace(query="", fragment="").geturl()
    return url


async def _post_json(url: str, headers: dict, payload: dict) -> httpx.Response | None:
    try:
        return await get_client().post(url, headers=headers, json=payload, timeout=_TIMEOUT)
    except httpx.TimeoutException:
        log.warning(f"links POST {url} → timeout")
    except httpx.HTTPError as e:
        log.warning(f"links POST {url} → network error ({e})")
    return None


def _http_error(status: int, text: str) -> dict:
    return {"status": "error", "code": f"HTTP_{status}",
            "message": (text or "").strip()[:500] or f"HTTP {status}"}


async def lumma_request(ad_type: str, source_url: str) -> dict:
    domain = _env("LUMMA_API_DOMAIN").rstrip("/")
    payload = {"source_url": source_url, "worker_id": _env("LUMMA_WORKER_ID")}
    headers = {"Content-Type": "application/json", "x-api-key": _env("LUMMA_API_KEY")}
    url = f"{domain}/api/create/{ad_type}"
    r = await _post_json(url, headers, payload)
    if r is None:
        return {"status": "error", "code": "REQUEST_FAILED", "message": "Не удалось связаться с Lumma API"}
    try:
        data = r.json()
        if isinstance(data, dict) and data.get("status"):
            return data
    except ValueError:
        pass
    if r.is_success:
        return {"status": "error", "code": "INVALID_RESPONSE", "message": "Lumma API вернул неожиданный ответ"}
    log.error(f"Lumma HTTP {r.status_code}: {r.text[:200]}")
    return _http_error(r.status_code, r.text)


async def shlepock_generate(service_code: str, source_url: str) -> dict:
    domain = _env("SHLEPOCK_API_DOMAIN").rstrip("/")
    payload = {"serviceCode": service_code, "profileId": _env("SHLEPOCK_PROFILE_ID"),
               "url": _normalize_source_url(source_url)}
    headers = {"Content-Type": "application/json",
               "Authorization": f"Bearer {_env('SHLEPOCK_API_KEY')}"}
    url = f"{domain}/public-api/v1/links/generate"
    r = await _post_json(url, headers, payload)
    if r is None:
        return {"status": "error", "code": "REQUEST_FAILED", "message": "Не удалось связаться с Shlepock API"}
    try:
        data = r.json()
    except ValueError:
        log.error(f"Shlepock не JSON: HTTP {r.status_code}")
        return {"status": "error", "code": f"HTTP_{r.status_code}",
                "message": f"API вернул не JSON (HTTP {r.status_code})"}
    if isinstance(data, dict) and data.get("error") is False:
        link = (data.get("data") or {}).get("link", "")
        if not link:
            return {"status": "error", "code": "INVALID_RESPONSE", "message": "API не вернул ссылку"}
        return {"status": "success", "code": "OK", "message": link}
    code, message = "API_ERROR", "Неизвестная ошибка"
    if isinstance(data, dict):
        code = str(data.get("code") or code)
        message = str(data.get("message") or message)
    log.error(f"Shlepock [{service_code}] {code}: {message[:200]}")
    return {"status": "error", "code": code, "message": message}


async def generate_one(api: str, service_code: str, source_url: str) -> dict:
    if not is_configured(api):
        return {"status": "error", "code": "NOT_CONFIGURED",
                "message": f"API {api} не настроен (нет env-переменных)"}
    if api == "lumma":
        return await lumma_request(service_code, source_url)
    return await shlepock_generate(service_code, source_url)


async def generate_all(source_url: str) -> list[dict]:
    """Run all targets for the URL in parallel. Returns [{label, ok, link?, code?, message?}]."""
    targets = detect_services(source_url)
    try:
        results = await asyncio.wait_for(
            asyncio.gather(*[
                generate_one(api, code, source_url) for api, code, _label in targets
            ], return_exceptions=True),
            timeout=_TIMEOUT + 5,
        )
    except asyncio.TimeoutError:
        results = [{"status": "error", "code": "TIMEOUT",
                    "message": "Превышено время ожидания"} for _ in targets]
    out = []
    for (_api, _code, label), res in zip(targets, results):
        if isinstance(res, Exception):
            res = {"status": "error", "code": "REQUEST_FAILED",
                   "message": str(res)[:500]}
        item = {"label": label, "ok": res.get("status") == "success"}
        if item["ok"]:
            item["link"] = res.get("message", "")
        else:
            item["code"] = str(res.get("code", "N/A"))
            item["message"] = str(res.get("message", "Неизвестная ошибка"))[:500]
        out.append(item)
    return out
