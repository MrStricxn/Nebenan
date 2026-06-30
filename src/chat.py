import json
import logging
from playwright.async_api import async_playwright

log = logging.getLogger("nebena")

_BASE         = "https://api.nebenan.de"
_SEND_URL     = f"{_BASE}/api/v2/private_conversations.json"
_CLIENT_VER   = "web CoreFE-client-c_v284-1__be14f47"
_USER_AGENT   = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"

_READ_HEADERS = {
    "accept":          "application/json",
    "accept-language": "de-DE,de;q=0.9",
    "user-agent":      _USER_AGENT,
}

_WRITE_HEADERS = {
    "accept":                   "application/json",
    "accept-language":          "de-DE,de;q=0.9",
    "content-type":             "application/json",
    "nebenan-client-version":   _CLIENT_VER,
    "x-translations-lang":      "de-DE",
    "user-agent":               _USER_AGENT,
}


def get_token(storage_state: dict) -> str | None:
    for c in storage_state.get("cookies", []):
        if c.get("name") == "s" and "nebenan" in c.get("domain", ""):
            return c["value"]
    return None


async def _request(method: str, url: str, token: str, body: dict | None = None) -> dict | list | None:
    headers = {**(_WRITE_HEADERS if body is not None else _READ_HEADERS), "x-auth-token": token}
    async with async_playwright() as pw:
        ctx = await pw.request.new_context(extra_http_headers=headers)
        if method == "GET":
            r = await ctx.get(url)
        else:
            r = await ctx.post(url, data=json.dumps(body))
        result = await r.json() if r.status in (200, 201) else None
        if r.status not in (200, 201):
            log.warning(f"API {method} {url} → {r.status}: {await r.text()[:200]}")
        await ctx.dispose()
    return result


async def fetch_conversations(token: str, page: int = 1, per_page: int = 30) -> list[dict]:
    d = await _request("GET", f"{_BASE}/api/v2/private_conversations.json?page={page}&per_page={per_page}", token)
    if not d:
        return []
    return d.get("private_conversations", []) if isinstance(d, dict) else d


async def fetch_messages(token: str, partner_id: int, per_page: int = 50) -> dict:
    d = await _request("GET", f"{_BASE}/api/v2/private_conversations/{partner_id}.json?per_page={per_page}", token)
    return d or {}


async def fetch_profile(token: str) -> dict:
    d = await _request("GET", f"{_BASE}/api/core/v3/profile", token)
    return d or {}


async def send_message(token: str, receiver_id: int, text: str) -> bool:
    """Send via direct API POST — no browser needed."""
    payload = {
        "private_conversation_message": {
            "body":        text,
            "images":      [],
            "embeddables": [],
            "receiver_id": receiver_id,
        },
        "user_agent": _USER_AGENT,
    }
    result = await _request("POST", _SEND_URL, token, body=payload)
    ok = result is not None
    if ok:
        log.info(f"send_message → {receiver_id}: OK")
    else:
        log.warning(f"send_message → {receiver_id}: FAILED")
    return ok
