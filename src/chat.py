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
    """Send text via direct API POST — no browser needed."""
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


_LAUNCH_ARGS = ["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage"]
_TA_SEL      = "textarea[data-testid='c-message_form-textfield']"
_BTN_SEL     = "button[data-testid='c-message_form-submit']"
_TRIGGER     = "el => { el.dispatchEvent(new Event('focus',{bubbles:true})); el.dispatchEvent(new Event('input',{bubbles:true})); el.dispatchEvent(new Event('change',{bubbles:true})) }"


async def send_with_photos(storage_state: dict, receiver_id: int, text: str, photo_paths: list[str]) -> bool:
    """Send a message with attached photos via Playwright browser."""
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False, args=_LAUNCH_ARGS)
        ctx = await browser.new_context(
            storage_state=storage_state,
            user_agent=_USER_AGENT,
        )
        page = await ctx.new_page()
        try:
            await page.goto(f"https://nebenan.de/messages/{receiver_id}", timeout=30000)
            ta = page.locator(_TA_SEL)
            await ta.wait_for(timeout=15000)

            # Attach photos via the hidden file input in the compose form
            file_input = page.locator("input[type='file']").first
            try:
                await file_input.set_input_files(photo_paths, timeout=5000)
                await page.wait_for_timeout(1500)
            except Exception as e:
                log.warning(f"send_with_photos: file input not found ({e}), sending text only")

            if text:
                await ta.click()
                await ta.fill(text)
                await ta.evaluate(_TRIGGER)
                await page.wait_for_timeout(500)

            await page.locator(_BTN_SEL).click(timeout=8000)
            await page.wait_for_timeout(1500)
            log.info(f"send_with_photos → {receiver_id}: OK ({len(photo_paths)} фото)")
            return True
        except Exception as e:
            log.error(f"send_with_photos({receiver_id}): {e}")
            return False
        finally:
            await browser.close()
