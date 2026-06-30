import logging
from playwright.async_api import async_playwright

log = logging.getLogger("nebena")

_BASE = "https://api.nebenan.de"
_LAUNCH_ARGS = ["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage"]
_TRIGGER = """el => {
    el.dispatchEvent(new Event('focus', {bubbles:true}));
    el.dispatchEvent(new Event('input', {bubbles:true}));
    el.dispatchEvent(new Event('change', {bubbles:true}));
}"""


def get_token(storage_state: dict) -> str | None:
    for c in storage_state.get("cookies", []):
        if c.get("name") == "s" and "nebenan" in c.get("domain", ""):
            return c["value"]
    return None


async def _api_get(token: str, path: str) -> dict | list | None:
    async with async_playwright() as pw:
        ctx = await pw.request.new_context(extra_http_headers={
            "x-auth-token": token,
            "accept": "application/json",
            "accept-language": "de",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })
        r = await ctx.get(f"{_BASE}{path}")
        result = await r.json() if r.status == 200 else None
        await ctx.dispose()
    return result


async def fetch_conversations(token: str, page: int = 1, per_page: int = 30) -> list[dict]:
    d = await _api_get(token, f"/api/v2/private_conversations.json?page={page}&per_page={per_page}")
    if not d:
        return []
    return d.get("private_conversations", []) if isinstance(d, dict) else d


async def fetch_messages(token: str, partner_id: int, per_page: int = 50) -> dict:
    d = await _api_get(token, f"/api/v2/private_conversations/{partner_id}.json?per_page={per_page}")
    return d or {}


async def fetch_profile(token: str) -> dict:
    d = await _api_get(token, "/api/core/v3/profile")
    return d or {}


async def send_reply(storage_state: dict, partner_id: int, text: str) -> bool:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False, args=_LAUNCH_ARGS)
        ctx = await browser.new_context(
            storage_state=storage_state,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        )
        page = await ctx.new_page()
        try:
            await page.goto(f"https://nebenan.de/messages/{partner_id}", timeout=30000)
            ta = page.locator("textarea[data-testid='c-message_form-textfield']")
            await ta.wait_for(timeout=15000)
            await ta.click()
            await ta.fill(text)
            await ta.evaluate(_TRIGGER)
            await page.wait_for_timeout(600)
            await page.locator("button[data-testid='c-message_form-submit']").click(timeout=8000)
            await page.wait_for_timeout(1500)
            return True
        except Exception as e:
            log.error(f"send_reply({partner_id}): {e}")
            return False
        finally:
            await browser.close()
