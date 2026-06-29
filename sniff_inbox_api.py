"""
Run this once to discover the correct inbox/conversations API endpoint.
It opens a real browser, logs in via cookies, navigates to /messages,
and prints all API requests nebenan.de makes.
"""
import asyncio
import json
import sys
from pathlib import Path
from playwright.async_api import async_playwright

sys.stdout.reconfigure(encoding="utf-8")

COOKIES_DIR = Path("cookies")
LAUNCH_ARGS = ["--disable-blink-features=AutomationControlled", "--no-sandbox"]


def load_storage_state(path: Path) -> dict:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        cookies = []
        for c in raw:
            same_site_map = {
                "strict": "Strict", "lax": "Lax", "none": "None",
                "no_restriction": "None", "unspecified": "Lax",
            }
            raw_ss = str(c.get("sameSite", "lax")).lower()
            same_site = same_site_map.get(raw_ss, "Lax")
            cookies.append({
                "name":     c["name"],
                "value":    c["value"],
                "domain":   c.get("domain", ".nebenan.de"),
                "path":     c.get("path", "/"),
                "secure":   c.get("secure", True),
                "httpOnly": c.get("httpOnly", False),
                "sameSite": same_site,
            })
        return {"cookies": cookies, "origins": []}
    return raw


async def main():
    cookie_file = next(COOKIES_DIR.glob("*.json"), None)
    if not cookie_file:
        print("No cookie files found in cookies/")
        return

    print(f"Using account: {cookie_file.stem}")
    storage = load_storage_state(cookie_file)

    captured = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False, args=LAUNCH_ARGS)
        ctx = await browser.new_context(
            storage_state=storage,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        )

        async def on_request(req):
            url = req.url
            if "api.nebenan.de" in url or "nebenan.de/api" in url:
                captured.append(f"[{req.method}] {url}")

        async def on_response(res):
            url = res.url
            if ("message" in url or "conversation" in url or "inbox" in url or "chat" in url) and "nebenan" in url:
                try:
                    body = await res.text()
                    print(f"\n=== RESPONSE {res.status} {url} ===")
                    print(body[:2000])
                except Exception:
                    pass

        page = await ctx.new_page()
        page.on("request", on_request)
        page.on("response", on_response)

        print("Navigating to /messages ...")
        await page.goto("https://nebenan.de/messages", timeout=30000)
        await page.wait_for_timeout(5000)

        print("\n=== ALL API REQUESTS ===")
        for r in captured:
            print(r)

        await browser.close()


asyncio.run(main())
