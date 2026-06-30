"""
Sniff the exact POST request when sending a message via 'Verkäufer kontaktieren'.
Navigates to messages URL with listing context and captures the API call.
"""
import asyncio, json, sys
from pathlib import Path
from playwright.async_api import async_playwright

sys.stdout.reconfigure(encoding="utf-8")

COOKIES_DIR = Path("cookies")
_API_POSTS  = "https://api.nebenan.de/api/core/v3/marketplace/posts"
_LAUNCH_ARGS = ["--disable-blink-features=AutomationControlled", "--no-sandbox"]
_SAME_SITE  = {"strict":"Strict","lax":"Lax","none":"None","no_restriction":"None","unspecified":"Lax"}

def load_storage(path: Path) -> dict:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        cookies = []
        for c in raw:
            ss = _SAME_SITE.get(str(c.get("sameSite","lax")).lower(), "Lax")
            cookies.append({"name":c["name"],"value":c["value"],
                "domain":c.get("domain",".nebenan.de"),"path":c.get("path","/"),
                "secure":c.get("secure",True),"httpOnly":c.get("httpOnly",False),"sameSite":ss})
        return {"cookies": cookies, "origins": []}
    return raw

def get_token(storage: dict) -> str:
    for c in storage.get("cookies",[]):
        if c.get("name") == "s" and "nebenan" in c.get("domain",""):
            return c["value"]
    return ""


async def main():
    cookie_file = next(COOKIES_DIR.glob("*.json"), None)
    storage = load_storage(cookie_file)
    token   = get_token(storage)
    headers = {"x-auth-token": token, "accept": "application/json", "accept-language": "de",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    # ── Step 1: fetch one listing via API ──
    async with async_playwright() as pw:
        req_ctx = await pw.request.new_context(extra_http_headers=headers)
        resp    = await req_ctx.get(f"{_API_POSTS}?categories=&limit=5")
        data    = await resp.json()
        items   = data.get("page", [])

        # Pick a listing that has images
        target = None
        for item in items:
            if item["post"].get("images"):
                target = item["post"]
                break
        if not target:
            target = items[0]["post"]

        await req_ctx.dispose()

        listing_id  = str(target["id"])
        seller_id   = target["author_details"]["associated_gid"].split("/")[-1]
        seller_name = target["author_details"]["name"]
        listing_title = target["subject"]
        img_url     = (target.get("images") or [{}])[0].get("url","")

        print(f"listing_id : {listing_id}")
        print(f"seller_id  : {seller_id}")
        print(f"seller_name: {seller_name}")
        print(f"title      : {listing_title}")
        print(f"image      : {img_url[:80]}...")

        captured = []

        async def on_request(req):
            if "nebenan" in req.url and req.method in ("POST","PUT","PATCH"):
                entry = {"method": req.method, "url": req.url}
                try:
                    body = req.post_data
                    if body:
                        entry["body"] = body[:1000]
                except Exception:
                    pass
                captured.append(entry)
                print(f"\n[REQUEST] {req.method} {req.url}")
                if "body" in entry:
                    print(f"  body: {entry['body']}")

        async def on_response(res):
            if "nebenan" in res.url and res.status in (200,201) and "message" in res.url.lower():
                try:
                    body = await res.text()
                    print(f"\n[RESPONSE {res.status}] {res.url}")
                    print(body[:800])
                except Exception:
                    pass

        # ── Step 2: open browser and try candidate URLs ──
        browser = await pw.chromium.launch(headless=False, args=_LAUNCH_ARGS)
        ctx = await browser.new_context(storage_state=storage,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
        page = await ctx.new_page()
        page.on("request", on_request)
        page.on("response", on_response)

        # Try candidate listing page URLs
        candidates = [
            f"https://nebenan.de/nachbarschaft/marktplatz/{listing_id}",
            f"https://nebenan.de/p/{listing_id}",
            f"https://nebenan.de/messages/{seller_id}?post_id={listing_id}",
            f"https://nebenan.de/messages/{seller_id}?from_post={listing_id}",
        ]

        found_btn_on = None
        for url in candidates:
            print(f"\n--- Trying: {url}")
            try:
                r = await page.goto(url, timeout=12000, wait_until="domcontentloaded")
                await page.wait_for_timeout(2500)
                final = page.url
                print(f"    final_url: {final}")

                # Check if listing image appears in the message form area
                imgs = await page.locator("img").count()
                print(f"    images on page: {imgs}")

                btn = page.locator("[data-testid='contact-seller-button']")
                cnt = await btn.count()
                print(f"    contact-seller-button: {cnt}")
                if cnt > 0:
                    found_btn_on = url
                    print("    -> CLICKING button...")
                    await btn.first.click(timeout=5000)
                    await page.wait_for_timeout(2000)
                    print(f"    -> after click URL: {page.url}")
                    break

                # Check if textarea is visible (already in message compose)
                ta = page.locator("textarea[data-testid='c-message_form-textfield']")
                ta_cnt = await ta.count()
                print(f"    textarea visible: {ta_cnt}")
                if ta_cnt > 0:
                    # Check if listing is shown in context (any image near form)
                    print("    -> textarea found! Looking for listing context...")
                    # Look for any embedded post preview near form
                    preview = page.locator("[class*='embeddable'], [class*='post-preview'], [class*='listing']")
                    prev_cnt = await preview.count()
                    print(f"    -> listing preview elements: {prev_cnt}")

                    # Type and send test message to capture API format
                    print("    -> Typing test message to capture POST format...")
                    await ta.click()
                    await ta.fill("TEST - bitte ignorieren")
                    await ta.evaluate("""el => {
                        el.dispatchEvent(new Event('focus', {bubbles:true}));
                        el.dispatchEvent(new Event('input', {bubbles:true}));
                        el.dispatchEvent(new Event('change', {bubbles:true}));
                    }""")
                    await page.wait_for_timeout(500)
                    btn_send = page.locator("button[data-testid='c-message_form-submit']")
                    if await btn_send.count() > 0:
                        enabled = await btn_send.is_enabled()
                        print(f"    -> send button enabled: {enabled}")
                        # DON'T actually click — we just want to see the form state
                    break
            except Exception as e:
                print(f"    error: {e}")

        print(f"\n\n=== SUMMARY ===")
        print(f"Contact button found on: {found_btn_on}")
        print(f"Total POST/PUT captured: {len(captured)}")
        for r in captured:
            print(json.dumps(r, ensure_ascii=False, indent=2))

        input("\nPress Enter to close browser...")
        await browser.close()


asyncio.run(main())
