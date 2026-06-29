import asyncio
from playwright.async_api import async_playwright
from src.db import mark_seller_contacted

BASE_URL = "https://nebenan.de"

_INPUT_MESSAGE = "textarea[data-testid='c-message_form-textfield']"
_BTN_SEND      = "button[data-testid='c-message_form-submit']"


async def _filter_uncontacted(conn, sellers: list[dict]) -> list[dict]:
    result = []
    for seller in sellers:
        async with conn.execute(
            "SELECT message_sent FROM sellers WHERE seller_id = ?",
            (seller["seller_id"],)
        ) as cur:
            row = await cur.fetchone()
        if row is None or row[0] == 0:
            result.append(seller)
    return result


async def _send_one(page, seller: dict, template: str) -> bool:
    try:
        msg_url = seller.get("message_url") or f"{BASE_URL}/messages/{seller['seller_id']}"
        await page.goto(msg_url, timeout=30000)
        await page.wait_for_selector(_INPUT_MESSAGE, timeout=10000)
        await page.fill(_INPUT_MESSAGE, template)
        await page.click(_BTN_SEND, timeout=10000)
        return True
    except Exception:
        return False


async def send_messages(
    sellers: list[dict],
    account_pool,
    template_loader,
    conn,
    delay: float = 2.0,
    progress=None,
    max_per_run: int = 0,
) -> int:
    uncontacted = await _filter_uncontacted(conn, sellers)
    if not uncontacted:
        return 0
    if max_per_run > 0:
        uncontacted = uncontacted[:max_per_run]

    sent_count = 0
    semaphore = asyncio.Semaphore(account_pool.total or 1)

    async def _worker(seller: dict):
        nonlocal sent_count
        async with semaphore:
            name, state = await account_pool.checkout()
            try:
                async with async_playwright() as pw:
                    browser = await pw.chromium.launch(headless=True)
                    ctx = await browser.new_context(
                        storage_state=state,
                        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                    )
                    page = await ctx.new_page()
                    template = template_loader.get_random()
                    success = await _send_one(page, seller, template)
                    if success:
                        await mark_seller_contacted(conn, seller["seller_id"])
                        sent_count += 1
                    await browser.close()
            finally:
                await account_pool.release(name)
        if progress:
            progress.advance(progress.task_ids[0])
        await asyncio.sleep(delay)

    await asyncio.gather(*[_worker(s) for s in uncontacted])
    return sent_count
