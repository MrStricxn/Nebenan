import asyncio
import logging
from playwright.async_api import async_playwright
from src.db import mark_seller_contacted

log = logging.getLogger("nebena")

BASE_URL = "https://nebenan.de"

_INPUT_MESSAGE = "textarea[data-testid='c-message_form-textfield']"
_BTN_SEND      = "button[data-testid='c-message_form-submit']"

_LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
]

# Dispatch input/change events to activate the submit button (JS-driven form)
_TRIGGER_EVENTS = """
    el => {
        el.dispatchEvent(new Event('focus', { bubbles: true }));
        el.dispatchEvent(new Event('input', { bubbles: true }));
        el.dispatchEvent(new Event('change', { bubbles: true }));
    }
"""


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
    """Navigate to message thread, fill form with JS events, submit."""
    try:
        msg_url = seller.get("message_url") or f"{BASE_URL}/messages/{seller['seller_id']}"
        await page.goto(msg_url, timeout=30000)

        # Wait for the compose textarea
        textarea = page.locator(_INPUT_MESSAGE)
        await textarea.wait_for(timeout=15000)

        # Click to focus, then set value + trigger JS events that activate the submit button
        await textarea.click()
        await textarea.fill(template)
        await textarea.evaluate(_TRIGGER_EVENTS)
        await page.wait_for_timeout(600)

        # Click submit — button should now be active
        btn = page.locator(_BTN_SEND)
        await btn.click(timeout=8000)
        await page.wait_for_timeout(1200)
        return True
    except Exception as e:
        log.warning(f"  ошибка отправки ({seller.get('seller_name', '?')}): {e}")
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
        log.info("Нет новых продавцов для отправки (все уже получили сообщение)")
        return 0
    if max_per_run > 0:
        uncontacted = uncontacted[:max_per_run]

    log.info(f"Готовы к отправке: {len(uncontacted)} продавцов")

    sent_count = 0
    semaphore = asyncio.Semaphore(account_pool.total or 1)

    async def _worker(seller: dict):
        nonlocal sent_count
        async with semaphore:
            account_name, state = await account_pool.checkout()
            log.info(f"[{account_name}] → {seller['seller_name']} ...")
            try:
                async with async_playwright() as pw:
                    browser = await pw.chromium.launch(
                        headless=False,
                        args=_LAUNCH_ARGS,
                    )
                    ctx = await browser.new_context(
                        storage_state=state,
                        user_agent=(
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/124.0.0.0 Safari/537.36"
                        ),
                    )
                    page = await ctx.new_page()
                    template = template_loader.get_random()
                    success = await _send_one(page, seller, template)
                    if success:
                        await mark_seller_contacted(conn, seller["seller_id"])
                        sent_count += 1
                        log.info(f"[{account_name}] ✓ отправлено → {seller['seller_name']}")
                    else:
                        log.warning(f"[{account_name}] ✗ не отправлено → {seller['seller_name']}")
                    await browser.close()
            except Exception as e:
                log.error(f"[{account_name}] критическая ошибка: {e}")
            finally:
                await account_pool.release(account_name)
        if progress:
            progress.advance(progress.task_ids[0])
        await asyncio.sleep(delay)

    await asyncio.gather(*[_worker(s) for s in uncontacted])
    log.info(f"Рассылка завершена: {sent_count}/{len(uncontacted)} отправлено")
    return sent_count
