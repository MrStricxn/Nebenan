import asyncio
import logging
from playwright.async_api import async_playwright
from src.db import mark_seller_replied, mark_phase2_sent, get_sellers_awaiting_reply
from src.parser import _get_auth_token

log = logging.getLogger("nebena")

BASE_URL = "https://nebenan.de"
_CONV_API = "https://api.nebenan.de/api/v2/private_conversations.json"

_BTN_CONTACT   = "[data-testid='contact-seller-button']"
_INPUT_MESSAGE = "textarea[data-testid='c-message_form-textfield']"
_BTN_SEND      = "button[data-testid='c-message_form-submit']"

_LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
]


async def _fetch_conversations(request_ctx) -> list[dict]:
    all_convs: list[dict] = []
    page = 1
    while True:
        url = f"{_CONV_API}?page={page}&per_page=20"
        response = await request_ctx.get(url)
        if response.status != 200:
            log.warning(f"Conversations API вернул {response.status}")
            break
        try:
            data = await response.json()
        except Exception as e:
            log.warning(f"Ошибка разбора conversations: {e}")
            break
        convs = data.get("private_conversations", [])
        if not convs:
            break
        all_convs.extend(convs)
        if len(convs) < 20:
            break
        page += 1
    return all_convs


async def _check_replies_via_api(
    account_name: str,
    storage_state: dict,
    conn,
    semaphore: asyncio.Semaphore,
) -> list[str]:
    auth_token = _get_auth_token(storage_state)
    if not auth_token:
        log.warning(f"[{account_name}] нет auth-токена — пропуск")
        return []

    log.info(f"[{account_name}] проверка входящих...")
    replied_seller_ids: list[str] = []

    async with semaphore:
        async with async_playwright() as pw:
            request_ctx = await pw.request.new_context(
                extra_http_headers={
                    "x-auth-token": auth_token,
                    "accept": "application/json",
                    "accept-language": "de",
                }
            )

            conversations = await _fetch_conversations(request_ctx)
            await request_ctx.dispose()

            for conv in conversations:
                try:
                    partner_id = str(conv.get("partner_id", ""))
                    if not partner_id:
                        continue

                    unseen = conv.get("unseen", False)
                    last_msg = conv.get("last_private_conversation_message") or {}
                    last_sender = str(last_msg.get("sender_id", ""))

                    # Partner replied = unseen message and partner was the last to write
                    if unseen and last_sender == partner_id:
                        replied_seller_ids.append(partner_id)
                except (KeyError, TypeError):
                    continue

    log.info(f"[{account_name}] найдено ответов: {len(replied_seller_ids)}")
    return replied_seller_ids


_TRIGGER_EVENTS = """
    el => {
        el.dispatchEvent(new Event('focus', { bubbles: true }));
        el.dispatchEvent(new Event('input', { bubbles: true }));
        el.dispatchEvent(new Event('change', { bubbles: true }));
    }
"""

async def _send_phase2_playwright(page, seller: dict, template: str) -> bool:
    """Phase 2: reply in existing conversation."""
    try:
        msg_url = seller.get("message_url") or f"{BASE_URL}/messages/{seller['seller_id']}"
        await page.goto(msg_url, timeout=30000)
        textarea = page.locator(_INPUT_MESSAGE)
        await textarea.wait_for(timeout=12000)
        await textarea.click()
        await textarea.fill(template)
        await textarea.evaluate(_TRIGGER_EVENTS)
        await page.wait_for_timeout(600)
        await page.locator(_BTN_SEND).click(timeout=8000)
        await page.wait_for_timeout(1200)
        return True
    except Exception as e:
        log.warning(f"  ошибка Phase 2 ({seller.get('seller_name', '?')}): {e}")
        return False


async def check_and_reply(
    conn,
    account_pool,
    template_loader,
    delay: float = 2.0,
    progress=None,
) -> int:
    semaphore = asyncio.Semaphore(account_pool.total or 1)
    replied_ids: set[str] = set()

    tasks = []
    for _ in range(account_pool.total):
        name, state = await account_pool.checkout()
        task = asyncio.create_task(
            _check_replies_via_api(name, state, conn, semaphore)
        )
        tasks.append((name, task))

    results = await asyncio.gather(*[t for _, t in tasks], return_exceptions=True)
    for name, _ in tasks:
        await account_pool.release(name)

    for result in results:
        if isinstance(result, list):
            replied_ids.update(result)

    if not replied_ids:
        log.info("Входящих ответов не найдено")
        return 0

    log.info(f"Нашли ответы от {len(replied_ids)} продавцов — помечаем в БД")
    for seller_id in replied_ids:
        await mark_seller_replied(conn, seller_id)

    awaiting = await get_sellers_awaiting_reply(conn)
    if not awaiting:
        log.info("Phase 2 уже отправлен всем, кто ответил")
        return 0

    log.info(f"Phase 2: {len(awaiting)} продавцов ожидают ответа")
    sent_count = 0

    async def _worker(seller: dict):
        nonlocal sent_count
        async with semaphore:
            account_name, state = await account_pool.checkout()
            log.info(f"[{account_name}] Phase 2 → {seller['seller_name']} ...")
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
                    # Mark BEFORE sending — prevents double-send even if the process crashes mid-send
                    await mark_phase2_sent(conn, seller["seller_id"])
                    success = await _send_phase2_playwright(page, seller, template)
                    if success:
                        sent_count += 1
                        log.info(f"[{account_name}] ✓ Phase 2 отправлен → {seller['seller_name']}")
                    else:
                        log.warning(f"[{account_name}] ✗ Phase 2 не удалось отправить → {seller['seller_name']}")
                    await browser.close()
            except Exception as e:
                log.error(f"[{account_name}] критическая ошибка Phase 2: {e}")
            finally:
                await account_pool.release(account_name)
        if progress:
            progress.advance(progress.task_ids[0])
        await asyncio.sleep(delay)

    await asyncio.gather(*[_worker(s) for s in awaiting])
    log.info(f"Phase 2 завершён: {sent_count}/{len(awaiting)} отправлено")
    return sent_count
