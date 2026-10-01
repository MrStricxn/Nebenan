import asyncio
import logging
from playwright.async_api import async_playwright  # browser flow only (phase-2 send)
from src.http import api_request_ex
from src.db import mark_seller_replied, mark_phase2_sent, get_sellers_awaiting_reply
from src.parser import _get_auth_token
from src.proxy import playwright_proxy, ensure_logged_in, is_ip_blocked
from src.sender import _dismiss_consent, _submit_click, _goto_with_retry

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


async def _fetch_conversations(headers: dict, account_name: str = "") -> list[dict]:
    all_convs: list[dict] = []
    page = 1
    while True:
        url = f"{_CONV_API}?page={page}&per_page=20"
        # Via api_request_ex: timeouts + non-2xx also feed ban detection.
        data, status, _txt = await api_request_ex(
            "GET", url, (headers or {}).get("x-auth-token", ""),
            account_name=account_name,
            extra_headers={"accept": "application/json", "accept-language": "de"})
        if not isinstance(data, dict):
            log.warning(f"Conversations API вернул {status}")
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
        conversations = await _fetch_conversations({
            "x-auth-token": auth_token,
            "accept": "application/json",
            "accept-language": "de",
        }, account_name)

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

async def _send_phase2_playwright(page, seller: dict, template: str, account_name: str = "?") -> bool:
    """Phase 2: reply in existing conversation."""
    try:
        msg_url = seller.get("message_url") or f"{BASE_URL}/messages/{seller['seller_id']}"
        sid = seller.get('seller_id', '?')
        resp = await _goto_with_retry(page, msg_url, what=f"phase2({sid})")
        if resp is None:
            return False
        if is_ip_blocked(resp):
            log.warning(f"  [{account_name}] IP прокси заблокирован (403) — ротируйте прокси")
            return False
        if not await ensure_logged_in(page, account_name, where=" (phase-2)"):
            return False
        await _dismiss_consent(page)
        textarea = page.locator(_INPUT_MESSAGE)
        await textarea.wait_for(timeout=12000)
        await textarea.click()
        await textarea.fill(template)
        await textarea.evaluate(_TRIGGER_EVENTS)
        await page.wait_for_timeout(600)
        await _submit_click(page, page.locator(_BTN_SEND), what=f"phase2({sid})")
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
            browser = None
            try:
                async with async_playwright() as pw:
                    import os as _os
                    _headless = _os.environ.get("HEADLESS", "1") != "0"
                    launch_kwargs: dict = {
                        "headless": _headless,
                        "args": _LAUNCH_ARGS,
                    }
                    _proxy = playwright_proxy()
                    if _proxy:
                        # Global proxy at launch (see chat.py): per-context
                        # proxy without it raises Playwright proxy error.
                        launch_kwargs["proxy"] = _proxy
                    browser = await pw.chromium.launch(**launch_kwargs)
                    ctx_kwargs = {
                        "storage_state": state,
                        "user_agent": (
                            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/124.0.0.0 Safari/537.36"
                        ),
                    }
                    ctx = await browser.new_context(**ctx_kwargs)
                    page = await ctx.new_page()
                    template = template_loader.get_random()
                    success = await _send_phase2_playwright(page, seller, template, account_name)
                    if success:
                        # Mark only after success so failures are retried
                        await mark_phase2_sent(conn, seller["seller_id"])
                        sent_count += 1
                        log.info(f"[{account_name}] ✓ Phase 2 отправлен → {seller['seller_name']}")
                    else:
                        log.warning(f"[{account_name}] ✗ Phase 2 не удалось отправить → {seller['seller_name']} (будет повтор)")
            except Exception as e:
                log.error(f"[{account_name}] критическая ошибка Phase 2: {e}")
            finally:
                if browser is not None:
                    try:
                        await browser.close()
                    except Exception:
                        pass
                await account_pool.release(account_name)
        if progress:
            progress.advance(progress.task_ids[0])
        await asyncio.sleep(delay)

    await asyncio.gather(*[_worker(s) for s in awaiting])
    log.info(f"Phase 2 завершён: {sent_count}/{len(awaiting)} отправлено")
    return sent_count
