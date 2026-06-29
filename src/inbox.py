import asyncio
from playwright.async_api import async_playwright
from src.db import mark_seller_replied, mark_phase2_sent, get_sellers_awaiting_reply
from src.parser import _get_auth_token

BASE_URL = "https://nebenan.de"
_CONV_API = "https://api.nebenan.de/api/core/v3/conversations"

# Same message form selectors as sender
_INPUT_MESSAGE = "textarea[data-testid='c-message_form-textfield']"
_BTN_SEND      = "button[data-testid='c-message_form-submit']"


async def _fetch_conversations(request_ctx, auth_token: str) -> list[dict]:
    """Fetch all conversations via API, return list of conversation dicts."""
    headers = {"x-auth-token": auth_token, "accept": "application/json"}
    response = await request_ctx.get(_CONV_API, headers=headers)
    if response.status != 200:
        return []
    try:
        data = await response.json()
        if isinstance(data, list):
            return data
        return data.get("conversations", data.get("items", []))
    except Exception:
        return []


async def _check_replies_via_api(
    storage_state: dict,
    conn,
    semaphore: asyncio.Semaphore,
) -> list[str]:
    """
    Check conversations for seller replies using the API.
    Returns list of seller_ids that have replied since Phase 1.
    """
    auth_token = _get_auth_token(storage_state)
    if not auth_token:
        return []

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

            conversations = await _fetch_conversations(request_ctx, auth_token)
            await request_ctx.dispose()

            for conv in conversations:
                try:
                    # Extract seller_id from conversation partner
                    partner = conv.get("partner") or conv.get("other_user") or {}
                    partner_id = str(
                        partner.get("id") or
                        conv.get("partner_id") or
                        conv.get("user_id") or ""
                    )
                    if not partner_id:
                        continue

                    # Check if last message is from partner (they replied)
                    last_msg = conv.get("last_message") or conv.get("latest_message") or {}
                    sender_id = str(last_msg.get("sender_id") or last_msg.get("author_id") or "")

                    # Also check unread count — if partner sent something, unread > 0
                    unread = conv.get("unread_count", 0)

                    if sender_id == partner_id or unread > 0:
                        replied_seller_ids.append(partner_id)
                except (KeyError, TypeError):
                    continue

    return replied_seller_ids


async def _send_phase2_playwright(page, seller: dict, template: str) -> bool:
    """Send Phase 2 message using Playwright."""
    try:
        msg_url = seller.get("message_url") or f"{BASE_URL}/messages/{seller['seller_id']}"
        await page.goto(msg_url, timeout=30000)
        await page.wait_for_selector(_INPUT_MESSAGE, timeout=10000)
        await page.fill(_INPUT_MESSAGE, template)
        await page.click(_BTN_SEND, timeout=10000)
        return True
    except Exception:
        return False


async def check_and_reply(
    conn,
    account_pool,
    template_loader,
    delay: float = 2.0,
    progress=None,
) -> int:
    """
    Check inbox for seller replies and send Phase 2 messages.
    Returns number of Phase 2 messages sent.
    """
    semaphore = asyncio.Semaphore(account_pool.total or 1)
    replied_ids: set[str] = set()

    # Check all accounts for replies
    tasks = []
    for _ in range(account_pool.total):
        name, state = await account_pool.checkout()
        task = asyncio.create_task(
            _check_replies_via_api(state, conn, semaphore)
        )
        task._account_name = name
        tasks.append((name, task))

    results = await asyncio.gather(*[t for _, t in tasks], return_exceptions=True)
    for name, _ in tasks:
        await account_pool.release(name)

    for result in results:
        if isinstance(result, list):
            replied_ids.update(result)

    if not replied_ids:
        return 0

    # Mark replied sellers in DB
    for seller_id in replied_ids:
        await mark_seller_replied(conn, seller_id)

    # Get sellers who need Phase 2 response
    awaiting = await get_sellers_awaiting_reply(conn)
    if not awaiting:
        return 0

    sent_count = 0

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
                    success = await _send_phase2_playwright(page, seller, template)
                    if success:
                        await mark_phase2_sent(conn, seller["seller_id"])
                        sent_count += 1
                    await browser.close()
            finally:
                await account_pool.release(name)
        if progress:
            progress.advance(progress.task_ids[0])
        await asyncio.sleep(delay)

    await asyncio.gather(*[_worker(s) for s in awaiting])
    return sent_count
