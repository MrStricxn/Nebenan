import asyncio
import logging
import time
from playwright.async_api import async_playwright
from src.db import mark_seller_contacted
from src.proxy import playwright_proxy, ensure_logged_in, is_ip_blocked

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


async def _emit(on_log, text: str, level: str = "info") -> None:
    """Stdlib log (file/console) + optional dashboard (WS) log. Never raises.

    When on_log is given, the record is flagged so the WSHandler
    stdlib→WS bridge skips it — otherwise every event would reach the
    dashboard twice (once via the bridge, once via on_log).
    """
    kw = {"extra": {"via_dashboard": True}} if on_log is not None else {}
    if level == "error":
        log.error(text, **kw)
    elif level == "warning":
        log.warning(text, **kw)
    else:
        log.info(text, **kw)
    if on_log is not None:
        try:
            await on_log(text, level)
        except Exception:
            pass


async def _filter_uncontacted(conn, sellers: list[dict]) -> list[dict]:
    if not sellers:
        return []
    ids = [s["seller_id"] for s in sellers]
    sent: dict = {}
    # Chunked: thousands of ids would exceed SQLite's variable limit.
    for off in range(0, len(ids), 500):
        chunk = ids[off:off + 500]
        marks = ",".join("?" for _ in chunk)
        async with conn.execute(
            f"SELECT seller_id, message_sent FROM sellers WHERE seller_id IN ({marks})",
            chunk,
        ) as cur:
            rows = await cur.fetchall()
        sent.update({r[0]: r[1] for r in rows})
    # Missing row = never seen = uncontacted (same as the old per-row check).
    return [s for s in sellers if sent.get(s["seller_id"], 0) == 0]


# Sourcepoint consent overlay (privacy-mgmt.com) reappears in fresh browser
# contexts and intercepts pointer events on the submit button. It can also
# (re)load LATE (fresh consentUUID per campaign), so dismissal must happen
# right before the submit click — not once after page load.
_CONSENT_IFRAME = "iframe[title='SP Consent message']"
_CONSENT_ACCEPT_LABELS = (
    "Alle akzeptieren", "Alles akzeptieren", "Zustimmen", "Akzeptieren", "Accept all",
    "Einverstanden", "Verstanden", "Ich stimme zu",
)
# Deny-variants are only a fallback: the goal is dismissing the overlay
# (deny closes it too, and is the privacy-friendlier choice).
_CONSENT_DENY_LABELS = (
    "Alle ablehnen", "Ablehnen", "Nur notwendige", "Notwendige akzeptieren",
    "Reject all", "Necessary only",
)


async def _dismiss_consent(page) -> bool:
    """Dismiss the Sourcepoint consent dialog if present. Never raises.

    Returns True only if the overlay is actually gone (verified) or was
    never there. A blind click is NOT enough — some campaigns ignore the
    first click or open a settings layer instead of closing.
    """
    try:
        for _round in range(3):
            frame_loc = page.locator(_CONSENT_IFRAME)
            if await frame_loc.count() == 0:
                return True
            frame = frame_loc.content_frame()
            if frame is None:
                break
            clicked = False
            for label in _CONSENT_ACCEPT_LABELS + _CONSENT_DENY_LABELS:
                try:
                    btn = frame.get_by_role("button", name=label)
                    if await btn.count():
                        try:
                            await btn.first.click(timeout=4000)
                        except Exception:
                            try:
                                await btn.first.evaluate("(el) => el.click()")
                            except Exception:
                                continue
                        clicked = True
                        break
                except Exception:
                    continue
            if not clicked:
                # Last resort: Sourcepoint accept-type button by class
                # (sp_choice_type_11 — campaigns like "Ich stimme zu" where
                # role/name matching fails but the class is stable).
                try:
                    cls_btn = frame.locator("button.sp_choice_type_11")
                    if await cls_btn.count():
                        try:
                            await cls_btn.first.click(timeout=4000)
                        except Exception:
                            try:
                                await cls_btn.first.evaluate("(el) => el.click()")
                            except Exception:
                                pass
                        clicked = True
                except Exception:
                    pass
            if not clicked:
                try:
                    await page.keyboard.press("Escape")
                except Exception:
                    pass
            try:
                await frame_loc.first.wait_for(state="detached", timeout=3000)
                return True
            except Exception:
                pass
        try:
            return await page.locator(_CONSENT_IFRAME).count() == 0
        except Exception:
            return False
    except Exception:
        return False


async def _submit_click(page, btn, what: str = "submit", attempts: int = 3) -> None:
    """Click a submit button that the late-loading consent overlay may block.

    Retries with dismissal between attempts; final fallback is a DOM-level
    click, which bypasses hit-testing entirely (the overlay can't intercept
    it). Raises on total failure.
    """
    # The DOM-click fallback below raises on its own on total failure.
    for _i in range(attempts):
        try:
            await btn.click(timeout=8000)
            return
        except Exception:
            log.info(f"{what}: submit click blocked, dismissing consent")
            await _dismiss_consent(page)
    # Overlay survived dismissal (unknown button labels?) — DOM click still
    # reaches the real button through it.
    log.warning(f"{what}: overlay persists, using DOM-click fallback")
    await btn.evaluate("(el) => el.click()")


async def _goto_with_retry(page, url: str, what: str = "goto"):
    """Two-attempt page load (30s + 60s) for slow-proxy page loads.

    Page loads through the proxy can be 3-4x slower (dozens of
    subresources) — retry once with a longer timeout instead of failing
    on a single 30s timeout. Returns the response or None.
    """
    for _attempt, _gt in ((1, 30000), (2, 60000)):
        try:
            return await page.goto(url, timeout=_gt, wait_until="domcontentloaded")
        except Exception as e:
            log.warning(f"{what}: goto attempt {_attempt} failed ({e})")
    return None


# nebenan's deleted-listing 404 page. Only the distinctive line is
# matched ("Das tut uns sehr leid" also appears on other error pages
# and would cause false skips).
_DELETED_MARKER = "Diese Seite gibt es nicht"


def _is_listing_deleted(html: str) -> bool:
    """True when the page HTML is nebenan's deleted-listing notice."""
    return _DELETED_MARKER in (html or "")


async def _send_one(page, seller: dict, template: str, account_name: str = "?",
                  on_log=None) -> bool:
    """Navigate to listing page → click contact button → fill form → submit."""
    try:
        listing_url = seller.get("listing_url")
        msg_url     = seller.get("message_url") or f"{BASE_URL}/messages/{seller['seller_id']}"

        if listing_url:
            resp = await _goto_with_retry(page, listing_url, what=f"sender({seller.get('seller_name', '?')})")
            if resp is None:
                await _emit(on_log, f"[{account_name}] страница не загрузилась (таймаут) — "
                                    f"пропуск {seller.get('seller_name', '?')}", "warning")
                return False
            if is_ip_blocked(resp):
                await _emit(on_log, f"[{account_name}] IP прокси заблокирован (HTTP 403) — "
                                        f"ротируйте прокси, пропуск {seller.get('seller_name', '?')}", "warning")
                return False
            try:
                listing_html = await page.content()
            except Exception:
                listing_html = ""
            if _is_listing_deleted(listing_html):
                await _emit(on_log, f"[{account_name}] объявление удалено — "
                                    f"пропуск {seller.get('seller_name', '?')}", "warning")
                return False
            contact_btn = page.locator("[data-testid='contact-seller-button']")
            try:
                await contact_btn.wait_for(timeout=10000)
                await contact_btn.click(timeout=5000)
                # Wait until we land on the messages page (listing image auto-attaches)
                await page.wait_for_url("**/messages/**", timeout=15000)
            except Exception:
                # Button not found or timeout — fall back to direct messages URL
                resp = await _goto_with_retry(page, msg_url, what=f"sender({seller.get('seller_name', '?')})")
                if resp is None:
                    await _emit(on_log, f"[{account_name}] страница не загрузилась (таймаут) — "
                                        f"пропуск {seller.get('seller_name', '?')}", "warning")
                    return False
                if is_ip_blocked(resp):
                    await _emit(on_log, f"[{account_name}] IP прокси заблокирован (HTTP 403) — "
                                        f"ротируйте прокси, пропуск {seller.get('seller_name', '?')}", "warning")
                    return False
        else:
            resp = await _goto_with_retry(page, msg_url, what=f"sender({seller.get('seller_name', '?')})")
            if resp is None:
                await _emit(on_log, f"[{account_name}] страница не загрузилась (таймаут) — "
                                    f"пропуск {seller.get('seller_name', '?')}", "warning")
                return False
            if is_ip_blocked(resp):
                await _emit(on_log, f"[{account_name}] IP прокси заблокирован (HTTP 403) — "
                                    f"ротируйте прокси, пропуск {seller.get('seller_name', '?')}", "warning")
                return False

        account = account_name
        if not await ensure_logged_in(page, account, where=" (sender)"):
            await _emit(on_log, f"[{account}] сессия истекла — пропуск {seller.get('seller_name', '?')}", "warning")
            return False

        # Fill the compose textarea (dismiss consent first — the overlay
        # hides the form and the textarea never becomes visible under it).
        await _dismiss_consent(page)
        textarea = page.locator(_INPUT_MESSAGE)
        await textarea.wait_for(timeout=15000)
        await textarea.click()
        await textarea.fill(template)
        await textarea.evaluate(_TRIGGER_EVENTS)
        await page.wait_for_timeout(600)

        btn = page.locator(_BTN_SEND)
        try:
            await _submit_click(page, btn, what=f"sender({seller.get('seller_name', '?')})")
        except Exception as e:
            await _emit(on_log, f"  кнопка отправки не кликнулась: {e}", "warning")
            return False
        await page.wait_for_timeout(1200)
        return True
    except Exception as e:
        await _emit(on_log, f"  ошибка отправки ({seller.get('seller_name', '?')}): {e}", "warning")
        return False


async def send_messages(
    sellers: list[dict],
    account_pool,
    template_loader,
    conn,
    delay: float = 2.0,
    progress=None,
    max_per_run: int = 0,
    on_log=None,
) -> int:
    if getattr(account_pool, "total", 0) == 0:
        await _emit(on_log, "Нет аккаунтов в пуле — отправка невозможна", "warning")
        return 0
    uncontacted = await _filter_uncontacted(conn, sellers)
    if not uncontacted:
        log.info("Нет новых продавцов для отправки (все уже получили сообщение)")
        return 0
    if max_per_run > 0:
        uncontacted = uncontacted[:max_per_run]

    log.info(f"Готовы к отправке: {len(uncontacted)} продавцов")

    # Strictly sequential: ONE browser for the whole run, one send at a
    # time (fresh context per account). No parallel browsers — watchable
    # in headed mode, gentle on RAM, and the delay between sends is real.
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

    sent_count = 0
    browser = None
    try:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(**launch_kwargs)
            for i, seller in enumerate(uncontacted):
                t_send = time.perf_counter()
                try:
                    account_name, state = await asyncio.wait_for(
                        account_pool.checkout(), timeout=180)
                except asyncio.TimeoutError:
                    await _emit(on_log, "очередь аккаунтов недоступна 180с — остановка", "error")
                    break
                if not getattr(account_pool, "is_fresh", lambda name: True)(account_name):
                    # Refresh failed in checkout — skip instead of burning
                    # a browser launch that ensure_logged_in will reject.
                    await _emit(on_log, f"[{account_name}] пропуск — сессия мертва", "warning")
                    await account_pool.release(account_name)
                    continue
                log.info(f"[{account_name}] → {seller['seller_name']} ({i + 1}/{len(uncontacted)}) ...")
                ctx = None
                try:
                    # Inside try: a new_context failure must still release()
                    # the account, otherwise the pool drains into deadlock.
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
                    success = await _send_one(page, seller, template, account_name, on_log)
                    el = time.perf_counter() - t_send
                    if success:
                        await mark_seller_contacted(conn, seller["seller_id"])
                        sent_count += 1
                        await _emit(on_log, f"[{account_name}] ✓ отправлено → {seller['seller_name']} ({el:.0f}с)")
                    else:
                        await _emit(on_log, f"[{account_name}] ✗ не отправлено → {seller['seller_name']} ({el:.0f}с)", "warning")
                except Exception as e:
                    await _emit(on_log, f"[{account_name}] критическая ошибка: {e}", "error")
                finally:
                    if ctx is not None:
                        try:
                            await ctx.close()
                        except Exception:
                            pass
                    await account_pool.release(account_name)
                if progress:
                    progress.advance(progress.task_ids[0])
                if delay and i < len(uncontacted) - 1:
                    await asyncio.sleep(delay)
    except Exception as e:
        await _emit(on_log, f"критическая ошибка запуска браузера: {e}", "error")
    finally:
        if browser is not None:
            try:
                await browser.close()
            except Exception:
                pass
    log.info(f"Рассылка завершена: {sent_count}/{len(uncontacted)} отправлено")
    return sent_count
