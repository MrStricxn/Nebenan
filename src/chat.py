import logging
from playwright.async_api import async_playwright  # browser flows only (send_with_photos)

from src.http import api_request, api_request_ex
from src.proxy import playwright_proxy, ensure_logged_in, is_ip_blocked
from src.sender import _dismiss_consent, _submit_click, _goto_with_retry

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


async def _request(method: str, url: str, token: str, body: dict | None = None,
                   account_name: str = "") -> dict | list | None:
    headers = _WRITE_HEADERS if body is not None else _READ_HEADERS
    return await api_request(method, url, token, body=body, extra_headers=headers,
                             account_name=account_name)


async def fetch_conversations(token: str, page: int = 1, per_page: int = 30,
                              account_name: str = "") -> list[dict]:
    d = await _request("GET", f"{_BASE}/api/v2/private_conversations.json?page={page}&per_page={per_page}", token,
                       account_name=account_name)
    if not d:
        return []
    return d.get("private_conversations", []) if isinstance(d, dict) else d


async def fetch_messages(token: str, partner_id: int, per_page: int = 50,
                         account_name: str = "") -> dict:
    d = await _request("GET", f"{_BASE}/api/v2/private_conversations/{partner_id}.json?per_page={per_page}", token,
                       account_name=account_name)
    return d or {}


async def fetch_profile(token: str, account_name: str = "") -> dict:
    d = await _request("GET", f"{_BASE}/api/core/v3/profile", token,
                       account_name=account_name)
    return d or {}


async def send_message(token: str, receiver_id: int, text: str,
                       account_name: str = "") -> tuple[bool, int]:
    """Send text via direct API POST — no browser needed.
    Returns (ok, status): status 0 = network/timeout, 401 = session expired."""
    payload = {
        "private_conversation_message": {
            "body":        text,
            "images":      [],
            "embeddables": [],
            "receiver_id": receiver_id,
        },
        "user_agent": _USER_AGENT,
    }
    result, status, _txt = await api_request_ex(
        "POST", _SEND_URL, token, body=payload,
        extra_headers=_WRITE_HEADERS, account_name=account_name)
    ok = result is not None
    if ok:
        log.info(f"send_message → {receiver_id}: OK")
    else:
        log.warning(f"send_message → {receiver_id}: FAILED (status {status})")
    return ok, status


_LAUNCH_ARGS = ["--disable-blink-features=AutomationControlled", "--no-sandbox", "--disable-dev-shm-usage"]
_TA_SEL      = "textarea[data-testid='c-message_form-textfield']"
_BTN_SEL     = "button[data-testid='c-message_form-submit']"
_FORM_SEL    = "div.c-messages-messageForm"
_TRIGGER     = "el => { el.dispatchEvent(new Event('focus',{bubbles:true})); el.dispatchEvent(new Event('input',{bubbles:true})); el.dispatchEvent(new Event('change',{bubbles:true})) }"
# Self-diagnostics when the submit button never appears (next failure explains itself)
_FORM_DIAG_JS = """() => {
  const ta = document.querySelector("textarea[data-testid='c-message_form-textfield']");
  const fi = document.querySelector("div.c-messages-messageForm input[type='file']") || document.querySelector("input[type='file']");
  const btn = document.querySelector("button[data-testid='c-message_form-submit']");
  const r = btn ? btn.getBoundingClientRect() : null;
  return JSON.stringify({
    taLen: ta ? ta.value.length : -1,
    files: fi && fi.files ? fi.files.length : -1,
    btnRect: r ? {x: Math.round(r.x), y: Math.round(r.y), w: Math.round(r.width), h: Math.round(r.height)} : null,
    btnHidden: btn ? (btn.offsetParent === null) : null,
    consent: document.querySelectorAll("iframe[title='SP Consent message']").length,
  });
}"""


async def send_with_photos(storage_state: dict, receiver_id: int, text: str,
                         photo_paths: list[str], account_name: str = "?") -> tuple[bool, int, int]:
    """Send a message with attached photos via Playwright browser.

    Returns (sent, attached, expected): callers must distinguish
    "message sent with photos" from "message sent but photos dropped".
    """
    import os
    valid_paths = [p for p in (photo_paths or []) if p and os.path.isfile(p)]
    expected = len(valid_paths)
    attached = 0
    if photo_paths and not valid_paths:
        log.warning(f"send_with_photos → {receiver_id}: no valid photo files, sending text only")
    # Photo+text sends run VISIBLE (headless=False): the operator watches
    # the flow live. (Under Xvfb on Railway this uses the virtual display.)
    headless = False
    browser = None
    async with async_playwright() as pw:
        try:
            launch_kwargs: dict = {"headless": headless, "args": _LAUNCH_ARGS}
            _proxy = playwright_proxy()
            if _proxy:
                # Global proxy at launch: per-context proxy without it raises
                # "Browser needs to be launched with the global proxy".
                launch_kwargs["proxy"] = _proxy
            browser = await pw.chromium.launch(**launch_kwargs)
            ctx_kwargs = {
                "storage_state": storage_state,
                "user_agent": _USER_AGENT,
            }
            ctx = await browser.new_context(**ctx_kwargs)
            page = await ctx.new_page()
            try:
                resp = await _goto_with_retry(
                    page, f"https://nebenan.de/messages/{receiver_id}",
                    what=f"send_with_photos({receiver_id})")
                if resp is None:
                    return False, attached, expected
                if is_ip_blocked(resp):
                    log.warning(f"send_with_photos({receiver_id}): IP прокси заблокирован (403) — ротируйте прокси")
                    return False, attached, expected
                if not await ensure_logged_in(page, account_name, where=" (send-photo)"):
                    return False, attached, expected
                await _dismiss_consent(page)
                ta = page.locator(_TA_SEL)
                try:
                    await ta.wait_for(state="visible", timeout=30000)
                except Exception:
                    # Don't burn the diagnosis: record WHAT page we are on
                    # (block page? redirect? unrendered app?) for the next step.
                    url, title = "", ""
                    try:
                        url, title = page.url, await page.title()
                    except Exception:
                        pass
                    log.error(f"send_with_photos({receiver_id}): no textarea, url={url} title={title}")
                    return False, attached, expected

                # Attach photos via the file input inside the message form
                # (scoped — the page can contain unrelated file inputs).
                # NOTE: nebenan uploads each file immediately on select
                # (POST .../images/private_conversation_message/upload.json,
                # answered 201) and clears the input by design — so
                # input.files.length is ALWAYS 0 afterwards and proves
                # nothing. The only reliable "attached" signal is the 201
                # upload response, and submit MUST wait for it: clicking
                # submit earlier sends a text-only message while the
                # uploaded image becomes an orphan.
                _UPLOAD_URL = "images/private_conversation_message/upload"
                form_files = page.locator(f"{_FORM_SEL} input[type='file']")
                file_input = form_files.first if await form_files.count() else page.locator("input[type='file']").first
                if valid_paths:
                    _uploaded: list[int] = []

                    def _on_upload(resp, _u=_UPLOAD_URL):
                        try:
                            if _u in resp.url and resp.request.method == "POST":
                                _uploaded.append(resp.status)
                        except Exception:
                            pass

                    try:
                        # Listener FIRST: uploads fire immediately on select.
                        page.on("response", _on_upload)
                        await file_input.set_input_files(valid_paths, timeout=30000)
                    except Exception as e:
                        log.warning(f"send_with_photos: file input failed ({e}), sending text only")
                        valid_paths = []
                    else:
                        # Bounded wait: one 201 per file (slow proxy → up to
                        # 45s per upload). Page/context closes right after,
                        # so the listener needs no removal.
                        waited = 0.0
                        budget = max(45.0, len(valid_paths) * 45.0)
                        while len(_uploaded) < len(valid_paths) and waited < budget:
                            await page.wait_for_timeout(500)
                            waited += 0.5
                        for s in _uploaded:
                            if s in (200, 201):
                                attached += 1
                            else:
                                log.warning(f"send_with_photos: upload → HTTP {s}")
                        if attached < len(valid_paths):
                            log.warning(
                                f"send_with_photos: only {attached}/{len(valid_paths)} uploads "
                                f"confirmed after {waited:.0f}s"
                            )
                    log.info(f"send_with_photos: {attached}/{len(valid_paths)} files uploaded")

                # fill() does not always update React-controlled state, leaving
                # the form 'empty' (submit stays hidden) — verify and retype.
                if text:
                    # The consent dialog can (re)appear during the long upload —
                    # dismiss again, then click. If the overlay still intercepts
                    # the pointer, fall back to a DOM click (no hit-testing).
                    await _dismiss_consent(page)
                    try:
                        await ta.click(timeout=8000)
                    except Exception:
                        await ta.evaluate("el => el.click()")
                    await ta.fill(text)
                    try:
                        cur = await ta.input_value()
                    except Exception:
                        cur = ""
                    if len(cur.strip()) < min(len(text.strip()), 10):
                        await ta.fill("")
                        await ta.press_sequentially(text, delay=10)
                    await ta.evaluate(_TRIGGER)
                    await page.wait_for_timeout(500)

                # The submit button is hidden until the form registers content
                # (text or a fully uploaded photo) — wait for visibility instead
                # of a fixed sleep (uploads over proxy can take a while).
                submit = page.locator(_BTN_SEL)
                try:
                    await submit.wait_for(state="visible", timeout=45000)
                except Exception:
                    diag = ""
                    try:
                        diag = await page.evaluate(_FORM_DIAG_JS)
                    except Exception:
                        pass
                    log.error(f"send_with_photos({receiver_id}): submit stayed hidden, diag={diag}")
                    return False, attached, expected
                await _submit_click(page, submit, what=f"send_with_photos({receiver_id})")
                await page.wait_for_timeout(1500)
                log.info(f"send_with_photos → {receiver_id}: OK ({attached} фото)")
                return True, attached, expected
            except Exception as e:
                log.error(f"send_with_photos({receiver_id}): {e}")
                return False, attached, expected
        except Exception as e:
            log.error(f"send_with_photos launch({receiver_id}): {e}")
            return False, attached, expected
        finally:
            if browser is not None:
                try:
                    await browser.close()
                except Exception:
                    pass
