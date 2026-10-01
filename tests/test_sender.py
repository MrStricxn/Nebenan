import pytest
import pytest_asyncio
import os
from unittest.mock import AsyncMock, MagicMock
from src.sender import _filter_uncontacted, _send_one, _goto_with_retry, _is_listing_deleted
from src.proxy import is_ip_blocked
from src.db import init_db, upsert_listing, mark_seller_contacted

TEST_DB = "data/test_sender.db"

@pytest_asyncio.fixture
async def conn():
    db = await init_db(TEST_DB)
    yield db
    await db.close()
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

@pytest.mark.asyncio
async def test_filter_removes_already_contacted(conn):
    await upsert_listing(conn, {
        "listing_id": "l1", "seller_id": "s1", "seller_name": "Hans",
        "title": "Sofa", "url": "https://nebenan.de/l1", "published_at": "2026-06-29T10:00:00"
    })
    await mark_seller_contacted(conn, "s1")
    await upsert_listing(conn, {
        "listing_id": "l2", "seller_id": "s2", "seller_name": "Anna",
        "title": "Tisch", "url": "https://nebenan.de/l2", "published_at": "2026-06-29T10:00:00"
    })
    sellers = [
        {"seller_id": "s1", "seller_name": "Hans", "url": "https://nebenan.de/l1"},
        {"seller_id": "s2", "seller_name": "Anna", "url": "https://nebenan.de/l2"},
    ]
    result = await _filter_uncontacted(conn, sellers)
    assert len(result) == 1
    assert result[0]["seller_id"] == "s2"

@pytest.mark.asyncio
async def test_filter_empty_list(conn):
    result = await _filter_uncontacted(conn, [])
    assert result == []


class _BlockedResp:
    status = 403


class _BlockedPage:
    """goto returns 403; locator must never be reached (fast-fail)."""
    async def goto(self, *a, **k):
        return _BlockedResp()

    def locator(self, *a, **k):
        raise AssertionError("locator must not be reached on HTTP 403")


@pytest.mark.asyncio
async def test_send_one_fast_fails_on_403():
    seen = []
    async def on_log(text, level="info"):
        seen.append(text)
    seller = {"seller_id": "1", "seller_name": "T",
              "listing_url": "https://nebenan.de/x",
              "message_url": "https://nebenan.de/messages/1"}
    assert await _send_one(_BlockedPage(), seller, "hi", "acc14", on_log) is False
    assert any("403" in m for m in seen)


def test_is_ip_blocked_none_and_ok():
    assert is_ip_blocked(None) is False
    assert is_ip_blocked(_BlockedResp()) is True


class _FlakyPage:
    """goto fails `fails` times, then returns `resp`."""
    def __init__(self, fails, resp=None):
        self.fails = fails
        self.resp = resp
        self.calls = 0

    async def goto(self, *a, **k):
        self.calls += 1
        if self.calls <= self.fails:
            raise TimeoutError("goto timeout")
        return self.resp


@pytest.mark.asyncio
async def test_goto_with_retry_succeeds_second_attempt():
    page = _FlakyPage(fails=1, resp=_BlockedResp())
    assert await _goto_with_retry(page, "https://nebenan.de/x", what="t") is not None
    assert page.calls == 2


@pytest.mark.asyncio
async def test_goto_with_retry_gives_up_after_two():
    page = _FlakyPage(fails=9)
    assert await _goto_with_retry(page, "https://nebenan.de/x", what="t") is None
    assert page.calls == 2


class _FakeCtx:
    def __init__(self, browser):
        self.browser = browser
        self.closed = False

    async def new_page(self):
        return object()

    async def close(self):
        self.closed = True


class _FakeBrowser:
    def __init__(self):
        self.contexts = []
        self.closed = False

    async def new_context(self, **kwargs):
        ctx = _FakeCtx(self)
        self.contexts.append(ctx)
        return ctx

    async def close(self):
        self.closed = True


class _FakePW:
    def __init__(self):
        self.browser = _FakeBrowser()
        self.launches = 0
        self.chromium = self

    async def launch(self, **kwargs):
        self.launches += 1
        return self.browser

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


@pytest.mark.asyncio
async def test_send_messages_one_browser_sequential(conn, monkeypatch):
    """One browser launch for N sellers, strict submission order."""
    import src.sender as sender_mod

    sellers = [
        {"seller_id": f"s{i}", "seller_name": f"N{i}",
         "message_url": f"https://nebenan.de/messages/{i}"}
        for i in range(3)
    ]
    order = []
    fake_pw = _FakePW()

    async def fake_send_one(page, seller, template, account_name="?", on_log=None):
        order.append(seller["seller_id"])
        return True

    class _FakePool:
        total = 1

        async def checkout(self):
            return ("acc1", {"cookies": []})

        async def release(self, name):
            pass

        def is_fresh(self, name):
            return True

    class _FakeLoader:
        def get_random(self):
            return "hi"

    monkeypatch.setattr(sender_mod, "async_playwright", lambda: fake_pw)
    monkeypatch.setattr(sender_mod, "_send_one", fake_send_one)
    monkeypatch.setattr(sender_mod, "playwright_proxy", lambda: None)

    result = await sender_mod.send_messages(
        sellers, _FakePool(), _FakeLoader(), conn, delay=0)
    assert result == 3
    assert order == ["s0", "s1", "s2"]
    assert fake_pw.launches == 1
    assert len(fake_pw.browser.contexts) == 3
    assert all(c.closed for c in fake_pw.browser.contexts)
    assert fake_pw.browser.closed is True


def test_is_listing_deleted_positive():
    html = ("<html><body><h1>Diese Seite gibt es nicht</h1>"
            "<p>Das tut uns sehr leid.</p></body></html>")
    assert _is_listing_deleted(html) is True


def test_is_listing_deleted_negative():
    assert _is_listing_deleted("<html><body>Verkaufe Sofa</body></html>") is False
    assert _is_listing_deleted("") is False
    assert _is_listing_deleted(None) is False
    # The sympathy line alone appears on other error pages — must not skip.
    assert _is_listing_deleted("<p>Das tut uns sehr leid.</p>") is False


class _OkResp:
    status = 200


class _DeletedPage:
    """Listing goto returns 200 with the deleted-notice HTML."""

    def __init__(self):
        self.locator_reached = False

    async def goto(self, *a, **k):
        return _OkResp()

    async def content(self):
        return "<h1>Diese Seite gibt es nicht</h1>"

    def locator(self, *a, **k):
        self.locator_reached = True
        raise AssertionError("locator must not be reached on deleted listing")


@pytest.mark.asyncio
async def test_send_one_skips_deleted_listing():
    seen = []
    async def on_log(text, level="info"):
        seen.append(text)
    page = _DeletedPage()
    seller = {"seller_id": "s9", "seller_name": "Gone",
              "listing_url": "https://nebenan.de/p/123"}
    result = await _send_one(page, seller, "hi", "acc1", on_log)
    assert result is False
    assert page.locator_reached is False
    assert any("удалено" in t for t in seen)
