"""Tests for Sourcepoint consent dismissal (sp_choice_type_11 campaigns)."""
import asyncio

from src.sender import _CONSENT_ACCEPT_LABELS, _dismiss_consent


def test_ich_stimme_zu_label_covered():
    assert "Ich stimme zu" in _CONSENT_ACCEPT_LABELS


class _Btn:
    def __init__(self, owner, key):
        self._owner = owner
        self._key = key

    @property
    def first(self):
        return self

    async def click(self, timeout=None):
        self._owner.clicked.append(self._key)

    async def evaluate(self, js):
        self._owner.clicked.append(self._key + ":js")


class _BtnList:
    def __init__(self, owner, key, present):
        self._btn = _Btn(owner, key)
        self._present = present

    @property
    def first(self):
        return self._btn

    async def count(self):
        return 1 if self._present else 0


class _FrameLoc:
    def __init__(self, page):
        self._page = page

    async def count(self):
        return 0 if self._page.gone else 1

    def content_frame(self):
        return self._page.frame

    @property
    def first(self):
        return self

    async def wait_for(self, state=None, timeout=None):
        if self._page.gone:
            return
        raise TimeoutError("still attached")


class _Frame:
    def __init__(self, owner):
        self._owner = owner

    def get_by_role(self, role=None, name=None):
        return _BtnList(self._owner, f"label:{name}", False)

    def locator(self, sel):
        if sel == "button.sp_choice_type_11":
            return _BtnList(self._owner, "sp11", True)
        return _BtnList(self._owner, sel, False)


class _Keyboard:
    async def press(self, key):
        pass


class _Page:
    def __init__(self):
        self.clicked = []
        self.gone = False
        self.frame = _Frame(self)
        self.keyboard = _Keyboard()

    def locator(self, sel):
        assert sel == "iframe[title='SP Consent message']"
        return _FrameLoc(self)


def _click_marks_gone(page):
    orig = _Btn.click

    async def patched(self, timeout=None):
        await orig(self, timeout)
        page.gone = True

    return patched


def test_sp_choice_type_11_fallback_click(monkeypatch):
    page = _Page()
    monkeypatch.setattr(_Btn, "click", _click_marks_gone(page))
    assert asyncio.run(_dismiss_consent(page)) is True
    assert "sp11" in page.clicked


def test_no_iframe_returns_true():
    page = _Page()
    page.gone = True
    assert asyncio.run(_dismiss_consent(page)) is True
