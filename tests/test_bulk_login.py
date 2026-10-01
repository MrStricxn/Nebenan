import logging

import pytest

from src.webapi import _parse_bulk_lines, _unique_cookie_name, _do_bulk_login
import src.webapi as webapi
import src.accounts as accounts_mod
from src.accounts import CredentialStore


def test_parse_valid_lines():
    pairs, errors = _parse_bulk_lines([
        "a@b.de:pass123",
        "c@d.de:we:ird:pass,",
        "  e@f.de:  spaced  ",
        "",
        "   ",
    ])
    assert errors == []
    assert pairs == [("a@b.de", "pass123"), ("c@d.de", "we:ird:pass,"),
                     ("e@f.de", "spaced")]


def test_parse_invalid_lines():
    pairs, errors = _parse_bulk_lines([
        "no-colon-here",
        "not-an-email:pass",
        "a@b.de:",
        "@:pass",
    ])
    assert pairs == []
    assert len(errors) == 4


def test_parse_caps():
    long_email = "a" * 250 + "@b.de"
    pairs, errors = _parse_bulk_lines([f"{long_email}:p", "a@b.de:" + "p" * 513])
    assert pairs == []
    assert len(errors) == 2


def test_unique_cookie_name():
    # NB: _safe_cookie_name splitext()s first — "a@b.de" → base "a@b" → "ab".
    assert _unique_cookie_name("a@b.de", set()) == "ab"
    assert _unique_cookie_name("a@b.de", {"ab"}) == "ab-2"
    assert _unique_cookie_name("a@b.de", {"ab", "ab-2"}) == "ab-3"


class _StubPool:
    def __init__(self, creds_path):
        self._accounts = {}
        self.creds = CredentialStore(str(creds_path))


@pytest.mark.asyncio
async def test_bulk_worker_ok_skip_fail(tmp_path, monkeypatch, caplog):
    pool = _StubPool(tmp_path / "logins.json")
    pool._accounts["havede"] = {"cookies": []}  # pre-existing (dup of have@de)

    async def fake_relogin(pool_arg, name, email, password):
        if email == "bad@x.de":
            return False
        # mirror real relogin(): live pool pickup + nothing else
        pool_arg._accounts[name] = {"cookies": []}
        return True

    monkeypatch.setattr(accounts_mod, "relogin", fake_relogin)
    monkeypatch.setattr(webapi.state, "pool", pool)
    monkeypatch.setattr(webapi.state, "task_running", False)

    pairs = [("have@de", "old"), ("new@x.de", "s3cret!"), ("bad@x.de", "pw")]
    with caplog.at_level(logging.INFO):
        await _do_bulk_login(pairs)

    assert "newx" in pool._accounts  # added live
    assert pool.creds.get("newx") == ("new@x.de", "s3cret!")  # autologin saved, password intact
    assert pool.creds.get("bad@x.de") is None  # failed → no creds
    assert webapi.state.task_running is False  # flag cleared
    assert "s3cret!" not in caplog.text  # passwords never hit the logs


@pytest.mark.asyncio
async def test_bulk_worker_no_pool(tmp_path, monkeypatch):
    monkeypatch.setattr(webapi.state, "pool", None)
    monkeypatch.setattr(webapi.state, "task_running", False)
    await _do_bulk_login([("a@b.de", "pw")])
    assert webapi.state.task_running is False
