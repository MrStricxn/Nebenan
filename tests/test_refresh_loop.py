import pytest
from unittest.mock import AsyncMock, patch

import src.webapi as webapi
from src.webapi import _refresh_once, _detect_ban, account_health, _record_auth_event, refresh_account_health


# ─── Ban detection ────────────────────────────────────────────────────────

def _body(**fields) -> str:
    """Build a minimal 401 profile JSON like nebenan returns."""
    prof = {"email": "test@example.com",
            "authentication_token": None,
            "status": {"is_email_verified": True}}
    prof.update(fields)
    return __import__("json").dumps(prof)


def test_detects_is_blocked():
    hits = _detect_ban(_body(is_blocked=True))
    assert "is_blocked" in hits


def test_detects_deactivated_at():
    hits = _detect_ban(_body(deactivated_at="2026-01-01"))
    assert "deactivated_at" in hits


def test_detects_ban_hint_in_string():
    hits = _detect_ban(_body(status={"note": "account is banned by moderation"}))
    assert hits


def test_empty_body_no_crash():
    assert _detect_ban("") == []


# ─── Health tracking ──────────────────────────────────────────────────────

def _make_h(name: str, kind: str = "ok", status: int = 200,
            signals: list | None = None, consecutive: int = 0,
            email: str = "a@b.c", chat_unavailable: bool = False):
    h = {"last_kind": kind, "last_at": "2026-01-01T00:00:00",
         "last_status": status, "last_body": "",
         "consecutive_401s": consecutive, "ban_signals": signals or [],
         "last_ok_at": "2026-01-01T00:00:00",
         "email": email, "chat_unavailable": chat_unavailable}
    webapi._account_health[name] = h


def test_account_health_ok():
    _make_h("alice")
    a = account_health("alice")
    assert a["verdict"] == "ok"
    assert a["consecutive_401s"] == 0


def test_account_health_expired():
    _make_h("bob", kind="auth_401", status=401, consecutive=3)
    a = account_health("bob")
    assert a["verdict"] == "expired"
    assert a["consecutive_401s"] == 3


def test_account_health_banned():
    _make_h("eve", kind="auth_401", status=401, signals=["status.is_blocked"],
            consecutive=12)
    a = account_health("eve")
    assert a["verdict"] == "banned"
    assert "status.is_blocked" in a["ban_signals"]


def test_account_health_offline():
    _make_h("carl", kind="network", status=0, chat_unavailable=True)
    a = account_health("carl")
    assert a["verdict"] == "offline"
    assert a["chat_unavailable"] is True


def test_record_auth_event_401_parses_email():
    webapi._account_health.pop("alice", None)
    body = __import__("json").dumps(
        {"email": "me@example.com", "authentication_token": None,
         "status": {"is_email_verified": True}})
    _record_auth_event("alice", "GET", "https://api.nebenan.de/x", 401, body)
    assert account_health("alice")["email"] == "me@example.com"
    assert account_health("alice")["last_kind"] == "auth_401"


def test_record_auth_event_401_no_email_still_ok():
    webapi._account_health.pop("alice", None)
    _record_auth_event("alice", "GET", "https://api.nebenan.de/x", 401, "")
    assert account_health("alice")["last_kind"] == "auth_401"


def test_record_auth_event_network():
    webapi._account_health.pop("alice", None)
    _record_auth_event("alice", "GET", "https://api.nebenan.de/x", 0, "timeout")
    assert account_health("alice")["last_kind"] == "network"
    assert account_health("alice")["consecutive_401s"] == 0


def test_record_auth_event_ok_resets():
    _make_h("alice", kind="auth_401", status=401, consecutive=5)
    _record_auth_event("alice", "GET", "https://api.nebenan.de/x", 200, "{}")
    assert account_health("alice")["consecutive_401s"] == 0
    assert account_health("alice")["last_kind"] == "ok"


def test_refresh_account_health_keys(monkeypatch):
    webapi._account_health.clear()
    _make_h("alice")
    _make_h("bob")
    health = refresh_account_health()
    assert set(health) == {"alice", "bob"}


# ─── Existing refresh-loop tests ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_refresh_once_hits_all_accounts(monkeypatch):
    pool = AsyncMock()
    pool._accounts = {"a": {}, "b": {}}
    pool.ensure_fresh = AsyncMock(return_value=True)
    monkeypatch.setattr(webapi.state, "pool", pool)
    await _refresh_once()
    assert pool.ensure_fresh.await_count == 2
    called = {c.args[0] for c in pool.ensure_fresh.await_args_list}
    assert called == {"a", "b"}


@pytest.mark.asyncio
async def test_refresh_once_no_pool(monkeypatch):
    monkeypatch.setattr(webapi.state, "pool", None)
    await _refresh_once()  # must not raise


@pytest.mark.asyncio
async def test_refresh_once_survives_account_error(monkeypatch):
    pool = AsyncMock()
    pool._accounts = {"a": {}, "b": {}}
    pool.ensure_fresh = AsyncMock(side_effect=[False, True])
    monkeypatch.setattr(webapi.state, "pool", pool)
    await _refresh_once()  # must not raise; failure is only logged
    assert pool.ensure_fresh.await_count == 2
