import pytest
import pytest_asyncio
import base64
import json
import os
import time
from src.accounts import AccountPool, CredentialStore, _jwt_exp, _relogin_attempts, _relogin_launch_mode

FIXTURE_DIR = "tests/fixtures/cookies"


@pytest.mark.asyncio
async def test_load_accounts():
    pool = AccountPool(FIXTURE_DIR)
    await pool.load()
    assert pool.total == 1
    assert pool.available == 1


@pytest.mark.asyncio
async def test_checkout_and_release():
    pool = AccountPool(FIXTURE_DIR)
    await pool.load()
    name, state = await pool.checkout()
    assert name == "valid_account"
    assert "cookies" in state
    assert pool.available == 0
    await pool.release(name)
    assert pool.available == 1


@pytest.mark.asyncio
async def test_empty_cookies_dir(tmp_path):
    pool = AccountPool(str(tmp_path))
    await pool.load()
    assert pool.total == 0


@pytest.mark.asyncio
async def test_invalid_json_skipped(tmp_path):
    bad = tmp_path / "broken.json"
    bad.write_text("not valid json")
    pool = AccountPool(str(tmp_path))
    await pool.load()
    assert pool.total == 0


def _b64(obj) -> str:
    return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")


def _fake_jwt(exp: int) -> str:
    return f"{_b64({'alg': 'none'})}.{_b64({'exp': exp, 'iat': exp - 900, 'sub': '1'})}.sig"


def test_jwt_exp():
    assert _jwt_exp(_fake_jwt(1234567890)) == 1234567890
    assert _jwt_exp("not-a-jwt") is None
    assert _jwt_exp("") is None


def _write_state(tmp_path, at: str, rt: str = "old-rt"):
    now = int(time.time())
    state = {"cookies": [
        {"name": "at", "value": at, "domain": ".nebenan.de", "path": "/",
         "expires": now + 900},
        {"name": "rt", "value": rt, "domain": ".nebenan.de", "path": "/",
         "expires": now + 60 * 86400},
        {"name": "s", "value": "sess", "domain": ".nebenan.de", "path": "/",
         "expires": now + 90 * 86400},
    ], "origins": []}
    (tmp_path / "a.json").write_text(json.dumps(state), encoding="utf-8")


class _FakeResp:
    def __init__(self, status, payload):
        self.status_code = status
        self._payload = payload
        self.text = json.dumps(payload)
    def json(self):
        return self._payload


class _FakeClient:
    def __init__(self, status, payload):
        self.status = status
        self.payload = payload
        self.calls = 0
    async def post(self, *args, **kwargs):
        self.calls += 1
        return _FakeResp(self.status, self.payload)


@pytest.mark.asyncio
async def test_ensure_fresh_skips_healthy(tmp_path, monkeypatch):
    _write_state(tmp_path, _fake_jwt(int(time.time()) + 3600))
    pool = AccountPool(str(tmp_path))
    await pool.load()
    fake = _FakeClient(200, {})
    monkeypatch.setattr("src.accounts.get_client", lambda: fake)
    assert await pool.ensure_fresh("a") is True
    assert fake.calls == 0  # healthy `at` → no HTTP at all


@pytest.mark.asyncio
async def test_ensure_fresh_refreshes_expiring(tmp_path, monkeypatch):
    new_exp = int(time.time()) + 900
    new_at = _fake_jwt(new_exp)
    new_rt = "r" * 64  # realistic length (real rt is 86 chars)
    fake = _FakeClient(200, {"pair": {"access_token": new_at,
                                      "refresh_token": new_rt}})
    monkeypatch.setattr("src.accounts.get_client", lambda: fake)
    _write_state(tmp_path, _fake_jwt(int(time.time()) + 60))
    pool = AccountPool(str(tmp_path))
    await pool.load()
    assert await pool.ensure_fresh("a") is True
    assert fake.calls == 1
    saved = json.loads((tmp_path / "a.json").read_text(encoding="utf-8"))
    by_name = {c["name"]: c for c in saved["cookies"]}
    assert by_name["at"]["value"] == new_at
    assert by_name["at"]["expires"] == new_exp
    assert by_name["rt"]["value"] == new_rt
    assert by_name["s"]["value"] == "sess"  # untouched


@pytest.mark.asyncio
async def test_ensure_fresh_401_dead(tmp_path, monkeypatch):
    fake = _FakeClient(401, {"failures": ["invalid"]})
    monkeypatch.setattr("src.accounts.get_client", lambda: fake)
    old_at = _fake_jwt(int(time.time()) + 60)
    _write_state(tmp_path, old_at)
    pool = AccountPool(str(tmp_path))
    await pool.load()
    assert await pool.ensure_fresh("a") is False
    saved = json.loads((tmp_path / "a.json").read_text(encoding="utf-8"))
    by_name = {c["name"]: c for c in saved["cookies"]}
    assert by_name["at"]["value"] == old_at  # burned rt → file untouched


@pytest.mark.asyncio
async def test_checkout_no_http_for_non_jwt(tmp_path, monkeypatch):
    _write_state(tmp_path, "opaque-token")
    pool = AccountPool(str(tmp_path))
    await pool.load()

    async def _boom(*args, **kwargs):
        raise AssertionError("no HTTP expected")
    monkeypatch.setattr("src.accounts.get_client", lambda: _FakeClientBoom(_boom))
    name, state = await pool.checkout()
    assert name == "a"
    await pool.release(name)


class _FakeClientBoom:
    def __init__(self, fn):
        self._fn = fn
    async def post(self, *args, **kwargs):
        return await self._fn(*args, **kwargs)


def test_cred_store_roundtrip(tmp_path):
    store = CredentialStore(str(tmp_path / "logins.json"))
    assert store.get("a") is None
    assert store.configured() == []  # missing file → empty, no crash
    store.set("a", "a@x.de", "secret1")
    assert store.get("a") == ("a@x.de", "secret1")
    assert store.configured() == ["a"]
    store.set("b", "b@x.de", "secret2")
    assert sorted(store.configured()) == ["a", "b"]
    store.delete("a")
    assert store.get("a") is None
    assert store.configured() == ["b"]
    assert "secret" not in json.dumps(store.configured())  # no password leak


@pytest.mark.asyncio
async def test_ensure_fresh_relogin_fallback(tmp_path, monkeypatch):
    _write_state(tmp_path, _fake_jwt(int(time.time()) + 60))  # expiring at
    pool = AccountPool(str(tmp_path), creds_path=str(tmp_path / "logins.json"))
    await pool.load()
    pool.creds.set("a", "a@x.de", "pw")
    fake = _FakeClient(401, {"failures": ["invalid"]})  # burned rt
    monkeypatch.setattr("src.accounts.get_client", lambda: fake)
    calls = []
    async def _fake_relogin(p, name, email, password):
        calls.append((name, email, password))
        return True
    monkeypatch.setattr("src.accounts.relogin", _fake_relogin)
    _relogin_attempts.pop("a", None)
    try:
        assert await pool.ensure_fresh("a") is True
        assert calls == [("a", "a@x.de", "pw")]
        # cooldown: immediate retry must not spawn another browser
        assert await pool.ensure_fresh("a") is False
        assert len(calls) == 1
    finally:
        _relogin_attempts.pop("a", None)


def test_relogin_launch_mode_defaults(monkeypatch):
    monkeypatch.delenv("RELOGIN_HEADLESS", raising=False)
    monkeypatch.delenv("RELOGIN_DIRECT", raising=False)
    assert _relogin_launch_mode() == (False, False)


def test_relogin_launch_mode_overrides(monkeypatch):
    monkeypatch.setenv("RELOGIN_HEADLESS", "1")
    monkeypatch.setenv("RELOGIN_DIRECT", "0")
    assert _relogin_launch_mode() == (True, True)
