import asyncio
import base64
import json
import logging
import os
import threading
import time
from pathlib import Path

import httpx

from src.http import get_client

log = logging.getLogger("nebena")

# Reverse-engineered from the nebenan.de web app (goodhood SDK):
# POST {base}/api/core/v3/tokens {"refresh_token": <rt>} → pair.access_token.
# `at` is a JWT living exactly 15 min, `rt` an opaque 60-day token.
_TOKENS_URL = "https://api.nebenan.de/api/core/v3/tokens"
_REFRESH_MARGIN = 5 * 60  # refresh `at` when less than this much life remains
_RELOGIN_COOLDOWN = 10 * 60  # min seconds between auto-login attempts per account
_CRED_PATH = "logins.json"  # never served, never logged, gitignored
_BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
               "AppleWebKit/537.36 (KHTML, like Gecko) "
               "Chrome/124.0.0.0 Safari/537.36")

# Last auto-login attempt: name → unix time. Stops every checkout of a
# dead account from spawning a Chromium when credentials keep failing.
_relogin_attempts: dict[str, float] = {}


class CredentialStore:
    """login:pass per account, filled via the dashboard (never via git).

    File format: {"account": {"email": "...", "password": "..."}}.
    Passwords are stored in the clear (auto-login is impossible otherwise)
    and are NEVER returned by any API — only set/unset flags leave this class.
    """

    def __init__(self, path: str = _CRED_PATH):
        self._path = Path(path)

    def _load(self) -> dict:
        try:
            data = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    def _save(self, data: dict) -> None:
        tmp = self._path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, self._path)

    def get(self, name: str) -> tuple[str, str] | None:
        entry = self._load().get(name)
        if not isinstance(entry, dict):
            return None
        email = entry.get("email") or ""
        password = entry.get("password") or ""
        if not email or not password:
            return None
        return email, password

    def set(self, name: str, email: str, password: str) -> None:
        data = self._load()
        data[name] = {"email": email, "password": password}
        self._save(data)

    def delete(self, name: str) -> None:
        data = self._load()
        if name in data:
            del data[name]
            self._save(data)

    def configured(self) -> list[str]:
        return [n for n, e in self._load().items()
                if isinstance(e, dict) and e.get("email") and e.get("password")]


def _relogin_launch_mode() -> tuple:
    """(headless, use_proxy) for interactive relogin.

    Relogin is watched by a human (captcha/2FA/manual finish), so the
    default is a VISIBLE browser over a DIRECT connection. Override with
    RELOGIN_HEADLESS=1 / RELOGIN_DIRECT=0.
    """
    headless = os.environ.get("RELOGIN_HEADLESS", "0") == "1"
    direct = os.environ.get("RELOGIN_DIRECT", "1") != "0"
    return headless, not direct


async def relogin(pool: "AccountPool", name: str, email: str, password: str) -> bool:
    """Full login:pass login via a real browser. Never raises.

    Success = session cookie `s` present afterwards; the fresh storage
    state atomically replaces cookies/{name}.json. Any failure (captcha,
    changed form, network) returns False — the caller keeps old behavior.
    """
    try:
        from playwright.async_api import async_playwright
        from src.proxy import playwright_proxy

        _headless, _use_proxy = _relogin_launch_mode()
        async with async_playwright() as pw:
            launch_kwargs: dict = {
                "headless": _headless,
                "args": ["--disable-blink-features=AutomationControlled",
                         "--no-sandbox", "--disable-dev-shm-usage"],
            }
            _proxy = playwright_proxy() if _use_proxy else None
            if _proxy:
                # Global proxy at launch (see chat.py): per-context proxy
                # without it raises Playwright proxy error.
                launch_kwargs["proxy"] = _proxy
            browser = await pw.chromium.launch(**launch_kwargs)
            log.info(f"relogin {name} → browser(headless={_headless}, "
                     f"proxy={'on' if _proxy else 'off'})")
            try:
                kwargs: dict = {"user_agent": _BROWSER_UA}
                ctx = await browser.new_context(**kwargs)
                page = await ctx.new_page()
                await page.goto("https://nebenan.de/login",
                                timeout=30000, wait_until="domcontentloaded")
                from src.sender import _dismiss_consent as _dc
                # Shared iframe-aware dismiss (Sourcepoint campaigns vary;
                # the dialog can also reload late — dismiss again before submit).
                try:
                    await _dc(page)
                except Exception:
                    pass
                # Live form (verified 2026-09-29): email is type="text"
                # (NOT type="email"), both fields carry data-testid.
                await page.locator(
                    '[data-testid="email"], input[name="email"], #email'
                ).first.fill(email, timeout=10000)
                await page.locator(
                    '[data-testid="password"], input[name="password"], #password'
                ).first.fill(password, timeout=10000)
                try:
                    async with page.expect_navigation(wait_until="domcontentloaded",
                                                      timeout=15000):
                        try:
                            await _dc(page)
                        except Exception:
                            pass
                        try:
                            await page.get_by_role("button", name="Anmelden").click(
                                timeout=10000)
                        except Exception:
                            await page.locator(
                                'button[type="submit"], input[type="submit"]'
                            ).first.click(timeout=10000)
                except Exception:
                    pass
                # The operator may finish login by hand in the visible
                # browser (captcha/2FA/manual) — poll for the session
                # cookie instead of checking once after a fixed sleep.
                log.info(f"relogin {name} → waiting up to 120s for session "
                         f"(complete the login in the opened browser if needed)")
                state = None
                cookies: list = []
                for _ in range(60):
                    await page.wait_for_timeout(2000)
                    try:
                        state = await ctx.storage_state()
                    except Exception:
                        continue
                    cookies = state.get("cookies", [])
                    if any(c.get("name") == "s" and "nebenan" in c.get("domain", "")
                           and c.get("value") for c in cookies):
                        break
                    state = None
                if state is None:
                    log.warning(f"relogin {name} → no session cookie after login "
                                f"(captcha? changed form?)")
                    return False
                pool._accounts[name] = {"cookies": cookies,
                                        "origins": state.get("origins", [])}
                pool._persist(name)
                log.info(f"relogin {name} → OK, session renewed via login:pass")
                return True
            finally:
                try:
                    await browser.close()
                except Exception:
                    pass
    except Exception as e:
        log.warning(f"relogin {name} → error ({e})")
        return False


def _normalize_storage_state(raw) -> dict:
    """Convert EditThisCookie array format to Playwright storage_state dict."""
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, list):
        return {"cookies": [], "origins": []}
    cookies = []
    for c in raw:
        same_site = c.get("sameSite", "no_restriction")
        cookie = {
            "name": c["name"],
            "value": c["value"],
            "domain": c["domain"],
            "path": c.get("path", "/"),
            "secure": c.get("secure", False),
            "httpOnly": c.get("httpOnly", False),
            "sameSite": {"no_restriction": "None", "lax": "Lax", "strict": "Strict"}.get(same_site, "None"),
        }
        exp = c.get("expirationDate", 0)
        if exp and exp > 0:
            cookie["expires"] = int(exp)
        cookies.append(cookie)
    return {"cookies": cookies, "origins": []}


def _jwt_exp(token: str) -> int | None:
    """Expiry (unix) from a JWT payload — no signature check needed."""
    try:
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        exp = json.loads(base64.urlsafe_b64decode(payload)).get("exp")
        return int(exp) if exp else None
    except Exception:
        return None


def _find_str(obj, names: tuple[str, ...], exclude: tuple[str, ...] = ()) -> str | None:
    """Recursively find first string value under any of `names` (len>=20)."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in names and isinstance(v, str) and len(v) >= 20 and v not in exclude:
                return v
        for v in obj.values():
            found = _find_str(v, names, exclude)
            if found:
                return found
    elif isinstance(obj, list):
        for v in obj:
            found = _find_str(v, names, exclude)
            if found:
                return found
    return None


async def _mint_pair(rt: str, name: str = "") -> tuple[str, str | None] | None:
    """Exchange `rt` for a fresh (at, rt?) pair. None = failed/burned."""
    try:
        r = await get_client().post(
            _TOKENS_URL, json={"refresh_token": rt},
            headers={"Content-Type": "application/json", "User-Agent": _BROWSER_UA},
        )
    except httpx.HTTPError as e:
        log.warning(f"refresh at → network error ({e})")
        return None
    if r.status_code == 401:
        log.warning("refresh at → 401 (rt burned — re-save cookies via save_cookies.py)")
        _last_refresh[name or "__last__"] = (401, r.text[:500])
        return None
    if r.status_code not in (200, 201):
        # Never log the body: refresh responses carry fresh tokens.
        log.warning(f"refresh at → {r.status_code} (body {len(r.text)} chars)")
        _last_refresh[name or "__last__"] = (r.status_code, r.text[:500])
        return None
    try:
        data = r.json()
    except Exception:
        log.warning("refresh at → bad JSON")
        return None
    new_at = (_find_str(data, ("access_token",))
              or _find_str(data, ("authentication_token", "auth_token", "token"),
                           exclude=(rt,)))
    if not new_at:
        # Never log the payload: it may contain access/refresh tokens.
        log.warning("refresh at → no access token in response")
        return None
    new_rt = _find_str(data, ("refresh_token", "refreshToken"), exclude=(new_at,))
    return new_at, new_rt


# Per-account refresh result: name → (status_code, body_text).
# Helps distinguish "rt burned" (401) from network errors / bans.
_last_refresh: dict[str, tuple[int, str]] = {}


def refresh_health(name: str) -> tuple[int, str]:
    """Return (last_refresh_status, last_refresh_body) for an account."""
    return _last_refresh.get(name, (0, ""))


class AccountPool:
    def __init__(self, cookies_dir: str = "cookies", creds_path: str = _CRED_PATH):
        self._dir = Path(cookies_dir)
        self._accounts: dict[str, dict] = {}
        self._free: asyncio.Queue = asyncio.Queue()
        self._refresh_locks: dict[str, asyncio.Lock] = {}
        self._locks_guard = threading.Lock()
        self._last_fresh: dict[str, bool] = {}  # checkout result per account
        self.creds = CredentialStore(creds_path)

    async def load(self) -> None:
        if not self._dir.exists():
            return
        for path in self._dir.glob("*.json"):
            if path.stat().st_size == 0:
                continue
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                state = _normalize_storage_state(raw)
                if not state.get("cookies"):
                    continue
                name = path.stem
                self._accounts[name] = state
                await self._free.put(name)
            except (json.JSONDecodeError, OSError, KeyError):
                pass

    async def checkout(self) -> tuple[str, dict]:
        while True:
            name = await self._free.get()
            if name not in self._accounts:
                continue  # removed via DELETE while checked out elsewhere
            fresh = await self.ensure_fresh(name)
            self._last_fresh[name] = fresh
            if not fresh:
                log.warning(f"checkout {name} → handing out possibly stale session (refresh failed)")
            if name not in self._accounts:
                continue  # removed during refresh
            return name, self._accounts[name]

    def is_fresh(self, name: str) -> bool:
        """Last ensure_fresh result. Lets loops skip dead sessions instead
        of burning a browser launch on them. Unknown = True (no data)."""
        return self._last_fresh.get(name, True)

    def _cookies_by_name(self, name: str) -> dict:
        state = self._accounts.get(name) or {}
        return {c.get("name"): c for c in state.get("cookies", []) if c.get("name")}

    def _persist(self, name: str) -> None:
        """Atomically write the in-memory state back to cookies/{name}.json."""
        path = self._dir / f"{name}.json"
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self._accounts[name], ensure_ascii=False),
                       encoding="utf-8")
        os.replace(tmp, path)

    async def _try_relogin(self, name: str) -> bool:
        """Fallback when rt-refresh is impossible/failed and login:pass is stored.

        Cooldown-guarded (one browser attempt per account per 10 min).
        Never raises; False = keep old behavior (manual save_cookies).
        """
        try:
            cred = self.creds.get(name)
            if not cred:
                return False
            now = time.time()
            if now - _relogin_attempts.get(name, 0) < _RELOGIN_COOLDOWN:
                return False
            _relogin_attempts[name] = now
            email, password = cred
            return await relogin(self, name, email, password)
        except Exception as e:
            log.warning(f"relogin {name} → hook error ({e})")
            return False

    async def ensure_fresh(self, name: str) -> bool:
        """Refresh `at` via `rt` when it expires within the margin.

        Never raises. True = fresh (or nothing to judge); False = refresh
        attempted and failed (rt likely burned — needs fresh save_cookies
        or stored login:pass for auto-relogin).
        """
        try:
            by_name = self._cookies_by_name(name)
            if not by_name:
                return await self._try_relogin(name)
            exp = _jwt_exp((by_name.get("at") or {}).get("value", ""))
            if exp is None:
                return True  # not a decodable JWT — leave to ensure_logged_in
            if exp - time.time() > _REFRESH_MARGIN:
                return True
            rt = (by_name.get("rt") or {}).get("value", "")
            if not rt:
                log.warning(f"refresh {name} → no rt cookie, cannot refresh")
                return await self._try_relogin(name)
            with self._locks_guard:
                lock = self._refresh_locks.setdefault(name, asyncio.Lock())
            need_relogin = False
            async with lock:
                # Re-check: another worker may have refreshed while we waited.
                exp = _jwt_exp((self._cookies_by_name(name).get("at") or {}).get("value", ""))
                if exp is not None and exp - time.time() > _REFRESH_MARGIN:
                    return True
                # Re-read rt INSIDE the lock: a queued worker must not mint
                # with an rt rotated out by the refresh ahead of it.
                fresh = self._cookies_by_name(name)
                rt = (fresh.get("rt") or {}).get("value", "")
                if not rt:
                    log.warning(f"refresh {name} → no rt cookie, cannot refresh")
                    need_relogin = True
                else:
                    pair = await _mint_pair(rt, name)
                    if not pair:
                        need_relogin = True
                    else:
                        new_at, new_rt = pair
                        fresh = self._cookies_by_name(name)
                        fresh["at"]["value"] = new_at
                        new_exp = _jwt_exp(new_at)
                        if new_exp:
                            fresh["at"]["expires"] = new_exp
                        if new_rt and "rt" in fresh:
                            fresh["rt"]["value"] = new_rt
                        self._persist(name)
                        log.info(f"refresh {name} → at renewed")
                        return True
            # Outside the lock: relogin polls the browser up to 120s and
            # must not stall other checkouts of this account (its own
            # cooldown guard still prevents stampedes).
            if need_relogin:
                return await self._try_relogin(name)
            return False
        except Exception as e:
            log.warning(f"refresh {name} → error ({e})")
            return False

    async def release(self, name: str) -> None:
        if name not in self._accounts:
            return  # removed via DELETE — drop the ghost, don't re-queue
        await self._free.put(name)

    async def add_account(self, name: str, state_: dict) -> bool:
        """Add a new account live (file must already be saved). Returns False if name exists."""
        if name in self._accounts:
            return False
        self._accounts[name] = state_
        await self._free.put(name)
        return True

    async def remove_account(self, name: str) -> bool:
        """Remove account + its file; rebuilds the free queue. Returns False if unknown."""
        if name not in self._accounts:
            return False
        del self._accounts[name]
        # Rebuild queue without the removed name (checked-out copies keep working).
        remaining: list[str] = []
        while not self._free.empty():
            remaining.append(await self._free.get())
        for n in remaining:
            if n in self._accounts:
                await self._free.put(n)
        try:
            (self._dir / f"{name}.json").unlink()
        except OSError:
            pass
        return True

    @property
    def available(self) -> int:
        return self._free.qsize()

    @property
    def total(self) -> int:
        return len(self._accounts)
