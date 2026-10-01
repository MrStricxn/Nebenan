import asyncio
import json
import logging
import os
import tempfile
import urllib.parse
import urllib.request
import uuid
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from src.accounts import AccountPool
from src.templates import TemplateLoader
from src.db import init_db, upsert_listing, is_seller_new, get_stats
from src.parser import parse_listings
from src.sender import send_messages
from src.inbox import check_and_reply
from src.chat import get_token, fetch_conversations, fetch_messages, fetch_profile, send_message, send_with_photos, _BASE
from src.http import api_request_ex, set_health_recorder


log = logging.getLogger("nebena")

_task_lock = asyncio.Lock()  # guards check-then-set of task_running flags
_POLL_SEM = asyncio.Semaphore(4)  # bounds concurrent poll API calls


async def _claim_task() -> bool:
    """Check-then-set task_running under lock. True = claimed by the caller.

    Scheduler/sender-loop use this so a manual run and an automatic one
    can never overlap (bare bools cleared in _do_* finally blocks allow
    interleaved claims).
    """
    async with _task_lock:
        if state.task_running:
            return False
        state.task_running = True
        return True


# ─── State ────────────────────────────────────────────────────────────────────

class AppState:
    pool: AccountPool = None
    templates: TemplateLoader = None
    templates2: TemplateLoader = None
    conn = None
    settings: dict = {
        "hours": 24,
        "delay": 2.0,
        "max_accounts": 0,
        "scheduler_interval": 15,
        "scheduler_per_run": 2,
        "per_account": 2,       # messages per account per batch
        "cooldown_min": 15,     # minutes between batches
    }
    scheduler_running: bool = False
    scheduler_task: asyncio.Task = None
    task_running: bool = False
    bulk_running: bool = False      # bulk login:pass has its own flag (long browser runs)
    sender_loop_running: bool = False
    sender_loop_task: asyncio.Task = None
    sender_stop_event: asyncio.Event = None
    log_history: list = []          # persisted across reloads, max 500

state = AppState()


# ─── Account health / ban detection ──────────────────────────────────────
#
# The nebenan API returns the account profile even on 401 (it knows who
# you are from the session cookie).  That lets us tell "expired session"
# from "account banned" without guessing:
#   * 401 + refresh also 401            → EXPIRED  (rt burned, re-login)
#   * 401 + body has ban-like fields     → BANNED   (account restricted)
#   * network error / timeout            → OFFLINE  (proxy / connectivity)
BAN_SIGNALS = frozenset([
    "is_blocked", "is_banned", "is_deactivated", "is_suspended",
    "is_restricted", "is_forbidden", "deactivated_at", "ban_reason",
    "banned_at", "suspended_at", "restricted_at",
])
_BAN_HINTS: frozenset[str] = frozenset(["blocked", "banned", "deactivated",
                                       "suspended", "restricted", "forbidden"])

# name → {email, last_kind, last_at, last_status, last_body,
#         consecutive_401s, ban_signals: list[str], last_ok_at,
#         chat_unavailable: bool}
_account_health: dict[str, dict] = {}


def _detect_ban(body_text: str) -> list[str]:
    """Return suspicious keys found in the 401 body (empty = no ban signal)."""
    hits: list[str] = []
    try:
        data = json.loads(body_text) if body_text else {}
    except Exception:
        return hits
    def walk(obj: object, path: str = "") -> None:
        if isinstance(obj, dict):
            for k, v in obj.items():
                p = f"{path}.{k}" if path else k
                if k in BAN_SIGNALS and v:
                    hits.append(p)
                if isinstance(v, str) and any(h in v.lower() for h in _BAN_HINTS):
                    hits.append(p)
                walk(v, p)
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                walk(v, f"{path}[{i}]")
    walk(data)
    return hits


def _record_auth_event(
    account_name: str, method: str, url: str, status: int, body: str,
) -> None:
    """Update per-account health on every non-2xx API response."""
    h = _account_health.setdefault(account_name, {
        "email": None, "last_kind": "ok", "last_at": None,
        "last_status": 0, "last_body": "", "consecutive_401s": 0,
        "ban_signals": [], "last_ok_at": None, "chat_unavailable": False,
    })
    now = datetime.now().isoformat()
    h["last_at"] = now
    h["last_status"] = status
    h["last_body"] = (body or "")[:1500]  # keep a sample, not the whole file
    if status == 0:
        h["last_kind"] = "network"
        h["consecutive_401s"] = 0
        h["chat_unavailable"] = True
    elif status == 401:
        h["last_kind"] = "auth_401"
        h["consecutive_401s"] += 1
        # Parse the email embedded in the 401 profile body (present even on
        # auth failure) so we can identify which account is affected.
        try:
            prof = json.loads(body) if body else {}
            if not h["email"] and isinstance(prof, dict):
                h["email"] = prof.get("email")
            status_obj = prof.get("status") if isinstance(prof, dict) else {}
            if isinstance(status_obj, dict):
                h["email"] = h["email"] or status_obj.get("email")
        except Exception:
            pass
        # Ban detection: look for restriction flags in the profile body.
        hits = _detect_ban(body)
        h["ban_signals"] = list(dict.fromkeys(h["ban_signals"] + hits))
    else:
        h["last_kind"] = "error"
        h["consecutive_401s"] = 0
    if status in (200, 201):
        h["last_kind"] = "ok"
        h["consecutive_401s"] = 0
        h["last_ok_at"] = now
        h["chat_unavailable"] = False
        # Heal: a working authed call clears stale ban signals (a truly
        # banned account would not answer 2xx here).
        h["ban_signals"] = []


def account_health(name: str) -> dict:
    """Return a sanitized health snapshot for an account."""
    h = _account_health.get(name, {})
    kind = h.get("last_kind", "ok")
    if kind == "ok" and h.get("last_ok_at") is None and not h:
        kind = "fresh"
    # Classify overall: BANNED if ban signals present, else per-kind.
    if h.get("ban_signals"):
        verdict = "banned"
    elif kind in ("auth_401",):
        verdict = "expired"
    elif kind == "network":
        verdict = "offline"
    elif kind in ("fresh", "ok"):
        verdict = "ok"
    else:
        verdict = kind
    return {
        "verdict": verdict,
        "email": h.get("email"),
        "last_kind": kind,
        "last_at": h.get("last_at"),
        "last_status": h.get("last_status"),
        "consecutive_401s": h.get("consecutive_401s", 0),
        "ban_signals": h.get("ban_signals", []),
        "last_body": h.get("last_body", ""),
        "chat_unavailable": h.get("chat_unavailable", False),
    }


def refresh_account_health() -> dict[str, dict]:
    """Return a fresh copy of the health map, keyed by known account names."""
    names = list(_account_health.keys())
    if state.pool is not None:
        # Include healthy/never-failed pool members (reported as fresh/ok).
        for n in state.pool._accounts:
            if n not in _account_health:
                names.append(n)
    return {n: account_health(n) for n in names}


# ─── WebSocket broadcaster ─────────────────────────────────────────────────────

class WSBroadcaster:
    def __init__(self):
        self.clients: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.clients:
            self.clients.remove(ws)

    async def broadcast(self, message: dict):
        dead = []
        for client in self.clients:
            try:
                await client.send_text(json.dumps(message, ensure_ascii=False))
            except Exception:
                dead.append(client)
        for c in dead:
            self.disconnect(c)

    async def log(self, text: str, level: str = "info"):
        ts = datetime.now().strftime("%H:%M:%S")
        entry = {"type": "log", "level": level, "text": text, "time": ts}
        state.log_history.append(entry)
        if len(state.log_history) > 500:
            state.log_history.pop(0)
        await self.broadcast(entry)

    async def task_update(self, running: bool, label: str = ""):
        await self.broadcast({"type": "task", "running": running, "label": label})

broadcaster = WSBroadcaster()


# ─── Logging handler (bridges Python logging → WebSocket) ─────────────────────

class WSHandler(logging.Handler):
    """Bridge Python logging to WebSocket. Only works when called from async context."""
    _LEVELS = {"DEBUG": "debug", "INFO": "info", "WARNING": "warning",
               "ERROR": "error", "CRITICAL": "error"}

    def emit(self, record: logging.LogRecord):
        if getattr(record, "via_dashboard", False):
            return  # already fanned out via broadcaster.log — skip the dupe
        msg = {"type": "log", "level": self._LEVELS.get(record.levelname, "info"),
               "text": self.format(record)}
        # schedule broadcast without blocking — safe from any context
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(broadcaster.broadcast(msg))
        except RuntimeError:
            # no running loop (e.g. startup logging) — just ignore
            pass


# ─── Lifespan ──────────────────────────────────────────────────────────────────

async def _poll_new_messages(interval: int = 60) -> None:
    """Background task: poll all accounts for new messages, broadcast via WS."""
    await asyncio.sleep(15)          # wait for startup to settle
    last_seen: dict[str, int] = {}   # "account:partner_id" -> last message id
    initialized = False

    async def _poll_one(name: str, storage: dict) -> None:
        token = get_token(storage)
        if not token:
            return
        try:
            # api_request_ex fires the health recorder on non-2xx
            convs, _status, _txt = await api_request_ex(
                "GET",
                f"{_BASE}/api/v2/private_conversations.json?page=1&per_page=30",
                token, account_name=name,
            )
            if not convs:
                return
            # API returns an envelope {"private_conversations": [...]} —
            # unwrap like chat.fetch_conversations; ignore anything else.
            if isinstance(convs, dict):
                convs = convs.get("private_conversations", [])
            if not isinstance(convs, list):
                return
            for conv in convs:
                pid      = conv.get("partner_id")
                last_msg = conv.get("last_private_conversation_message") or {}
                msg_id   = last_msg.get("id")
                key      = f"{name}:{pid}"
                if msg_id and initialized and last_seen.get(key) != msg_id:
                    preview = (last_msg.get("body") or "")[:80]
                    await broadcaster.broadcast({
                        "type":       "new_message",
                        "account":    name,
                        "partner_id": pid,
                        "preview":    preview,
                    })
                    log.info(f"[{name}] новое сообщение от partner_id={pid}")
                if msg_id:
                    last_seen[key] = msg_id
        except Exception as e:
            log.debug(f"poll [{name}]: {e}")

    while True:
        try:
            accounts = dict(state.pool._accounts) if state.pool else {}

            async def _guarded(nm: str, st: dict) -> None:
                async with _POLL_SEM:
                    await _poll_one(nm, st)

            await asyncio.gather(*[_guarded(n, s) for n, s in accounts.items()])
            # bound state: evict oldest entries beyond 2000
            if len(last_seen) > 2000:
                for k in list(last_seen)[: len(last_seen) - 2000]:
                    last_seen.pop(k, None)
        except Exception as e:
            log.debug(f"poll cycle error: {e}")

        initialized = True
        await asyncio.sleep(interval)


def _poll_interval() -> int:
    """Chat poll cadence. Env POLL_INTERVAL_SEC, default 15s, min 5s.

    Faster = livelier inbox for all connected browsers, but more API
    load per account (WAF-sensitive — don't go below 5s).
    """
    try:
        return max(5, int(os.environ.get("POLL_INTERVAL_SEC", "15")))
    except (TypeError, ValueError):
        return 15


def _refresh_interval() -> int:
    try:
        return max(60, int(os.environ.get("COOKIE_REFRESH_SEC", "300")))
    except (TypeError, ValueError):
        return 300


async def _refresh_once() -> None:
    """One pass of ensure_fresh over all accounts. Loud on failure."""
    pool = state.pool
    if pool is None:
        return
    names = list(pool._accounts.keys())
    results = await asyncio.gather(*[pool.ensure_fresh(n) for n in names],
                                   return_exceptions=True)
    for name, ok in zip(names, results):
        if isinstance(ok, Exception):
            log.warning(f"refresh-loop [{name}]: {ok} — нужен свежий save_cookies?")
        elif ok is False:
            log.warning(f"refresh-loop [{name}]: НЕ обновлён (rt мёртв?) — нужен свежий save_cookies")


async def _refresh_cookies_loop() -> None:
    """Background task: keep every account's `at` fresh via `rt` rotation.

    `at` lives ~15 min; a pass every 5 min guarantees a refresh lands
    before expiry, so browser/API flows never hit a dead session mid-run.
    `rt` itself rotates on each refresh — its lifetime is bounded by
    nebenan, so a dead `rt` still needs one manual save_cookies (loud log).
    """
    await asyncio.sleep(30)  # let startup settle
    while True:
        try:
            await _refresh_once()
        except Exception as e:
            log.debug(f"refresh-loop cycle error: {e}")
        await asyncio.sleep(_refresh_interval())


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    # Wire logging → WebSocket
    logger = logging.getLogger("nebena")
    logger.setLevel(logging.DEBUG)
    if not any(isinstance(h, WSHandler) for h in logger.handlers):
        h = WSHandler()
        h.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(h)

    state.pool = AccountPool("cookies")
    await state.pool.load()

    state.templates = TemplateLoader("Shablon.txt")
    try:
        state.templates.load()
    except FileNotFoundError:
        pass

    state.templates2 = TemplateLoader("Shablon2.txt")
    try:
        state.templates2.load()
    except FileNotFoundError:
        pass

    state.conn = await init_db("data/nebena.db")
    set_health_recorder(_record_auth_event)

    poll_task = asyncio.create_task(_poll_new_messages(_poll_interval()))
    refresh_task = asyncio.create_task(_refresh_cookies_loop())

    yield

    poll_task.cancel()
    with suppress(asyncio.CancelledError):
        await poll_task
    refresh_task.cancel()
    with suppress(asyncio.CancelledError):
        await refresh_task
    if state.scheduler_task and not state.scheduler_task.done():
        state.scheduler_task.cancel()
        with suppress(asyncio.CancelledError):
            await state.scheduler_task
    if state.sender_loop_task and not state.sender_loop_task.done():
        state.sender_loop_task.cancel()
        with suppress(asyncio.CancelledError):
            await state.sender_loop_task
    if state.conn is not None:
        await state.conn.close()
    from src.http import close_client
    await close_client()


# ─── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(lifespan=lifespan, title="Nebenan.de")
_cors_origins = [o.strip() for o in os.environ.get("CORS_ORIGINS", "").split(",") if o.strip()]
if _cors_origins:
    app.add_middleware(CORSMiddleware, allow_origins=_cors_origins, allow_methods=["*"], allow_headers=["*"])

# ─── API token gate ──────────────────────────────────────────────────────────
# Set API_TOKEN env to protect all /api/* on public deploys.
# Empty (default) = no auth, for local use. The SPA serves same-origin;
# the browser UI prompts for the token on first 401 and stores it in
# localStorage (see api() in static/index.html).
_API_TOKEN = os.environ.get("API_TOKEN", "").strip()


@app.middleware("http")
async def api_token_gate(request: Request, call_next):
    if _API_TOKEN and request.url.path.startswith("/api/"):
        if request.headers.get("authorization", "") != f"Bearer {_API_TOKEN}":
            return JSONResponse({"error": "Unauthorized"}, status_code=401)
    return await call_next(request)


# ─── WebSocket endpoint ────────────────────────────────────────────────────────

@app.websocket("/ws/logs")
async def ws_logs(websocket: WebSocket):
    if _API_TOKEN and websocket.query_params.get("token", "") != _API_TOKEN:
        await websocket.close(code=4401)
        return
    await broadcaster.connect(websocket)
    # Replay history so client sees logs from before this connection
    for entry in state.log_history:
        try:
            await websocket.send_text(json.dumps(entry, ensure_ascii=False))
        except Exception:
            break
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        broadcaster.disconnect(websocket)


# ─── Stats & accounts ──────────────────────────────────────────────────────────

@app.get("/api/stats")
async def api_stats():
    return await get_stats(state.conn)


@app.get("/api/accounts")
async def api_accounts():
    from src.parser import _get_auth_token
    accounts = []
    for name, storage in (state.pool._accounts.items() if state.pool else {}.items()):
        token = _get_auth_token(storage)
        accounts.append({"name": name, "has_token": bool(token)})
    return {"accounts": accounts, "total": len(accounts)}


@app.post("/api/accounts/check")
async def api_accounts_check():
    from src.parser import _get_auth_token
    from src.http import get_client
    client = get_client()
    results = {}
    for name, storage in (state.pool._accounts.items() if state.pool else {}.items()):
        token = _get_auth_token(storage)
        if not token:
            results[name] = "no_token"
            continue
        try:
            r = await client.get(
                "https://api.nebenan.de/api/core/v3/profile/notification_counts",
                headers={"x-auth-token": token, "accept": "application/json"},
            )
            results[name] = "ok" if r.status_code == 200 else "expired"
        except Exception as e:
            log.debug(f"accounts/check [{name}]: {e}")
            results[name] = "error"
    # Merge health diagnostics (verdict, email, ban signals).
    return {"results": results,
            "health": refresh_account_health()}


@app.get("/api/accounts/health")
async def api_accounts_health():
    """Per-account ban/expiry diagnostics for the dashboard."""
    return refresh_account_health()


@app.get("/api/credentials")
async def api_credentials_status():
    """Which accounts have login:pass stored (flags only — never passwords)."""
    if state.pool is None:
        return {"configured": []}
    return {"configured": state.pool.creds.configured()}


@app.post("/api/credentials")
async def api_credentials_set(body: dict):
    if state.pool is None:
        return JSONResponse({"error": "Пул не инициализирован"}, status_code=500)
    name = _s(body.get("account")).strip()
    if not name or name not in state.pool._accounts:
        return JSONResponse({"error": "Неизвестный аккаунт"}, status_code=404)
    email = _s(body.get("email")).strip()
    password = _s(body.get("password"))
    if not _valid_email(email):
        return JSONResponse({"error": "Некорректный email"}, status_code=400)
    if not password or len(password) > 512:
        return JSONResponse({"error": "Некорректный пароль"}, status_code=400)
    state.pool.creds.set(name, email, password)
    return {"ok": True}


@app.delete("/api/credentials/{name}")
async def api_credentials_delete(name: str):
    if state.pool is None:
        return JSONResponse({"error": "Пул не инициализирован"}, status_code=500)
    state.pool.creds.delete(name)
    return {"ok": True}


def _valid_email(email: str) -> bool:
    """Minimal email sanity: local@domain with both parts non-empty."""
    return bool(email) and "@" in email and not email.startswith("@") \
        and not email.endswith("@") and len(email) <= 254


def _s(v) -> str:
    """Coerce JSON values to str — non-string input must 400, never 500."""
    return v if isinstance(v, str) else ""


def _parse_bulk_lines(lines: list) -> tuple[list[tuple[str, str]], list[str]]:
    """Parse 'email:password' lines. Split on FIRST ':' (passwords may contain it).

    Returns (pairs, errors). Never logs or returns passwords — callers must
    only surface `errors` (which contain line numbers, not secrets).
    """
    pairs: list[tuple[str, str]] = []
    errors: list[str] = []
    for i, raw in enumerate(lines or [], 1):
        line = (raw or "").strip()
        if not line:
            continue
        if ":" not in line:
            errors.append(f"строка {i}: нет разделителя ':'")
            continue
        email, password = line.split(":", 1)
        email, password = email.strip(), password.strip()
        if not _valid_email(email):
            errors.append(f"строка {i}: некорректный email")
            continue
        if not password or len(password) > 512:
            errors.append(f"строка {i}: некорректный пароль")
            continue
        pairs.append((email, password))
    return pairs, errors


def _unique_cookie_name(base: str, taken: set[str]) -> str:
    """Sanitize base to a cookie name, suffixing -2/-3/... on collision."""
    name = _safe_cookie_name(base)
    if name not in taken:
        return name
    i = 2
    while f"{name}-{i}" in taken:
        i += 1
    return f"{name}-{i}"


async def _do_bulk_login(pairs: list[tuple[str, str]]):
    """Background: sequential login:pass per pair. Owns bulk_running flag.

    Only emails hit the logs — passwords never leave the request body,
    CredentialStore and the login form.
    """
    from src.accounts import relogin
    state.bulk_running = True
    ok = fail = skip = 0
    try:
        total = len(pairs)
        for n, (email, password) in enumerate(pairs, 1):
            if state.pool is None:
                break
            base = _safe_cookie_name(email)
            if base in state.pool._accounts:
                log.info(f"bulk-вход [{n}/{total}]: {email} → пропуск (уже есть)")
                skip += 1
                continue
            name = _unique_cookie_name(email, set(state.pool._accounts))
            log.info(f"bulk-вход [{n}/{total}]: {email} → вход...")
            try:
                good = await relogin(state.pool, name, email, password)
            except Exception as e:
                log.warning(f"bulk-вход: {email} → ошибка ({e})")
                good = False
            if good:
                state.pool.creds.set(name, email, password)
                log.info(f"bulk-вход: {email} → OK ({name})")
                ok += 1
            else:
                log.warning(f"bulk-вход: {email} → не удалось (капча? неверный пароль?)")
                fail += 1
    finally:
        state.bulk_running = False
    log.info(f"bulk-вход завершён: OK {ok}, ошибок {fail}, пропусков {skip}")


@app.post("/api/credentials/bulk")
async def api_credentials_bulk(body: dict):
    if state.pool is None:
        return JSONResponse({"error": "Пул не инициализирован"}, status_code=500)
    lines = (body or {}).get("lines")
    if not isinstance(lines, list) or not lines:
        return JSONResponse({"error": "Пустой список"}, status_code=400)
    if len(lines) > 50:
        return JSONResponse({"error": "Максимум 50 строк за раз"}, status_code=400)
    pairs, errors = _parse_bulk_lines(lines)
    if not pairs:
        return JSONResponse({"error": "Нет валидных строк", "details": errors}, status_code=400)
    async with _task_lock:
        if state.bulk_running:
            return JSONResponse({"error": "Массовый вход уже выполняется"}, status_code=409)
        state.bulk_running = True
    # _do_bulk_login owns the flag (clears in finally); keep it True across
    # create_task so a concurrent POST gets 409.
    asyncio.create_task(_do_bulk_login(pairs))
    return {"ok": True, "queued": len(pairs), "invalid": errors}


@app.get("/api/queue")
async def api_queue():
    async with state.conn.execute(
        """
        SELECT s.seller_id, s.seller_name, l.title, l.price, l.category, l.url
        FROM sellers s
        LEFT JOIN (SELECT seller_id, title, price, category, url FROM listings GROUP BY seller_id) l
          ON s.seller_id = l.seller_id
        WHERE s.message_sent = 0
        ORDER BY s.first_seen_at DESC
        LIMIT 200
        """
    ) as cur:
        rows = await cur.fetchall()
    return [{"seller_id": r[0], "seller_name": r[1], "title": r[2] or "",
             "price": r[3] or "", "category": r[4] or "", "url": r[5] or ""} for r in rows]


@app.get("/api/settings")
async def api_get_settings():
    return state.settings


@app.post("/api/settings")
async def api_save_settings(body: dict):
    for k, v in body.items():
        if k in state.settings:
            state.settings[k] = v
    return {"ok": True, "settings": state.settings}


@app.get("/api/templates")
async def api_templates():
    t1 = state.templates.count if state.templates else 0
    t2 = state.templates2.count if state.templates2 else 0
    return {"phase1": t1, "phase2": t2}


def _template_loader(phase: int):
    if phase == 1:
        return state.templates
    if phase == 2:
        return state.templates2
    return None


@app.get("/api/templates/full")
async def api_templates_full():
    return {
        "phase1": state.templates.all() if state.templates else [],
        "phase2": state.templates2.all() if state.templates2 else [],
    }


@app.post("/api/templates")
async def api_templates_add(body: dict):
    try:
        phase = int(body.get("phase", 1))
    except (TypeError, ValueError):
        return JSONResponse({"error": "phase должен быть 1 или 2"}, status_code=400)
    loader = _template_loader(phase)
    if loader is None:
        return JSONResponse({"error": "phase должен быть 1 или 2"}, status_code=400)
    text = (body.get("text") or "").strip()
    if not text:
        return JSONResponse({"error": "Пустой текст"}, status_code=400)
    if len(text) > 2000:
        return JSONResponse({"error": "Слишком длинный текст (макс. 2000)"}, status_code=400)
    index = loader.add(text)
    return {"ok": True, "index": index, "count": loader.count}


@app.put("/api/templates/{phase}/{index}")
async def api_templates_update(phase: int, index: int, body: dict):
    loader = _template_loader(phase)
    if loader is None:
        return JSONResponse({"error": "phase должен быть 1 или 2"}, status_code=400)
    text = (body.get("text") or "").strip()
    if not text:
        return JSONResponse({"error": "Пустой текст"}, status_code=400)
    if len(text) > 2000:
        return JSONResponse({"error": "Слишком длинный текст (макс. 2000)"}, status_code=400)
    try:
        loader.update(index, text)
    except IndexError:
        return JSONResponse({"error": "Нет шаблона с таким индексом"}, status_code=404)
    return {"ok": True}


@app.delete("/api/templates/{phase}/{index}")
async def api_templates_delete(phase: int, index: int):
    loader = _template_loader(phase)
    if loader is None:
        return JSONResponse({"error": "phase должен быть 1 или 2"}, status_code=400)
    try:
        loader.delete(index)
    except IndexError:
        return JSONResponse({"error": "Нет шаблона с таким индексом"}, status_code=404)
    return {"ok": True, "count": loader.count}


def _safe_cookie_name(raw: str) -> str:
    base = os.path.splitext(os.path.basename(raw or ""))[0]
    safe = "".join(ch for ch in base if ch.isalnum() or ch in "-_.").strip("._") or "account"
    return safe[:64]


@app.post("/api/cookies/upload")
async def api_cookies_upload(files: list[UploadFile] = File(...)):
    from src.accounts import _normalize_storage_state
    from src.parser import _get_auth_token
    if not state.pool:
        return JSONResponse({"error": "Пул аккаунтов не инициализирован"}, status_code=500)
    try:
        state.pool._dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        return JSONResponse({"error": f"Нет доступа к папке cookies: {e}"}, status_code=500)
    if len(files) > 20:
        return JSONResponse({"error": "Максимум 20 файлов за раз"}, status_code=400)
    used = set(state.pool._accounts)
    results = []
    for f in files:
        raw_name = f.filename or "account.json"
        if not raw_name.lower().endswith(".json"):
            results.append({"file": raw_name, "ok": False, "error": "Нужен .json файл"})
            continue
        try:
            data = await f.read()
            if len(data) > 5 * 1024 * 1024:
                raise ValueError("too big")
            storage = _normalize_storage_state(json.loads(data))
        except Exception:
            results.append({"file": raw_name, "ok": False, "error": "Невалидный JSON или файл >5MB"})
            continue
        if not storage.get("cookies"):
            results.append({"file": raw_name, "ok": False, "error": "В файле нет cookies"})
            continue
        name = _safe_cookie_name(raw_name)
        existed = name in state.pool._accounts
        if name in used and not existed:
            i = 2
            while f"{name}_{i}" in used:
                i += 1
            name = f"{name}_{i}"
            existed = False
        used.add(name)
        try:
            (state.pool._dir / f"{name}.json").write_text(
                json.dumps(storage, ensure_ascii=False), encoding="utf-8")
        except OSError as e:
            results.append({"file": raw_name, "ok": False, "error": f"Не сохранён: {e}"})
            continue
        if existed:
            results.append({"file": raw_name, "ok": True, "name": name,
                            "has_token": bool(_get_auth_token(storage)),
                            "note": "Файл обновлён, вступит после рестарта сервера"})
        else:
            await state.pool.add_account(name, storage)
            results.append({"file": raw_name, "ok": True, "name": name,
                            "has_token": bool(_get_auth_token(storage)),
                            "note": "Аккаунт добавлен без рестарта"})
    return {"results": results}


@app.delete("/api/cookies/{name}")
async def api_cookies_delete(name: str):
    if not state.pool or "/" in name or "\\" in name or name in ("", ".", ".."):
        return JSONResponse({"error": "Некорректное имя"}, status_code=400)
    if not await state.pool.remove_account(name):
        return JSONResponse({"error": "Аккаунт не найден"}, status_code=404)
    return {"ok": True}


# ─── Background task helpers ───────────────────────────────────────────────────

async def _do_parse():
    state.task_running = True
    await broadcaster.task_update(True, "Парсинг объявлений...")
    await broadcaster.log(f"Запуск парсера — последние {state.settings['hours']}ч, аккаунтов: {state.pool.total}")
    if state.pool.total == 0:
        await broadcaster.log(
            "Нет аккаунтов: папка cookies/ пуста. Запустите 'python save_cookies.py', "
            "залогиньтесь в браузере и перезапустите сервер.", "error")
    try:
        listings = await parse_listings(state.pool, hours=state.settings["hours"])
        new_sellers = 0
        for lst in listings:
            if await is_seller_new(state.conn, lst["seller_id"]):
                new_sellers += 1
            await upsert_listing(state.conn, lst)
        await broadcaster.log(f"Парсер завершён: {len(listings)} объявлений, {new_sellers} новых продавцов")
        await broadcaster.broadcast({"type": "stats_refresh"})
    except Exception as e:
        await broadcaster.log(f"Ошибка парсера: {e}", "error")
    finally:
        state.task_running = False
        await broadcaster.task_update(False)


async def _do_send(max_per_run: int = 0):
    state.task_running = True
    await broadcaster.task_update(True, "Отправка сообщений...")
    try:
        if state.templates.count == 0:
            await broadcaster.log("Нет шаблонов — добавьте тексты в Shablon.txt", "warning")
            return
        async with state.conn.execute(
            """
            SELECT s.seller_id, s.seller_name, s.message_url, l.url AS listing_url
            FROM sellers s
            LEFT JOIN (
                SELECT seller_id, url FROM listings
                GROUP BY seller_id
            ) l ON s.seller_id = l.seller_id
            WHERE s.message_sent = 0
            """
        ) as cur:
            rows = await cur.fetchall()
        sellers = [{"seller_id": r[0], "seller_name": r[1], "message_url": r[2] or "",
                    "listing_url": r[3] or ""} for r in rows]
        if not sellers:
            await broadcaster.log("Нет новых продавцов — сначала запустите парсер", "warning")
            return

        per_account  = state.settings["per_account"]
        cooldown_min = state.settings["cooldown_min"]
        batch_size   = state.pool.total * per_account  # e.g. 6 accounts × 2 = 12 per round
        if max_per_run > 0:
            sellers = sellers[:max_per_run]

        total_sent = 0
        round_num  = 0
        queue      = list(sellers)

        while queue:
            round_num += 1
            batch = queue[:batch_size]
            queue = queue[batch_size:]

            await broadcaster.log(
                f"Серия #{round_num}: {len(batch)} сообщений "
                f"({per_account} на аккаунт), осталось в очереди: {len(queue)}"
            )
            sent = await send_messages(batch, state.pool, state.templates, state.conn,
                                        delay=state.settings["delay"],
                                        on_log=broadcaster.log)
            total_sent += sent

            if queue:
                await broadcaster.log(
                    f"Серия #{round_num} завершена ({sent} отправлено). "
                    f"Кулдаун {cooldown_min} мин..."
                )
                await asyncio.sleep(cooldown_min * 60)

        await broadcaster.log(f"Рассылка завершена: {total_sent} сообщений отправлено")
        await broadcaster.broadcast({"type": "stats_refresh"})
    except Exception as e:
        await broadcaster.log(f"Ошибка отправки: {e}", "error")
    finally:
        state.task_running = False
        await broadcaster.task_update(False)


async def _do_inbox():
    state.task_running = True
    await broadcaster.task_update(True, "Проверка входящих...")
    try:
        if state.templates2.count == 0:
            await broadcaster.log("Нет шаблонов Phase 2 — добавьте тексты в Shablon2.txt", "warning")
            return
        await broadcaster.log("Проверка входящих сообщений...")
        sent = await check_and_reply(state.conn, state.pool, state.templates2,
                                     delay=state.settings["delay"])
        await broadcaster.log(f"Проверка входящих завершена: {sent} ответов отправлено")
        await broadcaster.broadcast({"type": "stats_refresh"})
    except Exception as e:
        await broadcaster.log(f"Ошибка проверки входящих: {e}", "error")
    finally:
        state.task_running = False
        await broadcaster.task_update(False)


# ─── API endpoints ─────────────────────────────────────────────────────────────

@app.post("/api/parser/run")
async def api_parser_run():
    async with _task_lock:
        if state.task_running:
            return JSONResponse({"error": "Задача уже выполняется"}, status_code=409)
        state.task_running = True
    # _do_parse owns the flag (sets True at start, clears in finally);
    # keep it True across create_task so a concurrent POST gets 409.
    asyncio.create_task(_do_parse())
    return {"ok": True}


@app.post("/api/sender/run")
async def api_sender_run(body: Optional[dict] = None):
    async with _task_lock:
        if state.task_running:
            return JSONResponse({"error": "Задача уже выполняется"}, status_code=409)
        state.task_running = True
    max_per_run = (body or {}).get("max_per_run", 0)
    asyncio.create_task(_do_send(max_per_run))
    return {"ok": True}


async def _sender_loop():
    state.sender_loop_running = True
    await broadcaster.broadcast({"type": "sender_status", "running": True, "cycle": 0})

    per_account  = state.settings.get("per_account", 2)
    cooldown_min = state.settings.get("cooldown_min", 15)
    batch_size   = state.pool.total * per_account
    run = 0

    await broadcaster.log(
        f"Рассылка запущена: {per_account}/аккаунт × {state.pool.total} = "
        f"{batch_size} в цикле, кулдаун {cooldown_min} мин"
    )

    try:
        while not state.sender_stop_event.is_set():
            async with state.conn.execute(
                """
                SELECT s.seller_id, s.seller_name, s.message_url,
                       (SELECT l.url FROM listings l
                        WHERE l.seller_id = s.seller_id AND l.url != ''
                        ORDER BY l.parsed_at DESC LIMIT 1) AS listing_url
                FROM sellers s
                WHERE s.message_sent = 0
                """
            ) as cur:
                rows = await cur.fetchall()

            if not rows:
                await broadcaster.log("Очередь пуста — все продавцы получили сообщение")
                break

            if state.templates.count == 0:
                await broadcaster.log("Нет шаблонов — добавьте тексты в Shablon.txt", "warning")
                break

            run += 1
            batch = [
                {
                    "seller_id":   r[0],
                    "seller_name": r[1],
                    "message_url": r[2] or "",
                    "listing_url": r[3] or "",
                }
                for r in rows[:batch_size]
            ]
            after = max(0, len(rows) - len(batch))

            await broadcaster.log(
                f"Цикл #{run}: {len(batch)} отправок ({per_account}/аккаунт), "
                f"после: {after} в очереди"
            )
            await broadcaster.broadcast({"type": "sender_status", "running": True, "cycle": run})

            if not await _claim_task():
                sent = 0
                await broadcaster.log(
                    f"Цикл #{run} пропущен — выполняется другая задача", "warning")
            else:
                await broadcaster.task_update(True, f"Рассылка — цикл #{run}")
                try:
                    sent = await send_messages(
                        batch, state.pool, state.templates, state.conn,
                        delay=state.settings["delay"],
                        on_log=broadcaster.log
                    )
                finally:
                    state.task_running = False
                    await broadcaster.task_update(False)

            await broadcaster.log(f"Цикл #{run} завершён: {sent}/{len(batch)} отправлено")
            await broadcaster.broadcast({"type": "stats_refresh"})

            if state.sender_stop_event.is_set():
                break

            async with state.conn.execute(
                "SELECT COUNT(*) FROM sellers WHERE message_sent = 0"
            ) as cur:
                remaining = (await cur.fetchone())[0]

            if remaining == 0:
                await broadcaster.log("Очередь пуста — рассылка завершена")
                break

            cooldown_sec = cooldown_min * 60
            await broadcaster.log(f"Ожидание {cooldown_min} мин до цикла #{run+1} (в очереди: {remaining})")
            await broadcaster.broadcast({
                "type": "sender_countdown",
                "seconds": cooldown_sec,
                "next_cycle": run + 1,
            })

            try:
                await asyncio.wait_for(state.sender_stop_event.wait(), timeout=float(cooldown_sec))
                break  # stop event fired
            except asyncio.TimeoutError:
                pass   # normal — proceed to next cycle

    except Exception as e:
        await broadcaster.log(f"Ошибка рассылки: {e}", "error")
    finally:
        state.sender_loop_running = False
        state.sender_loop_task = None
        await broadcaster.broadcast({"type": "sender_status", "running": False, "cycle": 0})
        await broadcaster.log("Рассылка остановлена")


@app.post("/api/sender/start")
async def api_sender_start():
    if state.sender_loop_running:
        return JSONResponse({"error": "Рассылка уже запущена"}, status_code=409)
    state.sender_stop_event = asyncio.Event()
    state.sender_loop_task  = asyncio.create_task(_sender_loop())
    return {"ok": True, "running": True}


@app.post("/api/sender/stop")
async def api_sender_stop():
    if state.sender_stop_event:
        state.sender_stop_event.set()
    await broadcaster.log("Остановка рассылки...", "warning")
    return {"ok": True}


@app.get("/api/sender/status")
async def api_sender_status():
    return {"running": state.sender_loop_running}


@app.post("/api/inbox/check")
async def api_inbox_check():
    async with _task_lock:
        if state.task_running:
            return JSONResponse({"error": "Задача уже выполняется"}, status_code=409)
        state.task_running = True
    asyncio.create_task(_do_inbox())
    return {"ok": True}


# ─── Scheduler ─────────────────────────────────────────────────────────────────

async def _scheduler_loop():
    run = 0
    while True:
        interval = state.settings["scheduler_interval"] * 60
        per_run  = state.settings["scheduler_per_run"]
        run += 1
        await broadcaster.log(f"Планировщик: запуск #{run}")
        await broadcaster.broadcast({"type": "scheduler", "running": True, "run": run})
        if not await _claim_task():
            await broadcaster.log("Планировщик: пропуск — выполняется ручная задача", "warning")
            await broadcaster.log(f"Планировщик: следующий запуск через {state.settings['scheduler_interval']} мин")
            await asyncio.sleep(interval)
            continue
        try:
            await _do_parse()
            await _do_send(per_run)
            await _do_inbox()
        except Exception as e:
            await broadcaster.log(f"Планировщик: ошибка — {e}", "error")
        state.task_running = False  # _do_* clear it themselves; ensure release
        await broadcaster.log(f"Планировщик: следующий запуск через {state.settings['scheduler_interval']} мин")
        await asyncio.sleep(interval)


@app.post("/api/scheduler/start")
async def api_scheduler_start():
    if state.scheduler_running:
        return JSONResponse({"error": "Планировщик уже запущен"}, status_code=409)
    state.scheduler_running = True
    state.scheduler_task = asyncio.create_task(_scheduler_loop())
    await broadcaster.log(f"Планировщик запущен (каждые {state.settings['scheduler_interval']} мин)")
    return {"ok": True, "running": True}


@app.post("/api/scheduler/stop")
async def api_scheduler_stop():
    if state.scheduler_task and not state.scheduler_task.done():
        state.scheduler_task.cancel()
    state.scheduler_running = False
    state.scheduler_task = None
    await broadcaster.log("Планировщик остановлен", "warning")
    await broadcaster.broadcast({"type": "scheduler", "running": False})
    return {"ok": True, "running": False}


@app.get("/api/scheduler/status")
async def api_scheduler_status():
    return {"running": state.scheduler_running}


# ─── Chat API ──────────────────────────────────────────────────────────────────

def _account_token(name: str) -> str | None:
    s = state.pool._accounts.get(name)
    return get_token(s) if s else None


async def _fresh_token(name: str) -> str | None:
    """ensure_fresh + token (for chat API endpoints using `at` directly)."""
    if state.pool is None:
        return None
    await state.pool.ensure_fresh(name)
    return _account_token(name)


def _account_state(name: str) -> dict | None:
    if state.pool is None:
        return None
    return state.pool._accounts.get(name)


@app.get("/api/chat/accounts")
async def api_chat_accounts():
    names = list(state.pool._accounts.keys())
    return {"accounts": names}


@app.get("/api/chat/conversations")
async def api_chat_conversations(account: str, page: int = 1, per_page: int = 30):
    token = await _fresh_token(account)
    if not token:
        return JSONResponse({"error": "Аккаунт не найден"}, status_code=404)
    page = max(1, page)
    per_page = min(max(1, per_page), 50)
    try:
        convs = await fetch_conversations(token, page=page, per_page=per_page,
                                          account_name=account)
    except Exception as e:
        log.warning(f"conversations [{account}]: {e}")
        return JSONResponse({"error": "Ошибка API чата"}, status_code=502)
    return {"conversations": convs, "page": page, "per_page": per_page}


@app.get("/api/chat/conversations/all")
async def api_chat_conversations_all(page: int = 1, per_page: int = 30):
    """Fetch conversations from ALL accounts, merged and sorted by last message."""
    accounts = dict(state.pool._accounts) if state.pool else {}
    page = max(1, page)
    per_page = min(max(1, per_page), 50)

    async def _fetch(name: str, storage: dict) -> list[dict]:
        try:
            # Refresh FIRST, then read the token from fresh state — the
            # snapshot may hold a rotated-out `at` (stale token → 401 → []).
            await state.pool.ensure_fresh(name)
            token = get_token(state.pool._accounts.get(name, storage))
            if not token:
                return []
            convs = await fetch_conversations(token, page=page, per_page=per_page,
                                              account_name=name)
            for c in convs:
                c["_account"] = name
            return convs
        except Exception as e:
            log.debug(f"conversations/all [{name}]: {e}")
            return []

    results = await asyncio.gather(*[_fetch(n, s) for n, s in accounts.items()])
    merged  = [c for r in results for c in r]

    def _sort_key(c: dict) -> str:
        lm = c.get("last_private_conversation_message") or {}
        return lm.get("created_at") or ""

    merged.sort(key=_sort_key, reverse=True)
    return {"conversations": merged, "page": page, "per_page": per_page}


@app.get("/api/chat/messages")
async def api_chat_messages(account: str, partner_id: int, per_page: int = 50):
    token = await _fresh_token(account)
    if not token:
        return JSONResponse({"error": "Аккаунт не найден"}, status_code=404)
    if partner_id <= 0:
        return JSONResponse({"error": "Некорректный partner_id"}, status_code=400)
    try:
        result, profile = await asyncio.gather(
            fetch_messages(token, partner_id, per_page=per_page, account_name=account),
            fetch_profile(token, account_name=account),
        )
    except Exception as e:
        log.warning(f"messages [{account}/{partner_id}]: {e}")
        return JSONResponse({"error": "Ошибка API чата"}, status_code=502)
    # Attach own user_id so the frontend knows which side is "me"
    if not isinstance(result, dict):
        return JSONResponse({"error": "Ошибка API чата"}, status_code=502)
    result["my_id"] = (profile or {}).get("id")
    return result


_TRANSLATE_CACHE: dict[tuple[str, str, str], tuple[str, float]] = {}
_TRANSLATE_CACHE_MAX = 2000
_TRANSLATE_CACHE_TTL = 24 * 3600  # translations are stable; TTL bounds staleness

try:
    import ssl as _ssl_mod
    import certifi as _certifi_mod
    _TRANSLATE_SSL_CTX = _ssl_mod.create_default_context(cafile=_certifi_mod.where())
except Exception:
    _TRANSLATE_SSL_CTX = None  # fall back to stdlib default verification


def _translate_call(text: str, source: str, target: str) -> str:
    url = (
        "https://api.mymemory.translated.net/get?"
        + urllib.parse.urlencode({"q": text, "langpair": f"{source}|{target}"})
    )
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    # Explicit CA bundle: fresh Windows Servers often have an incomplete
    # system store, which breaks stdlib default verification (CERTIFICATE_VERIFY_FAILED).
    with urllib.request.urlopen(req, timeout=8, context=_TRANSLATE_SSL_CTX) as resp:
        data = json.loads(resp.read().decode())
    # Only accept real translations: MyMemory signals quota/rate errors via
    # non-200 responseStatus, and such texts must never enter the 24h cache.
    if str(data.get("responseStatus")) != "200":
        return ""
    return data.get("responseData", {}).get("translatedText", "")


async def _do_translate(text: str, source: str, target: str) -> str:
    key = (text, source, target)
    now = datetime.now(timezone.utc).timestamp()
    hit = _TRANSLATE_CACHE.get(key)
    if hit is not None:
        val, exp = hit
        if exp > now:
            return val
        _TRANSLATE_CACHE.pop(key, None)
    loop = asyncio.get_running_loop()
    translated = await loop.run_in_executor(None, _translate_call, text, source, target)
    # Cache real translations only — never empties/echoes (they usually mean
    # "quota exhausted, retry later", which must not stick for 24h).
    # Also refuse MyMemory warning strings that slip through with 200.
    if translated and translated != text \
            and "MYMEMORY WARNING" not in translated \
            and "QUERY LENGTH LIMIT" not in translated:
        if len(_TRANSLATE_CACHE) >= _TRANSLATE_CACHE_MAX:
            _TRANSLATE_CACHE.pop(next(iter(_TRANSLATE_CACHE)))
        _TRANSLATE_CACHE[key] = (translated, now + _TRANSLATE_CACHE_TTL)
    return translated


@app.get("/api/translate")
async def api_translate(text: str, source: str = "de", target: str = "ru"):
    if len(text) > 500:
        return JSONResponse({"error": "Текст слишком длинный (макс. 500)"}, status_code=400)
    try:
        translated = await _do_translate(text, source, target)
        return {"translated": translated if translated != text else ""}
    except Exception as e:
        log.warning(f"translate error: {e}")
        return JSONResponse({"error": "Сервис перевода недоступен"}, status_code=502)


@app.post("/api/translate")
async def api_translate_post(body: dict):
    text = _s((body or {}).get("text"))
    source = (_s((body or {}).get("source")) or "de")[:8]
    target = (_s((body or {}).get("target")) or "ru")[:8]
    if not text:
        return JSONResponse({"error": "text обязателен"}, status_code=400)
    if len(text) > 500:
        return JSONResponse({"error": "Текст слишком длинный (макс. 500)"}, status_code=400)
    try:
        translated = await _do_translate(text, source, target)
        return {"translated": translated if translated != text else ""}
    except Exception as e:
        log.warning(f"translate error: {e}")
        return JSONResponse({"error": "Сервис перевода недоступен"}, status_code=502)


@app.get("/api/links/config")
async def api_links_config():
    from src import links
    return {"lumma": links.is_configured("lumma"),
            "shlepock": links.is_configured("shlepock"),
            "supported": sorted(links.DUAL_MAP)}


@app.post("/api/links/generate")
async def api_links_generate(body: dict):
    from src import links
    url = _s((body or {}).get("source_url")).strip()
    if not url or not links.URL_RE.match(url):
        return JSONResponse({"error": "Нужна корректная ссылка http(s)://…"}, status_code=400)
    if len(url) > 2000:
        return JSONResponse({"error": "Ссылка слишком длинная"}, status_code=400)
    targets = links.detect_services(url)
    if not targets:
        return JSONResponse({"error": "Домен не поддерживается (nebenan.de API не принимает)",
                             "supported": sorted(links.DUAL_MAP)}, status_code=400)
    try:
        results = await links.generate_all(url)
    except Exception as e:
        log.warning(f"links generate error: {e}")
        return JSONResponse({"error": "Ошибка генерации"}, status_code=502)
    return {"results": results}


@app.post("/api/chat/send-photo")
async def api_chat_send_photo(
    request:    Request,
    account:    str        = Form(...),
    partner_id: int        = Form(...),
    text:       str        = Form(""),
    photos:     list[UploadFile] = File(default=[]),
):
    storage = _account_state(account)
    if not storage:
        return JSONResponse({"error": "Аккаунт не найден"}, status_code=404)
    await state.pool.ensure_fresh(account)
    if partner_id <= 0:
        return JSONResponse({"error": "Некорректный partner_id"}, status_code=400)
    if not (text or "").strip() and not photos:
        # Empty payload: the browser flow would hold Chromium ~25s waiting
        # for a submit button that never appears. Fail fast instead.
        return JSONResponse({"error": "Нужно фото или текст"}, status_code=400)
    _ALLOWED_SUFFIXES = {".jpg", ".jpeg", ".png", ".webp"}
    _MAX_PHOTOS = 5
    _MAX_BYTES = 10 * 1024 * 1024
    _MAX_TOTAL = 50 * 1024 * 1024
    if len(photos) > _MAX_PHOTOS:
        return JSONResponse({"error": f"Максимум {_MAX_PHOTOS} фото"}, status_code=400)
    try:
        declared = int(request.headers.get("content-length", "0") or "0")
    except ValueError:
        declared = 0
    if declared > 60 * 1024 * 1024:
        return JSONResponse({"error": "Запрос слишком большой"}, status_code=400)
    tmp_paths: list[str] = []
    try:
        total = 0
        for photo in photos:
            suffix = os.path.splitext(photo.filename or "")[1].lower() or ".jpg"
            if suffix not in _ALLOWED_SUFFIXES:
                return JSONResponse({"error": f"Недопустимый тип файла: {suffix}"}, status_code=400)
            # Chunked read: a hostile client can bypass Content-Length with
            # chunked encoding, so enforce caps while spooling, not after.
            chunks: list[bytes] = []
            size = 0
            while True:
                chunk = await photo.read(1 << 20)
                if not chunk:
                    break
                size += len(chunk)
                if size > _MAX_BYTES:
                    return JSONResponse({"error": "Файл слишком большой (макс. 10MB)"}, status_code=400)
                if total + size > _MAX_TOTAL:
                    return JSONResponse({"error": "Суммарный размер слишком большой (макс. 50MB)"}, status_code=400)
                chunks.append(chunk)
            data = b"".join(chunks)
            total += size
            fd, tmp = tempfile.mkstemp(prefix="nebena_photo_", suffix=suffix)
            with os.fdopen(fd, "wb") as f:
                f.write(data)
            tmp_paths.append(tmp)
        sent, attached, expected = await send_with_photos(storage, partner_id, text, tmp_paths, account)
    finally:
        for p in tmp_paths:
            try:
                os.unlink(p)
            except OSError:
                pass
    if not sent:
        return JSONResponse({"error": "Не удалось отправить"}, status_code=502)
    if attached < expected:
        return JSONResponse(
            {"error": f"Отправлено без фото (загружено {attached}/{expected})",
             "attached": attached, "expected": expected},
            status_code=502)
    return {"ok": True, "attached": attached}


@app.post("/api/chat/send")
async def api_chat_send(body: dict):
    body = body or {}
    account    = _s(body.get("account"))
    partner_id = body.get("partner_id")
    text       = _s(body.get("text")).strip()
    if not account or not partner_id or not text:
        return JSONResponse({"error": "account, partner_id и text обязательны"}, status_code=400)
    if len(text) > 2000:
        return JSONResponse({"error": "Текст слишком длинный (макс. 2000)"}, status_code=400)
    try:
        pid = int(partner_id)
    except (TypeError, ValueError):
        return JSONResponse({"error": "Некорректный partner_id"}, status_code=400)
    if pid <= 0:
        return JSONResponse({"error": "Некорректный partner_id"}, status_code=400)
    token = await _fresh_token(account)
    if not token:
        return JSONResponse({"error": "Аккаунт не найден"}, status_code=404)
    try:
        ok, status = await send_message(token, pid, text, account_name=account)
    except Exception as e:
        log.warning(f"send [{account}/{pid}]: {e}")
        return JSONResponse({"error": "Ошибка API чата"}, status_code=502)
    if not ok and status == 401:
        return JSONResponse({"error": "Сессия аккаунта истекла — обновите cookies"},
                            status_code=502)
    if not ok:
        return JSONResponse({"error": "Не удалось отправить сообщение"},
                            status_code=502)
    return {"ok": ok}


@app.get("/api/sellers/{seller_id}/listing")
async def api_seller_listing(seller_id: int):
    """Most recent listing for a seller — used in chat header."""
    async with state.conn.execute(
        """SELECT l.url, l.title, l.price, s.message_url
           FROM listings l
           JOIN sellers s ON s.seller_id = l.seller_id
           WHERE l.seller_id = ?
           ORDER BY l.parsed_at DESC LIMIT 1""",
        (str(seller_id),),
    ) as cur:
        row = await cur.fetchone()
    if row:
        return {"url": row[0], "title": row[1], "price": row[2]}
    async with state.conn.execute(
        "SELECT message_url FROM sellers WHERE seller_id = ?", (str(seller_id),)
    ) as cur:
        srow = await cur.fetchone()
    if srow and srow[0]:
        return {"url": srow[0], "title": None, "price": None}
    return {"url": None, "title": None, "price": None}


@app.get("/api/chat/search")
async def api_chat_search(account: str, q: str):
    if not q or len(q.strip()) < 2:
        return {"results": [], "query": q}
    if not _account_token(account):
        return JSONResponse({"error": "Аккаунт не найден"}, status_code=404)

    # Escape LIKE wildcards so % and _ are matched literally
    raw = q.strip().lower().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    like = f"%{raw}%"

    # Search by seller name
    async with state.conn.execute(
        "SELECT seller_id, seller_name FROM sellers WHERE LOWER(seller_name) LIKE ? ESCAPE '\\' LIMIT 50",
        (like,),
    ) as cur:
        name_rows = await cur.fetchall()

    # Search by listing title (most recent title per seller)
    async with state.conn.execute(
        """
        SELECT s.seller_id, s.seller_name, l.title
        FROM listings l
        JOIN sellers s ON s.seller_id = l.seller_id
        WHERE LOWER(l.title) LIKE ? ESCAPE '\\'
        ORDER BY l.parsed_at DESC
        LIMIT 50
        """,
        (like,),
    ) as cur:
        title_rows = await cur.fetchall()

    results: dict[str, dict] = {}
    for sid, sname in name_rows:
        try:
            pid = int(sid)
        except (TypeError, ValueError):
            continue
        results[str(sid)] = {
            "partner_id":   pid,
            "seller_name":  sname,
            "match":        "name",
            "matched_text": sname,
        }
    for sid, sname, title in title_rows:
        key = str(sid)
        try:
            pid = int(sid)
        except (TypeError, ValueError):
            continue
        if key not in results:
            results[key] = {
                "partner_id":   pid,
                "seller_name":  sname,
                "match":        "listing",
                "matched_text": title,
            }
        else:
            results[key]["listing_title"] = title

    return {"results": list(results.values()), "query": q}


# ─── Serve UI ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    with open("static/index.html", encoding="utf-8") as f:
        return f.read()
