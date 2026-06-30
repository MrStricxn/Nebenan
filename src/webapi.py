import asyncio
import json
import logging
import os
import urllib.parse
import urllib.request
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from src.accounts import AccountPool
from src.templates import TemplateLoader
from src.db import init_db, upsert_listing, is_seller_new, get_stats
from src.parser import parse_listings
from src.sender import send_messages
from src.inbox import check_and_reply
from src.chat import get_token, fetch_conversations, fetch_messages, fetch_profile, send_message, send_with_photos


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
    sender_loop_running: bool = False
    sender_loop_task: asyncio.Task = None
    sender_stop_event: asyncio.Event = None
    log_history: list = []          # persisted across reloads, max 500

state = AppState()


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

    while True:
        try:
            accounts = state.pool._accounts if state.pool else {}
            for name, storage in accounts.items():
                token = get_token(storage)
                if not token:
                    continue
                try:
                    convs = await fetch_conversations(token, per_page=30)
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
        except Exception as e:
            log.debug(f"poll cycle error: {e}")

        initialized = True
        await asyncio.sleep(interval)


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

    poll_task = asyncio.create_task(_poll_new_messages())

    yield

    poll_task.cancel()
    if state.scheduler_task and not state.scheduler_task.done():
        state.scheduler_task.cancel()
    await state.conn.close()


# ─── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(lifespan=lifespan, title="NEbena")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ─── WebSocket endpoint ────────────────────────────────────────────────────────

@app.websocket("/ws/logs")
async def ws_logs(websocket: WebSocket):
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
    from playwright.async_api import async_playwright
    results = {}
    async with async_playwright() as pw:
        for name, storage in (state.pool._accounts.items() if state.pool else {}.items()):
            token = _get_auth_token(storage)
            if not token:
                results[name] = "no_token"
                continue
            try:
                req_ctx = await pw.request.new_context(
                    extra_http_headers={"x-auth-token": token, "accept": "application/json"}
                )
                r = await req_ctx.get(
                    "https://api.nebenan.de/api/core/v3/profile/notification_counts",
                    timeout=8000,
                )
                results[name] = "ok" if r.status == 200 else "expired"
                await req_ctx.dispose()
            except Exception:
                results[name] = "error"
    return results


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


# ─── Background task helpers ───────────────────────────────────────────────────

async def _do_parse():
    state.task_running = True
    await broadcaster.task_update(True, "Парсинг объявлений...")
    await broadcaster.log(f"Запуск парсера — последние {state.settings['hours']}ч, аккаунтов: {state.pool.total}")
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
                                       delay=state.settings["delay"])
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
    if state.task_running:
        return JSONResponse({"error": "Задача уже выполняется"}, status_code=409)
    asyncio.create_task(_do_parse())
    return {"ok": True}


@app.post("/api/sender/run")
async def api_sender_run(body: Optional[dict] = None):
    if state.task_running:
        return JSONResponse({"error": "Задача уже выполняется"}, status_code=409)
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

            state.task_running = True
            await broadcaster.task_update(True, f"Рассылка — цикл #{run}")
            try:
                sent = await send_messages(
                    batch, state.pool, state.templates, state.conn,
                    delay=state.settings["delay"]
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
    if state.task_running:
        return JSONResponse({"error": "Задача уже выполняется"}, status_code=409)
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
        try:
            await _do_parse()
            await _do_send(per_run)
            await _do_inbox()
        except Exception as e:
            await broadcaster.log(f"Планировщик: ошибка — {e}", "error")
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


def _account_state(name: str) -> dict | None:
    return state.pool._accounts.get(name)


@app.get("/api/chat/accounts")
async def api_chat_accounts():
    names = list(state.pool._accounts.keys())
    return {"accounts": names}


@app.get("/api/chat/conversations")
async def api_chat_conversations(account: str, page: int = 1):
    token = _account_token(account)
    if not token:
        return JSONResponse({"error": "Аккаунт не найден"}, status_code=404)
    convs = await fetch_conversations(token, page=page, per_page=30)
    return {"conversations": convs}


@app.get("/api/chat/messages")
async def api_chat_messages(account: str, partner_id: int, per_page: int = 50):
    token = _account_token(account)
    if not token:
        return JSONResponse({"error": "Аккаунт не найден"}, status_code=404)
    result = await fetch_messages(token, partner_id, per_page=per_page)
    # Attach own user_id so the frontend knows which side is "me"
    profile = await fetch_profile(token)
    result["my_id"] = profile.get("id")
    return result


@app.get("/api/translate")
async def api_translate(text: str, source: str = "de", target: str = "ru"):
    def _call():
        url = (
            "https://api.mymemory.translated.net/get?"
            + urllib.parse.urlencode({"q": text, "langpair": f"{source}|{target}"})
        )
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=8) as resp:
            return json.loads(resp.read().decode())

    try:
        loop = asyncio.get_event_loop()
        data = await loop.run_in_executor(None, _call)
        translated = data.get("responseData", {}).get("translatedText", "")
        return {"translated": translated if translated != text else ""}
    except Exception as e:
        log.warning(f"translate error: {e}")
        return JSONResponse({"translated": ""}, status_code=200)


@app.post("/api/chat/send-photo")
async def api_chat_send_photo(
    account:    str        = Form(...),
    partner_id: int        = Form(...),
    text:       str        = Form(""),
    photos:     list[UploadFile] = File(default=[]),
):
    storage = _account_state(account)
    if not storage:
        return JSONResponse({"error": "Аккаунт не найден"}, status_code=404)
    tmp_paths: list[str] = []
    try:
        for photo in photos:
            suffix = os.path.splitext(photo.filename or "")[1] or ".jpg"
            tmp = os.path.join(
                os.environ.get("TEMP", "/tmp"),
                f"nebena_photo_{os.getpid()}_{len(tmp_paths)}{suffix}",
            )
            with open(tmp, "wb") as f:
                f.write(await photo.read())
            tmp_paths.append(tmp)
        ok = await send_with_photos(storage, partner_id, text, tmp_paths)
    finally:
        for p in tmp_paths:
            try:
                os.unlink(p)
            except OSError:
                pass
    return {"ok": ok}


@app.post("/api/chat/send")
async def api_chat_send(body: dict):
    account    = body.get("account", "")
    partner_id = body.get("partner_id")
    text       = body.get("text", "").strip()
    if not account or not partner_id or not text:
        return JSONResponse({"error": "account, partner_id и text обязательны"}, status_code=400)
    token = _account_token(account)
    if not token:
        return JSONResponse({"error": "Аккаунт не найден"}, status_code=404)
    ok = await send_message(token, int(partner_id), text)
    return {"ok": ok}


# ─── Serve UI ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    with open("static/index.html", encoding="utf-8") as f:
        return f.read()
