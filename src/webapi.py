import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

from src.accounts import AccountPool
from src.templates import TemplateLoader
from src.db import init_db, upsert_listing, is_seller_new, get_stats
from src.parser import parse_listings
from src.sender import send_messages
from src.inbox import check_and_reply


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
    }
    scheduler_running: bool = False
    scheduler_task: asyncio.Task = None
    task_running: bool = False

state = AppState()


# ─── WebSocket log broadcaster ─────────────────────────────────────────────────

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
                await client.send_text(json.dumps(message))
            except Exception:
                dead.append(client)
        for c in dead:
            self.disconnect(c)

broadcaster = WSBroadcaster()


class WSHandler(logging.Handler):
    def emit(self, record: logging.LogRecord):
        level_map = {
            "DEBUG": "debug",
            "INFO": "info",
            "WARNING": "warning",
            "ERROR": "error",
            "CRITICAL": "error",
        }
        msg = {
            "type": "log",
            "level": level_map.get(record.levelname, "info"),
            "text": self.format(record),
        }
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(broadcaster.broadcast(msg))
        except RuntimeError:
            pass


# ─── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs("data", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    logger = logging.getLogger("nebena")
    logger.setLevel(logging.DEBUG)
    if not any(isinstance(h, WSHandler) for h in logger.handlers):
        ws_handler = WSHandler()
        ws_handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(ws_handler)

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

    yield

    if state.scheduler_task and not state.scheduler_task.done():
        state.scheduler_task.cancel()
    await state.conn.close()


# ─── App ───────────────────────────────────────────────────────────────────────

app = FastAPI(lifespan=lifespan, title="NEbena")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ─── WebSocket ─────────────────────────────────────────────────────────────────

@app.websocket("/ws/logs")
async def ws_logs(websocket: WebSocket):
    await broadcaster.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        broadcaster.disconnect(websocket)


# ─── API helpers ───────────────────────────────────────────────────────────────

def _log(text: str, level: str = "info"):
    asyncio.ensure_future(broadcaster.broadcast({"type": "log", "level": level, "text": text}))

def _task_update(running: bool, label: str = ""):
    asyncio.ensure_future(broadcaster.broadcast({"type": "task", "running": running, "label": label}))


# ─── Stats & accounts ──────────────────────────────────────────────────────────

@app.get("/api/stats")
async def api_stats():
    stats = await get_stats(state.conn)
    return stats


@app.get("/api/accounts")
async def api_accounts():
    names = list(state.pool._accounts.keys()) if state.pool else []
    return {"accounts": names, "total": len(names)}


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


# ─── Parser ────────────────────────────────────────────────────────────────────

async def _do_parse():
    state.task_running = True
    _task_update(True, "Парсинг объявлений...")
    logger = logging.getLogger("nebena")
    try:
        logger.info(f"Запуск парсера — последние {state.settings['hours']}ч, аккаунтов: {state.pool.total}")
        listings = await parse_listings(state.pool, hours=state.settings["hours"])
        new_sellers = []
        for lst in listings:
            if await is_seller_new(state.conn, lst["seller_id"]):
                new_sellers.append(lst)
            await upsert_listing(state.conn, lst)
        logger.info(f"Парсер завершён: {len(listings)} объявлений, {len(new_sellers)} новых продавцов")
        await broadcaster.broadcast({"type": "stats_refresh"})
    except Exception as e:
        logger.error(f"Ошибка парсера: {e}")
    finally:
        state.task_running = False
        _task_update(False)


@app.post("/api/parser/run")
async def api_parser_run(background_tasks: BackgroundTasks):
    if state.task_running:
        return JSONResponse({"error": "Задача уже выполняется"}, status_code=409)
    background_tasks.add_task(_do_parse)
    return {"ok": True}


# ─── Sender ────────────────────────────────────────────────────────────────────

async def _do_send(max_per_run: int = 0):
    state.task_running = True
    _task_update(True, "Отправка сообщений...")
    logger = logging.getLogger("nebena")
    try:
        if state.templates.count == 0:
            logger.warning("Нет шаблонов — добавьте тексты в Shablon.txt")
            return
        async with state.conn.execute(
            "SELECT seller_id, seller_name, message_url FROM sellers WHERE message_sent = 0"
        ) as cur:
            rows = await cur.fetchall()
        sellers = [{"seller_id": r[0], "seller_name": r[1], "message_url": r[2] or ""} for r in rows]
        if max_per_run > 0:
            sellers = sellers[:max_per_run]
        if not sellers:
            logger.info("Нет новых продавцов для отправки.")
            return
        logger.info(f"Отправка сообщений: {len(sellers)} продавцов")
        sent = await send_messages(sellers, state.pool, state.templates, state.conn,
                                   delay=state.settings["delay"], max_per_run=max_per_run)
        logger.info(f"Отправлено: {sent} сообщений")
        await broadcaster.broadcast({"type": "stats_refresh"})
    except Exception as e:
        logger.error(f"Ошибка отправки: {e}")
    finally:
        state.task_running = False
        _task_update(False)


@app.post("/api/sender/run")
async def api_sender_run(background_tasks: BackgroundTasks, body: Optional[dict] = None):
    if state.task_running:
        return JSONResponse({"error": "Задача уже выполняется"}, status_code=409)
    max_per_run = (body or {}).get("max_per_run", 0)
    background_tasks.add_task(_do_send, max_per_run)
    return {"ok": True}


# ─── Inbox ─────────────────────────────────────────────────────────────────────

async def _do_inbox():
    state.task_running = True
    _task_update(True, "Проверка входящих...")
    logger = logging.getLogger("nebena")
    try:
        if state.templates2.count == 0:
            logger.warning("Нет шаблонов Phase 2 — добавьте тексты в Shablon2.txt")
            return
        logger.info("Проверка входящих сообщений...")
        sent = await check_and_reply(state.conn, state.pool, state.templates2,
                                     delay=state.settings["delay"])
        logger.info(f"Проверка входящих завершена: {sent} ответов отправлено")
        await broadcaster.broadcast({"type": "stats_refresh"})
    except Exception as e:
        logger.error(f"Ошибка проверки входящих: {e}")
    finally:
        state.task_running = False
        _task_update(False)


@app.post("/api/inbox/check")
async def api_inbox_check(background_tasks: BackgroundTasks):
    if state.task_running:
        return JSONResponse({"error": "Задача уже выполняется"}, status_code=409)
    background_tasks.add_task(_do_inbox)
    return {"ok": True}


# ─── Scheduler ─────────────────────────────────────────────────────────────────

async def _scheduler_loop():
    logger = logging.getLogger("nebena")
    interval = state.settings["scheduler_interval"] * 60
    per_run = state.settings["scheduler_per_run"]
    run = 0
    while True:
        run += 1
        logger.info(f"Планировщик: запуск #{run}")
        await broadcaster.broadcast({"type": "scheduler", "running": True, "run": run})
        try:
            await _do_parse()
            await _do_send(per_run)
            await _do_inbox()
        except Exception as e:
            logger.error(f"Планировщик: ошибка — {e}")
        logger.info(f"Планировщик: следующий запуск через {state.settings['scheduler_interval']} мин")
        await asyncio.sleep(interval)


@app.post("/api/scheduler/start")
async def api_scheduler_start():
    if state.scheduler_running:
        return JSONResponse({"error": "Планировщик уже запущен"}, status_code=409)
    state.scheduler_running = True
    state.scheduler_task = asyncio.create_task(_scheduler_loop())
    return {"ok": True, "running": True}


@app.post("/api/scheduler/stop")
async def api_scheduler_stop():
    if state.scheduler_task and not state.scheduler_task.done():
        state.scheduler_task.cancel()
    state.scheduler_running = False
    state.scheduler_task = None
    logging.getLogger("nebena").info("Планировщик остановлен")
    await broadcaster.broadcast({"type": "scheduler", "running": False})
    return {"ok": True, "running": False}


@app.get("/api/scheduler/status")
async def api_scheduler_status():
    return {"running": state.scheduler_running}


# ─── Serve UI ──────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_ui():
    with open("static/index.html", encoding="utf-8") as f:
        return f.read()
