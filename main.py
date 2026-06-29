import asyncio
import sys
import os
from src.accounts import AccountPool
from src.templates import TemplateLoader
from src.db import init_db, upsert_listing, is_seller_new, get_stats
from src.parser import parse_listings
from src.sender import send_messages
from src.inbox import check_and_reply
from src.cli import (
    setup_logging, show_menu, show_settings_menu,
    make_progress, show_stats, console
)

DEFAULT_SETTINGS = {
    "hours": 24,
    "delay": 2.0,
    "max_accounts": 0,
    "scheduler_interval": 15,   # minutes between scheduler runs
    "scheduler_per_run": 2,     # sellers to contact per run
}

SCHEDULER_INTERVAL_SEC = 15 * 60  # 15 minutes


async def run_parser(pool, conn, settings, logger):
    logger.info(f"Starting parser — last {settings['hours']}h, {pool.total} accounts")
    progress = make_progress("Parsing listings...", 0)
    with progress:
        progress.add_task("Parsing listings...", total=None)
        listings = await parse_listings(pool, hours=settings["hours"], progress=progress)
    new_sellers = []
    for lst in listings:
        if await is_seller_new(conn, lst["seller_id"]):
            new_sellers.append({
                "seller_id":   lst["seller_id"],
                "seller_name": lst["seller_name"],
                "url":         lst["url"],
                "message_url": lst.get("message_url", ""),
            })
        await upsert_listing(conn, lst)
    logger.info(f"Parser done: {len(listings)} listings, {len(new_sellers)} new sellers")
    return new_sellers


async def run_sender(pool, templates, conn, settings, logger, max_per_run: int = 0):
    if templates.count == 0:
        logger.warning("No templates loaded — add templates to Shablon.txt first.")
        return 0
    async with conn.execute(
        "SELECT seller_id, seller_name, message_url FROM sellers WHERE message_sent = 0"
    ) as cur:
        rows = await cur.fetchall()
    sellers = [{"seller_id": r[0], "seller_name": r[1], "message_url": r[2] or ""} for r in rows]
    if not sellers:
        logger.info("No new sellers to message.")
        return 0
    if max_per_run > 0:
        sellers = sellers[:max_per_run]
    logger.info(f"Sending messages to {len(sellers)} sellers...")
    progress = make_progress("Sending messages...", len(sellers))
    with progress:
        progress.add_task("Sending messages...", total=len(sellers))
        sent = await send_messages(
            sellers, pool, templates, conn,
            delay=settings["delay"],
            progress=progress,
            max_per_run=max_per_run,
        )
    logger.info(f"Sender done: {sent} messages sent")
    return sent


async def run_inbox(pool, templates2, conn, settings, logger):
    if templates2.count == 0:
        logger.warning("No Phase 2 templates — add templates to Shablon2.txt first.")
        return 0
    logger.info("Checking inbox for replies...")
    progress = make_progress("Checking inbox...", 0)
    with progress:
        progress.add_task("Checking inbox...", total=None)
        sent = await check_and_reply(conn, pool, templates2, delay=settings["delay"], progress=progress)
    logger.info(f"Inbox check done: {sent} Phase 2 messages sent")
    return sent


async def run_scheduler(pool, templates, templates2, conn, settings, logger):
    """
    Scheduler loop: every 15 minutes — parse + send to 2 new sellers + check inbox.
    Press Ctrl+C to stop.
    """
    interval = settings["scheduler_interval"] * 60
    per_run = settings["scheduler_per_run"]
    console.print(
        f"[bold green]Scheduler started[/bold green] — "
        f"every {settings['scheduler_interval']} min, "
        f"max {per_run} messages/run. Press Ctrl+C to stop."
    )
    run_count = 0
    while True:
        run_count += 1
        console.print(f"\n[cyan]--- Scheduler run #{run_count} ---[/cyan]")
        try:
            await run_parser(pool, conn, settings, logger)
            await run_sender(pool, templates, conn, settings, logger, max_per_run=per_run)
            await run_inbox(pool, templates2, conn, settings, logger)
        except Exception as e:
            logger.error(f"Scheduler run error: {e}")
        console.print(f"[dim]Next run in {settings['scheduler_interval']} minutes...[/dim]")
        await asyncio.sleep(interval)


async def main():
    logger = setup_logging()
    os.makedirs("data", exist_ok=True)

    pool = AccountPool("cookies")
    await pool.load()
    if pool.total == 0:
        logger.warning("No cookie files found in cookies/ — add *.json files first.")

    templates = TemplateLoader("Shablon.txt")
    try:
        templates.load()
    except FileNotFoundError:
        logger.warning("Shablon.txt not found — sender will not work.")

    templates2 = TemplateLoader("Shablon2.txt")
    try:
        templates2.load()
    except FileNotFoundError:
        logger.warning("Shablon2.txt not found — Phase 2 replies will not work.")

    conn = await init_db("data/nebena.db")
    settings = dict(DEFAULT_SETTINGS)

    try:
        while True:
            choice = show_menu()
            if choice == "1":
                await run_parser(pool, conn, settings, logger)
            elif choice == "2":
                await run_sender(pool, templates, conn, settings, logger)
            elif choice == "3":
                await run_parser(pool, conn, settings, logger)
                await run_sender(pool, templates, conn, settings, logger)
            elif choice == "4":
                await run_inbox(pool, templates2, conn, settings, logger)
            elif choice == "5":
                try:
                    await run_scheduler(pool, templates, templates2, conn, settings, logger)
                except KeyboardInterrupt:
                    console.print("\n[yellow]Scheduler stopped.[/yellow]")
            elif choice == "6":
                settings = show_settings_menu(settings)
            elif choice == "7":
                stats = await get_stats(conn)
                show_stats(stats, pool.total, templates.count)
            elif choice == "8":
                console.print("[bold yellow]Goodbye![/bold yellow]")
                break
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
