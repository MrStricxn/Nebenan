import asyncio
import sys
import os
from src.accounts import AccountPool
from src.templates import TemplateLoader
from src.db import init_db, upsert_listing, is_seller_new, get_stats
from src.parser import parse_listings
from src.sender import send_messages
from src.cli import (
    setup_logging, show_menu, show_settings_menu,
    make_progress, show_stats, console
)

DEFAULT_SETTINGS = {"hours": 24, "delay": 2.0, "max_accounts": 0}


async def run_parser(pool, conn, settings, logger):
    logger.info(f"Starting parser — last {settings['hours']}h, {pool.total} accounts")
    progress = make_progress("Parsing listings...", 0)
    with progress:
        task_id = progress.add_task("Parsing listings...", total=None)
        listings = await parse_listings(pool, hours=settings["hours"], progress=progress)
    new_sellers = []
    for lst in listings:
        if await is_seller_new(conn, lst["seller_id"]):
            new_sellers.append({
                "seller_id": lst["seller_id"],
                "seller_name": lst["seller_name"],
                "url": lst["url"],
            })
        await upsert_listing(conn, lst)
    logger.info(f"Parser done: {len(listings)} listings, {len(new_sellers)} new sellers")
    return new_sellers


async def run_sender(pool, templates, conn, settings, logger):
    if templates.count == 0:
        logger.warning("No templates loaded — add templates to Shablon.txt first.")
        return 0
    async with conn.execute(
        "SELECT seller_id, seller_name FROM sellers WHERE message_sent = 0"
    ) as cur:
        rows = await cur.fetchall()
    sellers = [{"seller_id": r[0], "seller_name": r[1], "url": ""} for r in rows]
    if not sellers:
        logger.info("No new sellers to message.")
        return 0
    # fetch URLs from listings table for each seller
    enriched = []
    for s in sellers:
        async with conn.execute(
            "SELECT url FROM listings WHERE seller_id = ? LIMIT 1", (s["seller_id"],)
        ) as cur:
            row = await cur.fetchone()
        if row:
            s["url"] = row[0]
            enriched.append(s)
    logger.info(f"Sending messages to {len(enriched)} sellers...")
    progress = make_progress("Sending messages...", len(enriched))
    with progress:
        progress.add_task("Sending messages...", total=len(enriched))
        sent = await send_messages(enriched, pool, templates, conn,
                                   delay=settings["delay"], progress=progress)
    logger.info(f"Sender done: {sent} messages sent")
    return sent


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
                settings = show_settings_menu(settings)
            elif choice == "5":
                stats = await get_stats(conn)
                show_stats(stats, pool.total, templates.count)
            elif choice == "6":
                console.print("[bold yellow]Goodbye![/bold yellow]")
                break
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
