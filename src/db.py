import aiosqlite
from datetime import datetime, timezone


async def init_db(db_path: str = "data/nebena.db") -> aiosqlite.Connection:
    conn = await aiosqlite.connect(db_path)
    await conn.executescript("""
        CREATE TABLE IF NOT EXISTS sellers (
            seller_id   TEXT PRIMARY KEY,
            seller_name TEXT,
            first_seen_at TEXT,
            message_sent  INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS listings (
            listing_id  TEXT PRIMARY KEY,
            seller_id   TEXT,
            title       TEXT,
            url         TEXT,
            published_at TEXT,
            parsed_at   TEXT
        );
    """)
    await conn.commit()
    return conn


async def upsert_listing(conn: aiosqlite.Connection, listing: dict) -> None:
    now = datetime.now(timezone.utc).isoformat()
    await conn.execute(
        """
        INSERT OR IGNORE INTO sellers (seller_id, seller_name, first_seen_at)
        VALUES (?, ?, ?)
        """,
        (listing["seller_id"], listing["seller_name"], now),
    )
    await conn.execute(
        """
        INSERT OR IGNORE INTO listings
            (listing_id, seller_id, title, url, published_at, parsed_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            listing["listing_id"],
            listing["seller_id"],
            listing["title"],
            listing["url"],
            listing["published_at"],
            now,
        ),
    )
    await conn.commit()


async def is_seller_new(conn: aiosqlite.Connection, seller_id: str) -> bool:
    async with conn.execute(
        "SELECT 1 FROM sellers WHERE seller_id = ?", (seller_id,)
    ) as cur:
        row = await cur.fetchone()
    return row is None


async def mark_seller_contacted(conn: aiosqlite.Connection, seller_id: str) -> None:
    await conn.execute(
        "UPDATE sellers SET message_sent = 1 WHERE seller_id = ?", (seller_id,)
    )
    await conn.commit()


async def get_stats(conn: aiosqlite.Connection) -> dict:
    async with conn.execute("SELECT COUNT(*) FROM sellers") as cur:
        total_sellers = (await cur.fetchone())[0]
    async with conn.execute(
        "SELECT COUNT(*) FROM sellers WHERE message_sent = 1"
    ) as cur:
        messaged = (await cur.fetchone())[0]
    async with conn.execute("SELECT COUNT(*) FROM listings") as cur:
        total_listings = (await cur.fetchone())[0]
    return {
        "total_sellers": total_sellers,
        "messaged": messaged,
        "total_listings": total_listings,
    }
