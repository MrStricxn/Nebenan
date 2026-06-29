import aiosqlite
from datetime import datetime, timezone


async def init_db(db_path: str = "data/nebena.db") -> aiosqlite.Connection:
    conn = await aiosqlite.connect(db_path)
    await conn.executescript("""
        CREATE TABLE IF NOT EXISTS sellers (
            seller_id    TEXT PRIMARY KEY,
            seller_name  TEXT,
            first_seen_at TEXT,
            message_sent  INTEGER DEFAULT 0,
            replied       INTEGER DEFAULT 0,
            phase2_sent   INTEGER DEFAULT 0,
            message_url   TEXT
        );
        CREATE TABLE IF NOT EXISTS listings (
            listing_id  TEXT PRIMARY KEY,
            seller_id   TEXT,
            title       TEXT,
            price       TEXT,
            category    TEXT,
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
        INSERT OR IGNORE INTO sellers (seller_id, seller_name, first_seen_at, message_url)
        VALUES (?, ?, ?, ?)
        """,
        (listing["seller_id"], listing["seller_name"], now, listing.get("message_url", "")),
    )
    await conn.execute(
        """
        INSERT OR IGNORE INTO listings
            (listing_id, seller_id, title, price, category, url, published_at, parsed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            listing["listing_id"],
            listing["seller_id"],
            listing["title"],
            listing.get("price", ""),
            listing.get("category", ""),
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


async def mark_seller_replied(conn: aiosqlite.Connection, seller_id: str) -> None:
    await conn.execute(
        "UPDATE sellers SET replied = 1 WHERE seller_id = ?", (seller_id,)
    )
    await conn.commit()


async def mark_phase2_sent(conn: aiosqlite.Connection, seller_id: str) -> None:
    await conn.execute(
        "UPDATE sellers SET phase2_sent = 1 WHERE seller_id = ?", (seller_id,)
    )
    await conn.commit()


async def get_sellers_awaiting_reply(conn: aiosqlite.Connection) -> list[dict]:
    """Sellers who got Phase 1 message but haven't received Phase 2 yet."""
    async with conn.execute(
        """
        SELECT seller_id, seller_name, message_url
        FROM sellers
        WHERE message_sent = 1 AND replied = 1 AND phase2_sent = 0
        """
    ) as cur:
        rows = await cur.fetchall()
    return [{"seller_id": r[0], "seller_name": r[1], "message_url": r[2]} for r in rows]


async def get_stats(conn: aiosqlite.Connection) -> dict:
    async with conn.execute("SELECT COUNT(*) FROM sellers") as cur:
        total_sellers = (await cur.fetchone())[0]
    async with conn.execute(
        "SELECT COUNT(*) FROM sellers WHERE message_sent = 1"
    ) as cur:
        messaged = (await cur.fetchone())[0]
    async with conn.execute("SELECT COUNT(*) FROM listings") as cur:
        total_listings = (await cur.fetchone())[0]
    async with conn.execute(
        "SELECT COUNT(*) FROM sellers WHERE replied = 1"
    ) as cur:
        replied = (await cur.fetchone())[0]
    async with conn.execute(
        "SELECT COUNT(*) FROM sellers WHERE phase2_sent = 1"
    ) as cur:
        phase2_sent = (await cur.fetchone())[0]
    return {
        "total_sellers": total_sellers,
        "messaged": messaged,
        "replied": replied,
        "phase2_sent": phase2_sent,
        "total_listings": total_listings,
    }
