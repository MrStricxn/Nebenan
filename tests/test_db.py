import pytest
import pytest_asyncio
import aiosqlite
import os
from src.db import init_db, upsert_listing, is_seller_new, mark_seller_contacted, get_stats

TEST_DB = "data/test_nebena.db"

@pytest_asyncio.fixture
async def conn():
    db = await init_db(TEST_DB)
    yield db
    await db.close()
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

@pytest.mark.asyncio
async def test_init_db_creates_tables(conn):
    async with conn.execute("SELECT name FROM sqlite_master WHERE type='table'") as cur:
        tables = {row[0] async for row in cur}
    assert "sellers" in tables
    assert "listings" in tables

@pytest.mark.asyncio
async def test_new_seller_is_new(conn):
    assert await is_seller_new(conn, "seller_999") is True

@pytest.mark.asyncio
async def test_upsert_listing_creates_seller(conn):
    listing = {
        "listing_id": "l1",
        "seller_id": "s1",
        "seller_name": "Hans",
        "title": "Sofa",
        "url": "https://nebenan.de/l1",
        "published_at": "2026-06-29T10:00:00",
    }
    await upsert_listing(conn, listing)
    assert await is_seller_new(conn, "s1") is False

@pytest.mark.asyncio
async def test_upsert_listing_idempotent(conn):
    listing = {
        "listing_id": "l2",
        "seller_id": "s2",
        "seller_name": "Anna",
        "title": "Tisch",
        "url": "https://nebenan.de/l2",
        "published_at": "2026-06-29T11:00:00",
    }
    await upsert_listing(conn, listing)
    await upsert_listing(conn, listing)  # second call must not raise
    async with conn.execute("SELECT COUNT(*) FROM listings WHERE listing_id='l2'") as cur:
        row = await cur.fetchone()
    assert row[0] == 1

@pytest.mark.asyncio
async def test_mark_seller_contacted(conn):
    listing = {
        "listing_id": "l3",
        "seller_id": "s3",
        "seller_name": "Klaus",
        "title": "Stuhl",
        "url": "https://nebenan.de/l3",
        "published_at": "2026-06-29T12:00:00",
    }
    await upsert_listing(conn, listing)
    await mark_seller_contacted(conn, "s3")
    async with conn.execute("SELECT message_sent FROM sellers WHERE seller_id='s3'") as cur:
        row = await cur.fetchone()
    assert row[0] == 1

@pytest.mark.asyncio
async def test_get_stats(conn):
    for i in range(3):
        await upsert_listing(conn, {
            "listing_id": f"ls{i}",
            "seller_id": f"ss{i}",
            "seller_name": f"Person{i}",
            "title": f"Item{i}",
            "url": f"https://nebenan.de/ls{i}",
            "published_at": "2026-06-29T10:00:00",
        })
    await mark_seller_contacted(conn, "ss0")
    stats = await get_stats(conn)
    assert stats["total_sellers"] == 3
    assert stats["messaged"] == 1
    assert stats["total_listings"] == 3
