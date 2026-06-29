import pytest
import pytest_asyncio
import os
from unittest.mock import AsyncMock, MagicMock
from src.sender import _filter_uncontacted
from src.db import init_db, upsert_listing, mark_seller_contacted

TEST_DB = "data/test_sender.db"

@pytest_asyncio.fixture
async def conn():
    db = await init_db(TEST_DB)
    yield db
    await db.close()
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)

@pytest.mark.asyncio
async def test_filter_removes_already_contacted(conn):
    await upsert_listing(conn, {
        "listing_id": "l1", "seller_id": "s1", "seller_name": "Hans",
        "title": "Sofa", "url": "https://nebenan.de/l1", "published_at": "2026-06-29T10:00:00"
    })
    await mark_seller_contacted(conn, "s1")
    await upsert_listing(conn, {
        "listing_id": "l2", "seller_id": "s2", "seller_name": "Anna",
        "title": "Tisch", "url": "https://nebenan.de/l2", "published_at": "2026-06-29T10:00:00"
    })
    sellers = [
        {"seller_id": "s1", "seller_name": "Hans", "url": "https://nebenan.de/l1"},
        {"seller_id": "s2", "seller_name": "Anna", "url": "https://nebenan.de/l2"},
    ]
    result = await _filter_uncontacted(conn, sellers)
    assert len(result) == 1
    assert result[0]["seller_id"] == "s2"

@pytest.mark.asyncio
async def test_filter_empty_list(conn):
    result = await _filter_uncontacted(conn, [])
    assert result == []
