import pytest
import pytest_asyncio
import json
import os
from src.accounts import AccountPool

FIXTURE_DIR = "tests/fixtures/cookies"


@pytest.mark.asyncio
async def test_load_accounts():
    pool = AccountPool(FIXTURE_DIR)
    await pool.load()
    assert pool.total == 1
    assert pool.available == 1


@pytest.mark.asyncio
async def test_checkout_and_release():
    pool = AccountPool(FIXTURE_DIR)
    await pool.load()
    name, state = await pool.checkout()
    assert name == "valid_account"
    assert "cookies" in state
    assert pool.available == 0
    await pool.release(name)
    assert pool.available == 1


@pytest.mark.asyncio
async def test_empty_cookies_dir(tmp_path):
    pool = AccountPool(str(tmp_path))
    await pool.load()
    assert pool.total == 0


@pytest.mark.asyncio
async def test_invalid_json_skipped(tmp_path):
    bad = tmp_path / "broken.json"
    bad.write_text("not valid json")
    pool = AccountPool(str(tmp_path))
    await pool.load()
    assert pool.total == 0
