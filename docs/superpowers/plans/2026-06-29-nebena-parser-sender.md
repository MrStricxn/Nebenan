# NEbena Parser + Sender — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a multi-account async parser + first-message sender for nebenan.de marketplace listings with Rich CLI, SQLite dedup, and Playwright browser automation.

**Architecture:** Playwright async API drives one browser context per cookie account; BeautifulSoup parses HTML chunks; asyncio Semaphore controls concurrency. SQLite (via aiosqlite) deduplicates sellers across runs. Rich powers the CLI menu, progress bars, and logging.

**Tech Stack:** Python 3.11+, playwright, beautifulsoup4, aiosqlite, rich

## Global Constraints

- Python 3.11+ required
- All async I/O via `asyncio` — no `threading` or `multiprocessing`
- Playwright `async_playwright` only (not sync)
- DB file at `data/nebena.db` (relative to project root)
- Cookie files at `cookies/*.json` in Playwright `storage_state` format
- Templates file at `Shablon.txt` (project root), sections split by `---`
- Log file at `logs/nebena.log`, rotating, max 5MB × 3 backups
- No hardcoded nebenan.de URLs in tests — mock at the HTTP level
- All source files in `src/`, entry point at `main.py`

---

### Task 1: Project Scaffold + Dependencies

**Files:**
- Create: `requirements.txt`
- Create: `requirements-dev.txt`
- Create: `cookies/.gitkeep`
- Create: `data/.gitkeep`
- Create: `logs/.gitkeep`
- Create: `Shablon.txt`
- Create: `.gitignore`
- Create: `src/__init__.py`
- Create: `tests/__init__.py`

- [ ] **Step 1: Create requirements.txt**

```
playwright==1.44.0
beautifulsoup4==4.12.3
aiosqlite==0.20.0
rich==13.7.1
```

- [ ] **Step 2: Create requirements-dev.txt**

```
pytest==8.2.0
pytest-asyncio==0.23.7
```

- [ ] **Step 3: Create directory structure and placeholder files**

```bash
mkdir -p cookies data logs src tests
touch cookies/.gitkeep data/.gitkeep logs/.gitkeep
touch src/__init__.py tests/__init__.py
```

- [ ] **Step 4: Create Shablon.txt with sample templates**

```
Hallo! Ich habe Ihr Inserat gesehen und interessiere mich sehr dafür. Ist der Artikel noch verfügbar?
---
Guten Tag! Ich bin an Ihrem Angebot interessiert. Können wir uns auf einen Preis einigen?
---
Hi! Tolles Angebot! Ich würde es gerne kaufen. Wann könnten wir uns treffen?
```

- [ ] **Step 5: Create .gitignore**

```
__pycache__/
*.pyc
*.pyo
.pytest_cache/
data/nebena.db
logs/*.log
cookies/*.json
.env
```

- [ ] **Step 6: Install dependencies**

```bash
pip install -r requirements.txt
pip install -r requirements-dev.txt
playwright install chromium
```

Expected: no errors, `playwright install chromium` downloads ~130MB browser.

- [ ] **Step 7: Verify imports work**

```bash
python -c "import playwright; import bs4; import aiosqlite; import rich; print('OK')"
```

Expected output: `OK`

- [ ] **Step 8: Commit**

```bash
git init
git add .
git commit -m "chore: project scaffold with dependencies"
```

---

### Task 2: Database Layer (db.py)

**Files:**
- Create: `src/db.py`
- Create: `tests/test_db.py`

**Interfaces:**
- Produces:
  - `async init_db(db_path: str) -> aiosqlite.Connection`
  - `async upsert_listing(conn, listing: dict) -> None`  
    listing keys: `listing_id`, `seller_id`, `seller_name`, `title`, `url`, `published_at`
  - `async is_seller_new(conn, seller_id: str) -> bool`
  - `async mark_seller_contacted(conn, seller_id: str) -> None`
  - `async get_stats(conn) -> dict`  
    returns `{"total_sellers": int, "messaged": int, "total_listings": int}`

- [ ] **Step 1: Write failing tests**

Create `tests/test_db.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_db.py -v
```

Expected: `ImportError` or `ModuleNotFoundError` — `src.db` doesn't exist yet.

- [ ] **Step 3: Implement src/db.py**

```python
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_db.py -v
```

Expected: all 6 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/db.py tests/test_db.py
git commit -m "feat: SQLite database layer with seller dedup"
```

---

### Task 3: Account Pool (accounts.py)

**Files:**
- Create: `src/accounts.py`
- Create: `tests/test_accounts.py`
- Create: `tests/fixtures/cookies/valid_account.json`

**Interfaces:**
- Consumes: `cookies/*.json` files in Playwright `storage_state` format
- Produces:
  - `class AccountPool`
    - `AccountPool(cookies_dir: str = "cookies")` 
    - `async def load() -> None` — scans dir, loads JSON files
    - `async def checkout() -> tuple[str, dict]` — returns `(name, storage_state)`, blocks if none free
    - `async def release(name: str) -> None` — returns account to pool
    - `property available: int` — number of free accounts
    - `property total: int` — total loaded accounts

- [ ] **Step 1: Create fixture cookie file**

Create `tests/fixtures/cookies/valid_account.json`:

```json
{
  "cookies": [
    {
      "name": "session",
      "value": "abc123",
      "domain": ".nebenan.de",
      "path": "/",
      "expires": 9999999999,
      "httpOnly": true,
      "secure": true,
      "sameSite": "Lax"
    }
  ],
  "origins": []
}
```

- [ ] **Step 2: Write failing tests**

Create `tests/test_accounts.py`:

```python
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
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
pytest tests/test_accounts.py -v
```

Expected: `ImportError` — `src.accounts` not found.

- [ ] **Step 4: Implement src/accounts.py**

```python
import asyncio
import json
import os
from pathlib import Path


class AccountPool:
    def __init__(self, cookies_dir: str = "cookies"):
        self._dir = Path(cookies_dir)
        self._accounts: dict[str, dict] = {}
        self._free: asyncio.Queue = asyncio.Queue()

    async def load(self) -> None:
        if not self._dir.exists():
            return
        for path in self._dir.glob("*.json"):
            try:
                state = json.loads(path.read_text(encoding="utf-8"))
                name = path.stem
                self._accounts[name] = state
                await self._free.put(name)
            except (json.JSONDecodeError, OSError):
                pass  # invalid file — skip silently, caller logs warning

    async def checkout(self) -> tuple[str, dict]:
        name = await self._free.get()
        return name, self._accounts[name]

    async def release(self, name: str) -> None:
        await self._free.put(name)

    @property
    def available(self) -> int:
        return self._free.qsize()

    @property
    def total(self) -> int:
        return len(self._accounts)
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
pytest tests/test_accounts.py -v
```

Expected: all 4 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add src/accounts.py tests/test_accounts.py tests/fixtures/cookies/valid_account.json
git commit -m "feat: async account pool with cookie file loading"
```

---

### Task 4: Templates (templates.py)

**Files:**
- Create: `src/templates.py`
- Create: `tests/test_templates.py`

**Interfaces:**
- Consumes: path to `Shablon.txt`
- Produces:
  - `class TemplateLoader`
    - `TemplateLoader(path: str = "Shablon.txt")`
    - `def load() -> None` — reads and parses file
    - `def get_random() -> str` — returns one random template string
    - `property count: int` — number of loaded templates

- [ ] **Step 1: Write failing tests**

Create `tests/test_templates.py`:

```python
import pytest
import os
from src.templates import TemplateLoader


def test_load_three_templates(tmp_path):
    f = tmp_path / "Shablon.txt"
    f.write_text("Hello A\n---\nHello B\n---\nHello C", encoding="utf-8")
    loader = TemplateLoader(str(f))
    loader.load()
    assert loader.count == 3


def test_get_random_returns_string(tmp_path):
    f = tmp_path / "Shablon.txt"
    f.write_text("Template one\n---\nTemplate two", encoding="utf-8")
    loader = TemplateLoader(str(f))
    loader.load()
    result = loader.get_random()
    assert result in ("Template one", "Template two")


def test_strips_whitespace(tmp_path):
    f = tmp_path / "Shablon.txt"
    f.write_text("  Hello  \n---\n  World  ", encoding="utf-8")
    loader = TemplateLoader(str(f))
    loader.load()
    assert loader.count == 2
    result = loader.get_random()
    assert result in ("Hello", "World")


def test_empty_sections_ignored(tmp_path):
    f = tmp_path / "Shablon.txt"
    f.write_text("Hello\n---\n\n---\nWorld", encoding="utf-8")
    loader = TemplateLoader(str(f))
    loader.load()
    assert loader.count == 2


def test_file_not_found_raises():
    loader = TemplateLoader("nonexistent_file.txt")
    with pytest.raises(FileNotFoundError):
        loader.load()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_templates.py -v
```

Expected: `ImportError` — `src.templates` not found.

- [ ] **Step 3: Implement src/templates.py**

```python
import random
from pathlib import Path


class TemplateLoader:
    def __init__(self, path: str = "Shablon.txt"):
        self._path = Path(path)
        self._templates: list[str] = []

    def load(self) -> None:
        if not self._path.exists():
            raise FileNotFoundError(f"Templates file not found: {self._path}")
        raw = self._path.read_text(encoding="utf-8")
        self._templates = [t.strip() for t in raw.split("---") if t.strip()]

    def get_random(self) -> str:
        return random.choice(self._templates)

    @property
    def count(self) -> int:
        return len(self._templates)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_templates.py -v
```

Expected: all 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/templates.py tests/test_templates.py
git commit -m "feat: template loader with random selection from Shablon.txt"
```

---

### Task 5: Parser (parser.py)

**Files:**
- Create: `src/parser.py`
- Create: `tests/test_parser.py`

**Interfaces:**
- Consumes:
  - `AccountPool.checkout()` / `AccountPool.release()` from `src.accounts`
  - Playwright `async_playwright`
- Produces:
  - `async parse_listings(account_pool: AccountPool, hours: int = 24, progress=None) -> list[dict]`
    - Returns list of listing dicts with keys: `listing_id`, `seller_id`, `seller_name`, `title`, `price`, `url`, `published_at`
  - `def extract_listings_from_html(html: str) -> list[dict]` — pure BS4 function, testable without browser

**Note on testing:** `parse_listings` requires a real browser and valid cookies — tested manually. `extract_listings_from_html` is pure and fully unit-testable.

- [ ] **Step 1: Write failing tests for the pure extraction function**

Create `tests/test_parser.py`:

```python
import pytest
from src.parser import extract_listings_from_html


SAMPLE_HTML = """
<div class="marketplace-item" data-listing-id="123" data-seller-id="seller_abc">
  <a class="seller-link" href="/profile/seller_abc">Hans Mueller</a>
  <h2 class="listing-title">Vintage Sofa</h2>
  <span class="listing-price">50 €</span>
  <a class="listing-url" href="/marketplace/123">Details</a>
  <time datetime="2026-06-29T10:30:00Z">vor 2 Stunden</time>
</div>
<div class="marketplace-item" data-listing-id="456" data-seller-id="seller_xyz">
  <a class="seller-link" href="/profile/seller_xyz">Anna Schmidt</a>
  <h2 class="listing-title">Tisch</h2>
  <span class="listing-price">30 €</span>
  <a class="listing-url" href="/marketplace/456">Details</a>
  <time datetime="2026-06-29T08:00:00Z">vor 4 Stunden</time>
</div>
"""


def test_extract_returns_two_listings():
    listings = extract_listings_from_html(SAMPLE_HTML)
    assert len(listings) == 2


def test_extract_listing_fields():
    listings = extract_listings_from_html(SAMPLE_HTML)
    first = listings[0]
    assert first["listing_id"] == "123"
    assert first["seller_id"] == "seller_abc"
    assert first["seller_name"] == "Hans Mueller"
    assert first["title"] == "Vintage Sofa"
    assert first["price"] == "50 €"
    assert "nebenan.de" in first["url"] or first["url"].startswith("/marketplace/")
    assert first["published_at"] == "2026-06-29T10:30:00Z"


def test_extract_empty_html():
    listings = extract_listings_from_html("<html><body></body></html>")
    assert listings == []
```

**Important:** The HTML selectors in the test (`data-listing-id`, `data-seller-id`, `.listing-title`, etc.) are placeholders. Before implementing `parser.py`, you MUST inspect the real nebenan.de marketplace HTML (open DevTools → Elements) and update the selectors in both `test_parser.py` and `parser.py` to match actual DOM structure.

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_parser.py -v
```

Expected: `ImportError` — `src.parser` not found.

- [ ] **Step 3: Inspect nebenan.de marketplace HTML**

Open `https://nebenan.de/marketplace` in a browser (logged in). Open DevTools → Elements. Find:
- Container element for each listing item (class name or data attribute)
- Where `listing_id` is stored (data attribute, URL, etc.)
- Where `seller_id` is stored
- Where seller name text is
- Where title text is
- Where price text is
- Where the listing URL href is
- Where `published_at` is (look for `<time datetime="...">` or data attribute)

Update the selectors in `tests/test_parser.py` SAMPLE_HTML and assertions to match real DOM before continuing.

- [ ] **Step 4: Implement src/parser.py**

```python
import asyncio
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, BrowserContext


# UPDATE THESE SELECTORS after inspecting nebenan.de DOM
_LISTING_CONTAINER = "div.marketplace-item"        # container per listing
_ATTR_LISTING_ID   = "data-listing-id"             # attr on container
_ATTR_SELLER_ID    = "data-seller-id"              # attr on container
_SEL_SELLER_NAME   = "a.seller-link"               # text = seller name
_SEL_TITLE         = "h2.listing-title"            # text = listing title
_SEL_PRICE         = "span.listing-price"          # text = price
_SEL_URL           = "a.listing-url"               # href = listing path
_SEL_TIME          = "time"                        # datetime attr = ISO timestamp

BASE_URL = "https://nebenan.de"


def extract_listings_from_html(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    results = []
    for item in soup.select(_LISTING_CONTAINER):
        try:
            listing_id = item.get(_ATTR_LISTING_ID, "")
            seller_id  = item.get(_ATTR_SELLER_ID, "")
            seller_tag = item.select_one(_SEL_SELLER_NAME)
            title_tag  = item.select_one(_SEL_TITLE)
            price_tag  = item.select_one(_SEL_PRICE)
            url_tag    = item.select_one(_SEL_URL)
            time_tag   = item.select_one(_SEL_TIME)
            if not (listing_id and seller_id):
                continue
            url = url_tag["href"] if url_tag else ""
            if url and not url.startswith("http"):
                url = BASE_URL + url
            results.append({
                "listing_id":  listing_id,
                "seller_id":   seller_id,
                "seller_name": seller_tag.get_text(strip=True) if seller_tag else "",
                "title":       title_tag.get_text(strip=True) if title_tag else "",
                "price":       price_tag.get_text(strip=True) if price_tag else "",
                "url":         url,
                "published_at": time_tag.get("datetime", "") if time_tag else "",
            })
        except Exception:
            continue
    return results


def _is_within_hours(published_at: str, hours: int) -> bool:
    if not published_at:
        return False
    try:
        dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        return dt >= cutoff
    except ValueError:
        return False


async def _parse_with_account(
    storage_state: dict,
    hours: int,
    semaphore: asyncio.Semaphore,
    progress=None,
) -> list[dict]:
    async with semaphore:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True)
            ctx: BrowserContext = await browser.new_context(storage_state=storage_state)
            page = await ctx.new_page()
            listings = []
            url = f"{BASE_URL}/marketplace?sort=newest"
            stop = False
            while not stop:
                await page.goto(url, timeout=30000)
                html = await page.content()
                page_listings = extract_listings_from_html(html)
                if not page_listings:
                    break
                for lst in page_listings:
                    if _is_within_hours(lst["published_at"], hours):
                        listings.append(lst)
                        if progress:
                            progress.advance(progress.task_ids[0])
                    else:
                        stop = True
                        break
                # pagination: find next page URL or break
                soup = BeautifulSoup(html, "html.parser")
                next_link = soup.select_one("a[rel='next']")
                if next_link and not stop:
                    url = BASE_URL + next_link["href"]
                else:
                    break
            await browser.close()
            return listings


async def parse_listings(
    account_pool,
    hours: int = 24,
    progress=None,
) -> list[dict]:
    semaphore = asyncio.Semaphore(account_pool.total or 1)
    tasks = []
    for _ in range(account_pool.total):
        name, state = await account_pool.checkout()
        task = asyncio.create_task(
            _parse_with_account(state, hours, semaphore, progress)
        )
        task._account_name = name
        tasks.append((name, task))
    results = await asyncio.gather(*[t for _, t in tasks], return_exceptions=True)
    for (name, _), result in zip(tasks, results):
        await account_pool.release(name)
    all_listings: list[dict] = []
    for result in results:
        if isinstance(result, list):
            all_listings.extend(result)
    # dedup by listing_id across accounts
    seen = set()
    unique = []
    for lst in all_listings:
        if lst["listing_id"] not in seen:
            seen.add(lst["listing_id"])
            unique.append(lst)
    return unique
```

- [ ] **Step 5: Run unit tests to verify they pass**

```bash
pytest tests/test_parser.py -v
```

Expected: all 3 tests PASS (these test only `extract_listings_from_html`).

- [ ] **Step 6: Commit**

```bash
git add src/parser.py tests/test_parser.py
git commit -m "feat: Playwright+BeautifulSoup parser with 24h listing filter"
```

---

### Task 6: Sender (sender.py)

**Files:**
- Create: `src/sender.py`
- Create: `tests/test_sender.py`

**Interfaces:**
- Consumes:
  - `AccountPool.checkout()` / `AccountPool.release()` from `src.accounts`
  - `TemplateLoader.get_random()` from `src.templates`
  - `mark_seller_contacted(conn, seller_id)` from `src.db`
  - `aiosqlite.Connection`
- Produces:
  - `async send_messages(sellers: list[dict], account_pool, template_loader, conn, delay: float = 2.0, progress=None) -> int`
    - `sellers` items: `{"seller_id": str, "seller_name": str, "url": str}`
    - Returns count of successfully sent messages

**Note:** Playwright interactions with the real site are tested manually. Unit tests cover the pure logic: skip already-contacted sellers.

- [ ] **Step 1: Write failing tests**

Create `tests/test_sender.py`:

```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
pytest tests/test_sender.py -v
```

Expected: `ImportError` — `src.sender` not found.

- [ ] **Step 3: Implement src/sender.py**

```python
import asyncio
from playwright.async_api import async_playwright
from src.db import mark_seller_contacted

BASE_URL = "https://nebenan.de"

# UPDATE THESE SELECTORS after inspecting nebenan.de listing page DOM
_BTN_MESSAGE     = "button[data-action='send-message']"  # message button on listing page
_INPUT_MESSAGE   = "textarea.message-input"              # message textarea
_BTN_SEND        = "button[type='submit'].send-btn"      # submit button


async def _filter_uncontacted(conn, sellers: list[dict]) -> list[dict]:
    result = []
    for seller in sellers:
        async with conn.execute(
            "SELECT message_sent FROM sellers WHERE seller_id = ?",
            (seller["seller_id"],)
        ) as cur:
            row = await cur.fetchone()
        if row is None or row[0] == 0:
            result.append(seller)
    return result


async def _send_one(page, seller: dict, template: str) -> bool:
    try:
        await page.goto(seller["url"], timeout=30000)
        await page.click(_BTN_MESSAGE, timeout=10000)
        await page.fill(_INPUT_MESSAGE, template)
        await page.click(_BTN_SEND, timeout=10000)
        return True
    except Exception:
        return False


async def send_messages(
    sellers: list[dict],
    account_pool,
    template_loader,
    conn,
    delay: float = 2.0,
    progress=None,
) -> int:
    uncontacted = await _filter_uncontacted(conn, sellers)
    if not uncontacted:
        return 0

    sent_count = 0
    semaphore = asyncio.Semaphore(account_pool.total or 1)

    async def _worker(seller: dict):
        nonlocal sent_count
        async with semaphore:
            name, state = await account_pool.checkout()
            try:
                async with async_playwright() as pw:
                    browser = await pw.chromium.launch(headless=True)
                    ctx = await browser.new_context(storage_state=state)
                    page = await ctx.new_page()
                    template = template_loader.get_random()
                    success = await _send_one(page, seller, template)
                    if success:
                        await mark_seller_contacted(conn, seller["seller_id"])
                        sent_count += 1
                    await browser.close()
            finally:
                await account_pool.release(name)
            if progress:
                progress.advance(progress.task_ids[0])
            await asyncio.sleep(delay)

    await asyncio.gather(*[_worker(s) for s in uncontacted])
    return sent_count
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
pytest tests/test_sender.py -v
```

Expected: both tests PASS.

- [ ] **Step 5: Commit**

```bash
git add src/sender.py tests/test_sender.py
git commit -m "feat: async sender with per-account Playwright contexts"
```

---

### Task 7: CLI + Logging (cli.py)

**Files:**
- Create: `src/cli.py`

**Interfaces:**
- Consumes:
  - `get_stats(conn)` from `src.db`
  - `AccountPool.total` from `src.accounts`
  - `TemplateLoader.count` from `src.templates`
- Produces:
  - `def setup_logging() -> logging.Logger` — returns configured Rich logger with file rotation
  - `def show_menu() -> str` — prints menu, returns user choice ("1"–"6")
  - `def make_progress(description: str, total: int) -> rich.progress.Progress` — returns configured Progress context manager
  - `def show_stats(stats: dict) -> None` — prints stats table

No automated tests for this module — it's pure UI. Verified by running the app.

- [ ] **Step 1: Implement src/cli.py**

```python
import logging
import os
from logging.handlers import RotatingFileHandler
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich import print as rprint

console = Console()

BANNER = """
[bold cyan]
  ███╗   ██╗███████╗██████╗ ███████╗███╗   ██╗ █████╗ ███╗   ██╗
  ████╗  ██║██╔════╝██╔══██╗██╔════╝████╗  ██║██╔══██╗████╗  ██║
  ██╔██╗ ██║█████╗  ██████╔╝█████╗  ██╔██╗ ██║███████║██╔██╗ ██║
  ██║╚██╗██║██╔══╝  ██╔══██╗██╔══╝  ██║╚██╗██║██╔══██║██║╚██╗██║
  ██║ ╚████║███████╗██████╔╝███████╗██║ ╚████║██║  ██║██║ ╚████║
  ╚═╝  ╚═══╝╚══════╝╚═════╝ ╚══════╝╚═╝  ╚═══╝╚═╝  ╚═╝╚═╝  ╚═══╝
[/bold cyan]
[dim]nebenan.de Parser + Sender[/dim]
"""


def setup_logging() -> logging.Logger:
    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger("nebena")
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        return logger
    file_handler = RotatingFileHandler(
        "logs/nebena.log", maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    )
    rich_handler = RichHandler(console=console, rich_tracebacks=True, show_path=False)
    rich_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(rich_handler)
    return logger


def show_menu() -> str:
    console.print(BANNER)
    console.print(Panel(
        "[1] Run Parser\n"
        "[2] Run Sender\n"
        "[3] Run Both (Parser → Sender)\n"
        "[4] Settings\n"
        "[5] Statistics\n"
        "[6] Exit",
        title="[bold yellow]Main Menu[/bold yellow]",
        border_style="yellow",
    ))
    return Prompt.ask("[bold]Choose option[/bold]", choices=["1","2","3","4","5","6"])


def show_settings_menu(current: dict) -> dict:
    console.print(Panel(
        f"Current settings:\n"
        f"  Hours lookback : [cyan]{current['hours']}[/cyan]\n"
        f"  Message delay  : [cyan]{current['delay']}s[/cyan]\n"
        f"  Max accounts   : [cyan]{current['max_accounts']} (auto = all cookies)[/cyan]",
        title="[bold yellow]Settings[/bold yellow]",
        border_style="yellow",
    ))
    hours = Prompt.ask("Hours lookback", default=str(current["hours"]))
    delay = Prompt.ask("Delay between messages (seconds)", default=str(current["delay"]))
    return {"hours": int(hours), "delay": float(delay), "max_accounts": current["max_accounts"]}


def make_progress(description: str, total: int) -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("•"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    )


def show_stats(stats: dict, accounts_total: int, templates_count: int) -> None:
    table = Table(title="Statistics", border_style="cyan", show_header=True)
    table.add_column("Metric", style="bold")
    table.add_column("Value", style="cyan")
    table.add_row("Total sellers found", str(stats["total_sellers"]))
    table.add_row("Sellers messaged", str(stats["messaged"]))
    table.add_row("Total listings parsed", str(stats["total_listings"]))
    table.add_row("Accounts loaded", str(accounts_total))
    table.add_row("Templates loaded", str(templates_count))
    console.print(table)
```

- [ ] **Step 2: Verify import works**

```bash
python -c "from src.cli import setup_logging, show_menu; print('OK')"
```

Expected: `OK`

- [ ] **Step 3: Commit**

```bash
git add src/cli.py
git commit -m "feat: Rich CLI menu, logging, progress bar, stats table"
```

---

### Task 8: Orchestrator + Entry Point (main.py)

**Files:**
- Create: `main.py`

**Interfaces:**
- Consumes all previous modules:
  - `AccountPool` from `src.accounts`
  - `TemplateLoader` from `src.templates`
  - `init_db, get_stats, upsert_listing, is_seller_new` from `src.db`
  - `parse_listings` from `src.parser`
  - `send_messages` from `src.sender`
  - `setup_logging, show_menu, show_settings_menu, make_progress, show_stats` from `src.cli`

- [ ] **Step 1: Implement main.py**

```python
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
        await upsert_listing(conn, lst)
        if await is_seller_new(conn, lst["seller_id"]):
            new_sellers.append({
                "seller_id": lst["seller_id"],
                "seller_name": lst["seller_name"],
                "url": lst["url"],
            })
    logger.info(f"Parser done: {len(listings)} listings, {len(new_sellers)} new sellers")
    return new_sellers


async def run_sender(pool, templates, conn, settings, logger):
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
```

- [ ] **Step 2: Verify the app starts**

```bash
python main.py
```

Expected: Banner + menu appear. Press `6` to exit cleanly.

- [ ] **Step 3: Run full test suite**

```bash
pytest tests/ -v
```

Expected: all tests PASS.

- [ ] **Step 4: Commit**

```bash
git add main.py
git commit -m "feat: main orchestrator with async event loop and CLI menu"
```

---

### Task 9: Selector Discovery + Integration Test

**Files:**
- Modify: `src/parser.py` (update selectors)
- Modify: `src/sender.py` (update selectors)
- Modify: `tests/test_parser.py` (update SAMPLE_HTML)

This task cannot be automated — it requires a real nebenan.de account.

- [ ] **Step 1: Log in to nebenan.de and navigate to marketplace**

Open `https://nebenan.de/marketplace` in Chrome/Firefox with DevTools open.

- [ ] **Step 2: Inspect listing item DOM**

Right-click a listing → Inspect. Find and record:
- Container selector (class, data attribute)
- Where `listing_id` is (URL slug, data attr, hidden input)
- Where `seller_id` is
- Seller name element
- Title element
- Price element
- Listing detail URL
- Published time element (look for `<time>` tag with `datetime` attr)

- [ ] **Step 3: Update selectors in src/parser.py**

Replace the `_LISTING_CONTAINER`, `_ATTR_LISTING_ID`, etc. constants at the top of `src/parser.py` with real values found in Step 2.

- [ ] **Step 4: Update SAMPLE_HTML in tests/test_parser.py**

Replace `SAMPLE_HTML` with a real HTML snippet copied from the browser (anonymise if needed), matching the actual DOM structure. Update assertions to match real field names/values.

- [ ] **Step 5: Inspect listing detail page for message button**

Open any listing detail page. Find:
- Message button selector
- Message textarea selector
- Send/submit button selector

Update constants in `src/sender.py`.

- [ ] **Step 6: Run updated parser tests**

```bash
pytest tests/test_parser.py -v
```

Expected: all tests PASS with real selectors.

- [ ] **Step 7: Manual integration test — parser**

Add one real cookie file to `cookies/` and run:

```bash
python main.py
```

Choose `[1] Run Parser`. Verify:
- Browser opens (or runs headless)
- Listings appear in Rich progress
- `data/nebena.db` has rows in `listings` and `sellers` tables

```bash
python -c "import asyncio, aiosqlite; asyncio.run(asyncio.coroutine(lambda: None)()); import sqlite3; c = sqlite3.connect('data/nebena.db'); print(c.execute('SELECT COUNT(*) FROM listings').fetchone())"
```

- [ ] **Step 8: Manual integration test — sender**

Choose `[2] Run Sender`. Verify message appears in nebenan.de inbox on the test account.

- [ ] **Step 9: Final commit**

```bash
git add src/parser.py src/sender.py tests/test_parser.py
git commit -m "fix: update DOM selectors to match real nebenan.de structure"
```

---

## Self-Review

**Spec coverage check:**
- ✅ Parse new listings last 24h — `parse_listings()` with `hours` param + `_is_within_hours()`
- ✅ All Germany — URL param `sort=newest` (no location filter); pagination until cutoff
- ✅ Dedup by seller via SQLite — `is_seller_new()` + `upsert_listing()`
- ✅ Multi-account via cookies pool — `AccountPool` + `asyncio.Semaphore`
- ✅ Shablon.txt random template — `TemplateLoader.get_random()`
- ✅ First-message sender — `send_messages()` in `sender.py`
- ✅ CLI menu 1–6 — `show_menu()` in `cli.py`
- ✅ Progress bar — `make_progress()` via `rich.progress`
- ✅ File + console logging — `setup_logging()` with RotatingFileHandler + RichHandler
- ✅ Error handling: bad cookie skip, timeout skip, send failure → no mark — all in place

**Gap identified:** The marketplace URL `https://nebenan.de/marketplace?sort=newest` is assumed — verify this is the correct URL and query param during Task 9 selector discovery.
