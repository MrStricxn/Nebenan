# NEbena — Parser + Sender Design

**Date:** 2026-06-29  
**Site:** nebenan.de (German neighbourhood marketplace)  
**Scope:** Phase 1 — Parser + first-message Sender

---

## Overview

Automated tool that:
1. Parses new marketplace listings on nebenan.de (last 24h, all Germany)
2. Deduplicates by seller via SQLite
3. Sends first-greeting message to each new seller using a random template from `Shablon.txt`
4. Supports multi-account concurrency via cookies pool

---

## Project Structure

```
NEbena/
├── cookies/              # account1.json, account2.json, ...
├── Shablon.txt           # message templates separated by ---
├── data/
│   └── nebena.db         # SQLite database
├── logs/                 # rotating log files
├── src/
│   ├── parser.py         # Playwright + BeautifulSoup, listing collection
│   ├── sender.py         # first-message sending via Playwright
│   ├── db.py             # SQLite wrapper (sellers, listings, sent_messages)
│   ├── accounts.py       # cookie file loader, account pool management
│   ├── templates.py      # Shablon.txt reader, random template selector
│   └── cli.py            # Rich-based CLI menu, logging, progress bar
└── main.py               # entry point
```

---

## Components

### accounts.py
- Scans `cookies/` directory for `*.json` files
- Loads each as a named account with its cookie dict
- Exposes an async account pool — accounts are checked out per worker and returned after use
- Format: Playwright `storage_state` JSON (standard export format)

### parser.py
- Uses Playwright async API — one browser context per account
- Navigates to nebenan.de marketplace, all Germany, sorted by newest
- Scrolls / paginates until listings older than 24h are hit
- Extracts per listing: `listing_id`, `seller_id`, `seller_name`, `title`, `price`, `url`, `published_at`
- Passes raw HTML chunks to BeautifulSoup for field extraction
- Returns list of raw listing dicts to the coordinator in `main.py`

### db.py
Schema:
```sql
CREATE TABLE sellers (
    seller_id TEXT PRIMARY KEY,
    seller_name TEXT,
    first_seen_at TEXT,
    message_sent INTEGER DEFAULT 0
);

CREATE TABLE listings (
    listing_id TEXT PRIMARY KEY,
    seller_id TEXT,
    title TEXT,
    url TEXT,
    published_at TEXT,
    parsed_at TEXT
);
```
- `upsert_listing()` — insert or ignore listing
- `is_seller_new(seller_id)` → bool (True if not in sellers table)
- `mark_seller_contacted(seller_id)` — sets `message_sent = 1`

### templates.py
- Reads `Shablon.txt`, splits on `---` separator
- Strips whitespace from each template
- `get_random_template()` → returns one template string at random

### sender.py
- Receives list of new sellers with their listing URLs
- For each seller: opens listing page via Playwright (reuses account context), finds message button, fills template text, submits
- Marks seller as contacted in DB after successful send
- Skips sellers already marked `message_sent = 1`

### cli.py (Rich)
- Main menu:
  - `[1] Run Parser` — parse new listings, show progress bar
  - `[2] Run Sender` — send messages to new sellers
  - `[3] Run Both` — parser then sender in sequence
  - `[4] Settings` — configure: hours lookback (default 24), max accounts, delay between messages
  - `[5] Statistics` — total sellers found, messaged, listings parsed
  - `[6] Exit`
- Live progress bar via `rich.progress` during parse/send
- Rotating log file at `logs/nebena.log` + console output via `rich.logging`

### main.py
- Parses CLI args (or shows menu if no args)
- Orchestrates: load accounts → run parser workers → deduplicate via DB → run sender workers
- Concurrency: `asyncio` event loop, N parallel Playwright contexts (one per account), controlled via semaphore

---

## Data Flow

```
cookies/*.json
    └─► accounts.py (pool of N accounts)
            └─► parser.py workers (asyncio, 1 context/account)
                    └─► raw listings
                            └─► db.py (dedup by seller_id)
                                    └─► new sellers list
                                            └─► sender.py (Playwright, template from Shablon.txt)
                                                    └─► db.py mark_contacted
```

---

## Shablon.txt Format

```
Hallo! Ich interessiere mich für Ihr Angebot...
---
Guten Tag! Ich habe Ihr Inserat gesehen...
---
Hi, ist der Artikel noch verfügbar?
```

---

## Concurrency Model

- Single `asyncio` event loop
- `asyncio.Semaphore(N)` where N = number of loaded cookie files
- Each worker acquires a semaphore slot + checks out an account from the pool
- Playwright `async_playwright` — non-blocking browser operations
- No threading — pure async avoids GIL and browser conflicts

---

## Error Handling

- Cookie file invalid / expired → skip account, log warning, continue with remaining
- Listing page load timeout → skip listing, log error
- Message send failure → log error, do NOT mark seller as contacted (retry on next run)
- DB errors → fatal, log and exit with message

---

## Dependencies

```
playwright
beautifulsoup4
rich
aiosqlite
```

---

## Phase 2 (out of scope now)

- Second message after seller reply (webhook / polling inbox)
- Proxy support per account
