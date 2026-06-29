import asyncio
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, BrowserContext


# UPDATE THESE SELECTORS after inspecting nebenan.de DOM (Task 9)
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
                    href = next_link.get("href", "")
                    if href:
                        url = BASE_URL + href
                    else:
                        break
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
