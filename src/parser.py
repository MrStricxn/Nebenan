import asyncio
import logging
from datetime import datetime, timezone, timedelta
from playwright.async_api import async_playwright

log = logging.getLogger("nebena")

_API_POSTS = "https://api.nebenan.de/api/core/v3/marketplace/posts"
BASE_URL   = "https://nebenan.de"


def _get_auth_token(storage_state: dict) -> str | None:
    for c in storage_state.get("cookies", []):
        if c.get("name") == "s" and "nebenan" in c.get("domain", ""):
            return c["value"]
    return None


def _is_within_hours(published_at: str, hours: int) -> bool:
    if not published_at:
        return False
    try:
        dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        return dt >= cutoff
    except ValueError:
        return False


def _extract_from_api_page(page_items: list[dict]) -> list[dict]:
    results = []
    for item in page_items:
        try:
            post = item["post"]
            ad   = post["author_details"]
            gid  = ad["associated_gid"]
            seller_id = gid.split("/")[-1]

            md    = post.get("marketplace_details") or {}
            cat   = md.get("category") or {}
            price_cents = md.get("price_in_cents")
            price = f"{price_cents / 100:.0f} €" if price_cents else ""

            # URL the seller profile (always valid, unlike post URL)
            profile_url = ad.get("profile_url") or f"{BASE_URL}/profile/{seller_id}"
            msg_url     = ad.get("private_message_url") or f"{BASE_URL}/messages/{seller_id}"

            results.append({
                "listing_id":   str(post["id"]),
                "seller_id":    seller_id,
                "seller_name":  ad["name"],
                "title":        post["subject"],
                "price":        price,
                "category":     cat.get("title", ""),
                "url":          profile_url,   # seller profile — always reachable
                "published_at": post["created_at"],
                "message_url":  msg_url,
            })
        except (KeyError, TypeError):
            continue
    return results


async def _fetch_account_listings(
    account_name: str,
    storage_state: dict,
    hours: int,
    semaphore: asyncio.Semaphore,
    progress=None,
) -> list[dict]:
    auth_token = _get_auth_token(storage_state)
    if not auth_token:
        log.warning(f"[{account_name}] нет auth-токена — пропуск")
        return []

    headers = {
        "x-auth-token":   auth_token,
        "accept":         "application/json",
        "accept-language": "de",
        "user-agent":     "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }

    log.info(f"[{account_name}] парсинг объявлений...")
    listings: list[dict] = []

    async with semaphore:
        async with async_playwright() as pw:
            request_ctx = await pw.request.new_context(extra_http_headers=headers)
            after    = None
            stop     = False
            page_num = 0

            while not stop:
                url = f"{_API_POSTS}?categories=&limit=24"
                if after:
                    url += f"&after={after}"

                response = await request_ctx.get(url)
                if response.status != 200:
                    log.warning(f"[{account_name}] API вернул {response.status}")
                    break

                data       = await response.json()
                page_items = data.get("page", [])
                page_info  = data.get("page_info", {})
                page_num  += 1

                if not page_items:
                    break

                for lst in _extract_from_api_page(page_items):
                    if _is_within_hours(lst["published_at"], hours):
                        listings.append(lst)
                        if progress:
                            progress.advance(progress.task_ids[0])
                    else:
                        stop = True
                        break

                if stop or not page_info.get("has_next_page"):
                    break

                after = page_info.get("end_cursor")
                if not after:
                    break

            await request_ctx.dispose()

    log.info(f"[{account_name}] найдено {len(listings)} объявлений ({page_num} стр.)")
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
            _fetch_account_listings(name, state, hours, semaphore, progress)
        )
        tasks.append((name, task))

    results = await asyncio.gather(*[t for _, t in tasks], return_exceptions=True)
    for name, _ in tasks:
        await account_pool.release(name)

    all_listings: list[dict] = []
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            name = tasks[i][0]
            log.error(f"[{name}] ошибка парсинга: {result}")
        elif isinstance(result, list):
            all_listings.extend(result)

    seen:   set[str]    = set()
    unique: list[dict]  = []
    for lst in all_listings:
        if lst["listing_id"] not in seen:
            seen.add(lst["listing_id"])
            unique.append(lst)

    return unique
