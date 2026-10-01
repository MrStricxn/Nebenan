import asyncio
import logging
from datetime import datetime, timezone, timedelta

from src.http import api_request_ex

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


def _is_free_listing(post: dict, md: dict) -> bool:
    """Zu verschenken / free giveaways — user doesn't want these parsed."""
    if (post.get("content_type") or "").lower() == "marketplace_free":
        return True
    raw_price = md.get("price_in_cents")
    if raw_price is not None:
        try:
            if int(raw_price) == 0:
                return True
        except (TypeError, ValueError):
            pass
    cat = md.get("category") or {}
    title = cat.get("title") if isinstance(cat, dict) else cat
    if "verschenk" in (title or "").lower():
        return True
    return False


def _extract_from_api_page(page_items: list[dict]) -> list[dict]:
    results = []
    skipped_free = 0
    for item in page_items:
        try:
            post = item["post"]
            ad   = post["author_details"]
            gid  = ad["associated_gid"]
            seller_id = gid.split("/")[-1]

            md    = post.get("marketplace_details") or {}
            if _is_free_listing(post, md):
                skipped_free += 1
                continue
            cat   = md.get("category") or {}
            price_cents = md.get("price_in_cents")
            price = f"{price_cents / 100:.0f} €" if price_cents else ""

            listing_url = f"{BASE_URL}/feed/{post['id']}"
            msg_url     = ad.get("private_message_url") or f"{BASE_URL}/messages/{seller_id}"

            results.append({
                "listing_id":   str(post["id"]),
                "seller_id":    seller_id,
                "seller_name":  ad["name"],
                "title":        post["subject"],
                "price":        price,
                "category":     cat.get("title", ""),
                "url":          listing_url,
                "published_at": post.get("created_at", ""),
                "message_url":  msg_url,
            })
        except (KeyError, TypeError):
            continue
    if skipped_free:
        log.info(f"пропущено бесплатных (Zu verschenken): {skipped_free}")
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
        "x-auth-token":    auth_token,
        "accept":          "application/json",
        "accept-language": "de",
        "user-agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }

    log.info(f"[{account_name}] парсинг объявлений...")
    listings: list[dict] = []

    async with semaphore:
        after    = None
        page_num = 0

        while True:
            url = f"{_API_POSTS}?categories=&limit=24"
            if after:
                url += f"&after={after}"

            # Via api_request_ex: timeouts + non-2xx also feed ban detection.
            data, status, _txt = await api_request_ex(
                "GET", url, auth_token, account_name=account_name,
                extra_headers={
                    "accept":          "application/json",
                    "accept-language": "de",
                    "user-agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                })
            if not isinstance(data, dict):
                log.warning(f"[{account_name}] API статус {status} — стоп")
                break
            page_items = data.get("page") or []
            page_info  = data.get("page_info") or {}
            page_num  += 1

            if not page_items:
                log.info(f"[{account_name}] стр.{page_num}: пустая страница")
                break

            # Extract all items, filter by date — NO early break mid-page
            # (promoted/boosted listings can appear out of order)
            page_extracted = _extract_from_api_page(page_items)
            in_window = [l for l in page_extracted if _is_within_hours(l["published_at"], hours)]
            listings.extend(in_window)

            log.info(
                f"[{account_name}] стр.{page_num}: "
                f"{len(page_items)} сырых → {len(page_extracted)} извлечено → "
                f"{len(in_window)} в окне {hours}ч"
            )

            if progress:
                for _ in in_window:
                    progress.advance(progress.task_ids[0])

            # Stop pagination when the oldest item on this page is outside the window
            # (listings can be out of order → use min over the whole page)
            dates = [l["published_at"] for l in page_extracted if l["published_at"]]
            if dates and not _is_within_hours(min(dates), hours):
                break

            if not page_info.get("has_next_page"):
                break

            after = page_info.get("end_cursor")
            if not after:
                break

    log.info(f"[{account_name}] итого {len(listings)} объявлений ({page_num} стр.)")
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
            log.error(f"[{tasks[i][0]}] ошибка: {result}")
        elif isinstance(result, list):
            all_listings.extend(result)

    seen:   set[str]   = set()
    unique: list[dict] = []
    for lst in all_listings:
        if lst["listing_id"] not in seen:
            seen.add(lst["listing_id"])
            unique.append(lst)

    return unique
