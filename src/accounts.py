import asyncio
import json
import os
from pathlib import Path


def _normalize_storage_state(raw) -> dict:
    """Convert EditThisCookie array format to Playwright storage_state dict."""
    if isinstance(raw, dict):
        return raw
    if not isinstance(raw, list):
        return {"cookies": [], "origins": []}
    cookies = []
    for c in raw:
        same_site = c.get("sameSite", "no_restriction")
        cookie = {
            "name": c["name"],
            "value": c["value"],
            "domain": c["domain"],
            "path": c.get("path", "/"),
            "secure": c.get("secure", False),
            "httpOnly": c.get("httpOnly", False),
            "sameSite": {"no_restriction": "None", "lax": "Lax", "strict": "Strict"}.get(same_site, "None"),
        }
        exp = c.get("expirationDate", 0)
        if exp and exp > 0:
            cookie["expires"] = int(exp)
        cookies.append(cookie)
    return {"cookies": cookies, "origins": []}


class AccountPool:
    def __init__(self, cookies_dir: str = "cookies"):
        self._dir = Path(cookies_dir)
        self._accounts: dict[str, dict] = {}
        self._free: asyncio.Queue = asyncio.Queue()

    async def load(self) -> None:
        if not self._dir.exists():
            return
        for path in self._dir.glob("*.json"):
            if path.stat().st_size == 0:
                continue
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                state = _normalize_storage_state(raw)
                if not state.get("cookies"):
                    continue
                name = path.stem
                self._accounts[name] = state
                await self._free.put(name)
            except (json.JSONDecodeError, OSError, KeyError):
                pass

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
