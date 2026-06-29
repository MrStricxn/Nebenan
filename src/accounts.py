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
