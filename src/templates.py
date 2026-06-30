import random
from pathlib import Path


class TemplateLoader:
    def __init__(self, path: str = "Shablon.txt"):
        self._path = Path(path)
        self._templates: list[str] = []
        self._queue: list[str] = []

    def load(self) -> None:
        if not self._path.exists():
            raise FileNotFoundError(f"Templates file not found: {self._path}")
        raw = self._path.read_text(encoding="utf-8")
        self._templates = [t.strip() for t in raw.split("---") if t.strip()]
        self._queue = []

    def get_random(self) -> str:
        # Shuffle-then-drain: use each template once in random order before repeating
        if not self._queue:
            self._queue = self._templates[:]
            random.shuffle(self._queue)
        return self._queue.pop()

    @property
    def count(self) -> int:
        return len(self._templates)
