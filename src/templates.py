import random
import threading
from pathlib import Path


class TemplateLoader:
    def __init__(self, path: str = "Shablon.txt"):
        self._path = Path(path)
        self._templates: list[str] = []
        self._queue: list[str] = []
        self._lock = threading.RLock()

    def load(self) -> None:
        if not self._path.exists():
            raise FileNotFoundError(f"Templates file not found: {self._path}")
        raw = self._path.read_text(encoding="utf-8")
        self._templates = [t.strip() for t in raw.split("---") if t.strip()]
        self._queue = []

    def get_random(self) -> str:
        # Shuffle-then-drain: use each template once in random order before repeating
        with self._lock:
            if not self._queue:
                if not self._templates:
                    raise RuntimeError("no templates loaded")
                self._queue = self._templates[:]
                random.shuffle(self._queue)
            return self._queue.pop()

    def all(self) -> list[str]:
        return self._templates[:]

    def _reset_queue(self) -> None:
        self._queue = []

    def save(self) -> None:
        """Atomically rewrite the templates file (temp + rename)."""
        tmp = self._path.with_suffix(self._path.suffix + ".tmp")
        tmp.write_text("\n---\n".join(self._templates) + ("\n" if self._templates else ""),
                       encoding="utf-8")
        tmp.replace(self._path)

    def add(self, text: str) -> int:
        with self._lock:
            self._templates.append(text)
            self._reset_queue()
            self.save()
            return len(self._templates) - 1

    def update(self, index: int, text: str) -> None:
        with self._lock:
            if not 0 <= index < len(self._templates):
                raise IndexError("template index out of range")
            self._templates[index] = text
            self._reset_queue()
            self.save()

    def delete(self, index: int) -> None:
        with self._lock:
            if not 0 <= index < len(self._templates):
                raise IndexError("template index out of range")
            del self._templates[index]
            self._reset_queue()
            self.save()

    @property
    def count(self) -> int:
        return len(self._templates)
