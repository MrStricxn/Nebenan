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
