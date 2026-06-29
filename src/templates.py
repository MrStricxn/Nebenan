from pathlib import Path


class TemplateLoader:
    def __init__(self, path: str = "Shablon.txt"):
        self._path = Path(path)
        self._templates: list[str] = []
        self._index: int = 0

    def load(self) -> None:
        if not self._path.exists():
            raise FileNotFoundError(f"Templates file not found: {self._path}")
        raw = self._path.read_text(encoding="utf-8")
        self._templates = [t.strip() for t in raw.split("---") if t.strip()]
        self._index = 0

    def get_random(self) -> str:
        """Returns templates in round-robin order so each send gets a different text."""
        if not self._templates:
            return ""
        template = self._templates[self._index % len(self._templates)]
        self._index += 1
        return template

    @property
    def count(self) -> int:
        return len(self._templates)
