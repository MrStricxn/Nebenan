import pytest
import os
from src.templates import TemplateLoader


def test_load_three_templates(tmp_path):
    f = tmp_path / "Shablon.txt"
    f.write_text("Hello A\n---\nHello B\n---\nHello C", encoding="utf-8")
    loader = TemplateLoader(str(f))
    loader.load()
    assert loader.count == 3


def test_get_random_returns_string(tmp_path):
    f = tmp_path / "Shablon.txt"
    f.write_text("Template one\n---\nTemplate two", encoding="utf-8")
    loader = TemplateLoader(str(f))
    loader.load()
    result = loader.get_random()
    assert result in ("Template one", "Template two")


def test_strips_whitespace(tmp_path):
    f = tmp_path / "Shablon.txt"
    f.write_text("  Hello  \n---\n  World  ", encoding="utf-8")
    loader = TemplateLoader(str(f))
    loader.load()
    assert loader.count == 2
    result = loader.get_random()
    assert result in ("Hello", "World")


def test_empty_sections_ignored(tmp_path):
    f = tmp_path / "Shablon.txt"
    f.write_text("Hello\n---\n\n---\nWorld", encoding="utf-8")
    loader = TemplateLoader(str(f))
    loader.load()
    assert loader.count == 2


def test_file_not_found_raises():
    loader = TemplateLoader("nonexistent_file.txt")
    with pytest.raises(FileNotFoundError):
        loader.load()
