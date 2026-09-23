"""Gamebot Lite – lightweight access to Survivor data via SQLite."""

from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Optional

from .client import GamebotClient, duckdb_query, load_table

try:
    __version__ = version("gamebot-lite")
except PackageNotFoundError:  # e.g. imported from a source checkout
    __version__ = "0+unknown"

__all__ = [
    "GamebotClient",
    "load_table",
    "duckdb_query",
    "get_default_client",
    "DEFAULT_SQLITE_PATH",
]


DEFAULT_SQLITE_PATH = Path(__file__).resolve().parent / "data" / "gamebot.sqlite"


def get_default_client(path: Optional[Path] = None) -> GamebotClient:
    """Return a GamebotClient pointing at the packaged SQLite file."""

    sqlite_path = path or DEFAULT_SQLITE_PATH
    return GamebotClient(sqlite_path)
