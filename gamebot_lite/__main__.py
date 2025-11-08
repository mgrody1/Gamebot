from __future__ import annotations

import logging

from . import get_default_client

logger = logging.getLogger(__name__)


def main() -> None:
    client = get_default_client()
    tables = client.list_tables()
    logger.info("Gamebot Lite")
    logger.info("SQLite file: %s", client.sqlite_path)
    logger.info("Available tables:")
    for tbl in tables:
        logger.info("  - %s", tbl)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
