#!/usr/bin/env python3
"""Generate a warehouse ERD using eralchemy2."""

import logging
from pathlib import Path

from eralchemy2 import render_er

import params

logger = logging.getLogger(__name__)


def main() -> None:
    output_dir = Path("docs/erd")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "warehouse.png"

    conn_str = f"postgresql+psycopg2://{params.db_user}:{params.db_pass}@{params.db_host}:{params.port}/{params.db_name}"

    render_er(conn_str, str(output_path))
    logger.info("ERD written to %s", output_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
