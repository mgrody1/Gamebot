from __future__ import annotations

from . import DEFAULT_SQLITE_PATH, get_default_client


def main() -> None:
    client = get_default_client()
    tables = client.list_tables()
    print("Gamebot Lite")
    print(f"SQLite file: {client.sqlite_path}")
    print("Available tables:")
    for tbl in tables:
        print(f"  - {tbl}")


if __name__ == "__main__":
    main()
