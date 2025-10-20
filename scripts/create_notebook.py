#!/usr/bin/env python3
"""Generate pre-populated Jupyter notebooks for Gamebot."""

import argparse
import json
from datetime import datetime
from pathlib import Path

NOTEBOOK_DIR = Path("notebooks")
TEMPLATES_DIR = Path("templates")


def _ensure_dirs() -> None:
    NOTEBOOK_DIR.mkdir(parents=True, exist_ok=True)


def _load_template(template_name: str) -> dict:
    template_path = TEMPLATES_DIR / f"{template_name}.ipynb"
    if not template_path.exists():
        raise FileNotFoundError(f"Template {template_path} not found")
    return json.loads(template_path.read_text())


def _write_notebook(template: dict, output_name: str) -> Path:
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_path = NOTEBOOK_DIR / f"{output_name}_{timestamp}.ipynb"
    output_path.write_text(json.dumps(template, indent=1))
    return output_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Create starter notebooks")
    parser.add_argument(
        "kind",
        choices=["adhoc", "model"],
        help="Type of notebook to generate",
    )
    args = parser.parse_args()

    _ensure_dirs()
    if args.kind == "adhoc":
        template = _load_template("adhoc_analysis")
        path = _write_notebook(template, "adhoc_analysis")
    else:
        template = _load_template("model_prototyping")
        path = _write_notebook(template, "model_prototyping")

    print(f"Notebook created at {path}")


if __name__ == "__main__":
    main()
