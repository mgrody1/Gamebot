#!/usr/bin/env python3
"""
Build and publish gamebot-lite package to PyPI.

Usage:
    # Build package
    python scripts/build_package.py

    # Build and upload to TestPyPI
    python scripts/build_package.py --upload-test

    # Build and upload to PyPI (production)
    python scripts/build_package.py --upload-prod

Prerequisites:
    - pip install build twine
    - Configure PyPI API tokens:
      - TestPyPI: python -m twine upload --repository testpypi dist/*
      - PyPI: python -m twine upload dist/*
"""

import argparse
import subprocess
import sys
from pathlib import Path


def run_command(cmd: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run shell command and return result."""
    print(f"Running: {cmd}")
    return subprocess.run(cmd, shell=True, check=check)


def build_package():
    """Build the package using build module."""
    print("Building package...")

    # Clean previous builds
    dist_dir = Path("dist")
    if dist_dir.exists():
        print("Cleaning previous builds...")
        run_command("rm -rf dist/")

    # Build package
    run_command("python -m build")

    print("Package built successfully!")
    print("Files created:")
    for file in sorted(dist_dir.glob("*")):
        print(f"  {file}")


def upload_package(repository: str = "pypi"):
    """Upload package to PyPI or TestPyPI."""
    print(f"Uploading to {repository}...")

    if repository == "testpypi":
        run_command("python -m twine upload --repository testpypi dist/*")
    else:
        run_command("python -m twine upload dist/*")

    print(f"Package uploaded to {repository} successfully!")


def main():
    parser = argparse.ArgumentParser(
        description="Build and publish gamebot-lite package"
    )
    parser.add_argument(
        "--upload-test", action="store_true", help="Upload to TestPyPI after building"
    )
    parser.add_argument(
        "--upload-prod", action="store_true", help="Upload to PyPI after building"
    )

    args = parser.parse_args()

    if args.upload_test and args.upload_prod:
        print("Error: Cannot upload to both TestPyPI and PyPI in same run")
        sys.exit(1)

    # Build package
    build_package()

    # Upload if requested
    if args.upload_test:
        upload_package("testpypi")
    elif args.upload_prod:
        upload_package("pypi")
    else:
        print("\nTo upload package:")
        print("  TestPyPI: python scripts/build_package.py --upload-test")
        print("  PyPI:     python scripts/build_package.py --upload-prod")


if __name__ == "__main__":
    main()
