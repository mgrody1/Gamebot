#!/usr/bin/env python3
"""
Check that Pipfile and airflow/requirements.txt contain compatible package versions.

This script ensures that packages common to both files have compatible versions,
preventing deployment issues where different package versions are specified.
"""

import re
import sys
from pathlib import Path
from typing import Dict


def parse_pipfile_packages(pipfile_path: Path) -> Dict[str, str]:
    """Parse packages from Pipfile [packages] section."""
    packages = {}

    with open(pipfile_path, "r") as f:
        content = f.read()

    # Extract [packages] section - match until the next section starting with [word]
    packages_match = re.search(
        r"\[packages\](.*?)(?=^\[[\w-]+\]|\Z)", content, re.DOTALL | re.MULTILINE
    )
    if not packages_match:
        return packages

    packages_section = packages_match.group(1)

    # Parse each package line
    for line in packages_section.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        # Handle different Pipfile formats:
        # package = "*"
        # package = "==1.2.3"
        # package = "<2.0,>=1.9"
        # package = {version = "==1.2.3", extras = ["postgres"]}
        if "=" in line and not line.startswith("["):  # Avoid section headers
            parts = line.split("=", 1)
            package_name = parts[0].strip()

            # Extract version from various formats
            version_part = parts[1].strip()

            if version_part.startswith("{"):
                # Dictionary format: {version = "==1.2.3", extras = ["postgres"]}
                version_match = re.search(r'version\s*=\s*"([^"]*)"', version_part)
                version = version_match.group(1) if version_match else "*"
            elif version_part.startswith('"') and version_part.endswith('"'):
                # Quoted string version: "==1.2.3" or "*" or "<2.0,>=1.9"
                version = version_part[1:-1]
            else:
                # Unquoted version (shouldn't happen in valid TOML, but let's handle it)
                version = version_part

            packages[package_name] = version

    return packages


def parse_requirements_txt(requirements_path: Path) -> Dict[str, str]:
    """Parse packages from requirements.txt."""
    packages = {}

    with open(requirements_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # Handle package==version or package>=version etc.
            if "==" in line:
                package, version = line.split("==", 1)
                packages[package.strip()] = f"=={version.strip()}"
            elif ">=" in line:
                package, version = line.split(">=", 1)
                packages[package.strip()] = f">={version.strip()}"
            elif "<=" in line:
                package, version = line.split("<=", 1)
                packages[package.strip()] = f"<={version.strip()}"
            else:
                # No version specified
                packages[line] = "*"

    return packages


def normalize_package_name(name: str) -> str:
    """Normalize package names for comparison (handle dashes vs underscores)."""
    return name.lower().replace("-", "_").replace("_", "-")


def check_compatibility(pipfile_version: str, requirements_version: str) -> bool:
    """Check if two version specs are compatible."""
    # If either is "*", they're compatible
    if pipfile_version == "*" or requirements_version == "*":
        return True

    # If both have exact versions, they must match
    if pipfile_version.startswith("==") and requirements_version.startswith("=="):
        return pipfile_version == requirements_version

    # For now, assume other combinations are compatible
    # (Could be enhanced with proper version parsing)
    return True


def main():
    """Main function to check requirements synchronization."""
    repo_root = Path(__file__).parent.parent
    pipfile_path = repo_root / "Pipfile"
    requirements_path = repo_root / "airflow" / "requirements.txt"

    if not pipfile_path.exists():
        print(f"Error: {pipfile_path} not found")
        return 1

    if not requirements_path.exists():
        print(f"Error: {requirements_path} not found")
        return 1

    # Parse both files
    pipfile_packages = parse_pipfile_packages(pipfile_path)
    requirements_packages = parse_requirements_txt(requirements_path)

    # Normalize package names for comparison
    pipfile_normalized = {
        normalize_package_name(k): v for k, v in pipfile_packages.items()
    }
    requirements_normalized = {
        normalize_package_name(k): v for k, v in requirements_packages.items()
    }

    # Find common packages
    common_packages = set(pipfile_normalized.keys()) & set(
        requirements_normalized.keys()
    )

    # Check for conflicts
    conflicts = []
    for package in common_packages:
        pipfile_version = pipfile_normalized[package]
        requirements_version = requirements_normalized[package]

        if not check_compatibility(pipfile_version, requirements_version):
            conflicts.append(
                {
                    "package": package,
                    "pipfile": pipfile_version,
                    "requirements": requirements_version,
                }
            )

    # Report results
    if conflicts:
        print("❌ Requirements synchronization check FAILED")
        print("\nVersion conflicts found:")
        for conflict in conflicts:
            print(f"  {conflict['package']}:")
            print(f"    Pipfile: {conflict['pipfile']}")
            print(f"    requirements.txt: {conflict['requirements']}")

        print(
            f"\nPlease ensure {pipfile_path} and {requirements_path} have compatible versions."
        )
        return 1

    print("✅ Requirements synchronization check PASSED")
    if common_packages:
        print(f"Found {len(common_packages)} common packages with compatible versions")
    else:
        print("No common packages found between Pipfile and requirements.txt")

    return 0


if __name__ == "__main__":
    sys.exit(main())
