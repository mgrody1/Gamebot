"""Environment helpers shared across CLI scripts."""

from __future__ import annotations

import subprocess
from typing import Optional


def _run_git(args: list[str]) -> Optional[str]:
    try:
        result = subprocess.run(
            ["git", *args],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def current_git_branch() -> Optional[str]:
    """Return the current git branch or None if it cannot be determined."""

    return _run_git(["rev-parse", "--abbrev-ref", "HEAD"])


def current_git_commit() -> Optional[str]:
    """Return the current git commit SHA or None if it cannot be determined."""

    return _run_git(["rev-parse", "HEAD"])


def require_prod_on_main(environment: str) -> None:
    """
    Enforce that prod-facing scripts only execute from the main branch.

    Many scripts mutate the warehouse or long-lived artefacts. When `SURVIVOR_ENV`
    is set to `prod`, we require the git branch to be `main`. This prevents
    accidental runs from feature branches.
    """

    if environment.lower() != "prod":
        return

    branch = current_git_branch()
    if branch is None:
        raise RuntimeError(
            "Unable to determine git branch while running in prod environment. "
            "Ensure git is available inside the runtime."
        )
    if branch != "main":
        raise RuntimeError(
            f"Prod runs must execute from the 'main' branch (current branch: {branch})."
        )
