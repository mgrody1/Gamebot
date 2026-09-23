"""Regression tests for the pipeline (loader, cache, export) failure handling.

These need the pipeline dependencies, so they skip when only gamebot-lite is
installed (the publish workflows run pytest that way).
"""

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path
from unittest import mock

import pytest

pytest.importorskip("psycopg2")
pytest.importorskip("sqlalchemy")
pytest.importorskip("pyreadr")
pytest.importorskip("dotenv")

import pandas as pd  # noqa: E402

from Database import load_survivor_data  # noqa: E402
from gamebot_core import db_utils, env, github_data_loader, validation  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_script(name: str):
    spec = importlib.util.spec_from_file_location(
        f"_test_{name}", REPO_ROOT / "scripts" / f"{name}.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_create_sql_engine_keeps_special_character_password(monkeypatch):
    monkeypatch.setattr(db_utils.params, "db_user", "survivor_dev")
    monkeypatch.setattr(db_utils.params, "db_pass", "p@ss/w0rd%?")
    monkeypatch.setattr(db_utils.params, "db_host", "localhost")
    monkeypatch.setattr(db_utils.params, "port", "55433")
    monkeypatch.setattr(db_utils.params, "db_name", "survivor_dw_dev")

    url = db_utils.create_sql_engine().url

    assert url.password == "p@ss/w0rd%?"
    assert (url.host, url.port, url.database) == ("localhost", 55433, "survivor_dw_dev")


def _fake_response(content: bytes):
    return mock.Mock(
        status_code=200,
        headers={"Content-Type": "application/octet-stream"},
        content=content,
    )


def test_force_refresh_replaces_stale_cache(monkeypatch, tmp_path):
    monkeypatch.setattr(github_data_loader, "CACHE_DIR", tmp_path)
    (tmp_path / "castaways.rda").write_bytes(b"RDX3 stale")
    get = mock.Mock(return_value=_fake_response(b"RDX3 fresh"))
    monkeypatch.setattr(github_data_loader.requests, "get", get)

    path = github_data_loader._download_rda("castaways", "https://example.test/data")
    assert path.read_bytes() == b"RDX3 stale"
    get.assert_not_called()

    github_data_loader._download_rda(
        "castaways", "https://example.test/data", force_refresh=True
    )
    assert path.read_bytes() == b"RDX3 fresh"
    assert list(tmp_path.iterdir()) == [path]


def test_interrupted_cache_write_leaves_no_file(monkeypatch, tmp_path):
    monkeypatch.setattr(github_data_loader, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(
        github_data_loader.requests,
        "get",
        mock.Mock(return_value=_fake_response(b"RDX3 payload")),
    )
    monkeypatch.setattr(
        github_data_loader.os, "replace", mock.Mock(side_effect=KeyboardInterrupt)
    )

    with pytest.raises(KeyboardInterrupt):
        github_data_loader._download_rda("castaways", "https://example.test/data")

    assert list(tmp_path.iterdir()) == []


def test_uniqueness_failure_still_writes_report(monkeypatch, tmp_path):
    monkeypatch.setattr(validation, "CURRENT_VALIDATION_SUBDIR", tmp_path)
    monkeypatch.setattr(validation, "VALIDATION_SUMMARIES", {})
    monkeypatch.setattr(validation, "REFERENCE_CACHE", {})
    df = pd.DataFrame({"castaway_id": ["US0001", "US0001"]})

    with pytest.raises(ValueError):
        validation.validate_bronze_dataset("castaway_details", df)

    (report,) = tmp_path.glob("validation_castaway_details_*.json")
    result = json.loads(report.read_text())
    assert result["status"] == "failed"
    assert result["unique_constraint"]["status"] == "failed"
    assert result["unique_constraint"]["duplicate_sample"]


def test_loader_raises_when_database_unreachable(monkeypatch):
    monkeypatch.setattr(load_survivor_data, "connect_to_db", lambda: None)

    with pytest.raises(RuntimeError, match="Database connection failed"):
        load_survivor_data.main()


def _patch_loader(monkeypatch, conn, load_side_effect=None):
    mocks = {
        "connect_to_db": mock.Mock(return_value=conn),
        "run_schema_sql": mock.Mock(),
        "register_ingestion_run": mock.Mock(return_value="run-1"),
        "set_validation_run": mock.Mock(),
        "get_unique_constraint_cols_from_table_name": mock.Mock(return_value=[]),
        "load_dataset_to_table": mock.Mock(side_effect=load_side_effect),
        "finalise_validation_reports": mock.Mock(return_value=None),
        "finalize_ingestion_run": mock.Mock(),
        "current_git_branch": mock.Mock(return_value=None),
        "current_git_commit": mock.Mock(return_value=None),
    }
    for name, value in mocks.items():
        monkeypatch.setattr(load_survivor_data, name, value)
    return mocks


def test_loader_failure_marks_run_failed_and_closes(monkeypatch):
    conn = mock.MagicMock()
    mocks = _patch_loader(monkeypatch, conn, load_side_effect=ValueError("boom"))

    with pytest.raises(ValueError, match="boom"):
        load_survivor_data.main()

    mocks["finalize_ingestion_run"].assert_called_once_with(conn, "run-1", "failed")
    mocks["finalise_validation_reports"].assert_called_once_with(run_identifier="run-1")
    conn.rollback.assert_called_once()
    conn.close.assert_called_once()


def test_loader_passes_force_refresh(monkeypatch):
    conn = mock.MagicMock()
    mocks = _patch_loader(monkeypatch, conn)

    assert load_survivor_data.main(force_refresh=True) == "run-1"

    calls = mocks["load_dataset_to_table"].call_args_list
    assert calls and all(c.kwargs["force_refresh"] is True for c in calls)
    mocks["finalize_ingestion_run"].assert_called_once_with(conn, "run-1", "succeeded")
    conn.close.assert_called_once()


@pytest.mark.parametrize(
    "branch, allowed",
    [("main", True), ("release/1.0", True), ("data-release/x", True), ("dev", False)],
)
def test_require_prod_on_main(monkeypatch, branch, allowed):
    monkeypatch.delenv("GAMEBOT_CONTAINER_DEPLOYMENT", raising=False)
    monkeypatch.setattr(env, "current_git_branch", lambda: branch)

    if allowed:
        env.require_prod_on_main("prod")
    else:
        with pytest.raises(RuntimeError):
            env.require_prod_on_main("prod")


def _write_sqlite(path: Path, table: str) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(f"CREATE TABLE {table} (x INTEGER)")


def _tables(path: Path):
    with sqlite3.connect(path) as conn:
        return {r[0] for r in conn.execute("SELECT name FROM sqlite_master")}


def test_export_replaces_existing_file(monkeypatch, tmp_path):
    export = _load_script("export_sqlite")
    output = tmp_path / "gamebot.sqlite"
    _write_sqlite(output, "dropped_upstream")

    def fake_export(layer, path):
        _write_sqlite(path, "fresh")
        return pd.DataFrame(), ["fresh"]

    monkeypatch.setattr(export, "_export_to_file", fake_export)
    export.export_sqlite("gold", output)
    assert _tables(output) == {"fresh"}

    monkeypatch.setattr(export, "_export_to_file", mock.Mock(side_effect=RuntimeError))
    with pytest.raises(RuntimeError):
        export.export_sqlite("gold", output)
    assert _tables(output) == {"fresh"}
    assert list(tmp_path.iterdir()) == [output]


def _run_export_main(monkeypatch, tmp_path, output: str):
    export = _load_script("export_sqlite")
    monkeypatch.chdir(tmp_path)

    def fake_export(layer, path):
        _write_sqlite(path, "fresh")
        return pd.DataFrame(), ["fresh"]

    monkeypatch.setattr(export, "export_sqlite", fake_export)
    monkeypatch.setattr(
        sys,
        "argv",
        ["export_sqlite.py", "--layer", "gold", "--package", "--output", output],
    )
    return export


def test_export_package_output_same_as_package_path(monkeypatch, tmp_path):
    export = _run_export_main(monkeypatch, tmp_path, "gamebot_lite/data/gamebot.sqlite")

    export.main()

    manifest = json.loads((tmp_path / "gamebot_lite/data/manifest.json").read_text())
    assert manifest["sqlite_filename"] == "gamebot.sqlite"


def test_export_package_permission_error_exits_non_zero(monkeypatch, tmp_path):
    export = _run_export_main(monkeypatch, tmp_path, "out/gamebot.sqlite")
    monkeypatch.setattr("shutil.copy2", mock.Mock(side_effect=PermissionError))

    with pytest.raises(SystemExit) as excinfo:
        export.main()
    assert excinfo.value.code == 1


def test_env_helper_check_masks_connection_string(monkeypatch, capsys):
    env_helper = _load_script("env_helper")
    monkeypatch.setenv("DB_PASSWORD", "p@ss/w0rd%")
    monkeypatch.delenv("DOCKER_CONTAINER", raising=False)
    monkeypatch.delenv("IS_DEPLOYED", raising=False)
    monkeypatch.setattr("psycopg2.connect", mock.Mock(side_effect=Exception("offline")))

    env_helper.check_environment()

    out = capsys.readouterr().out
    assert "CONNECTION_STRING" in out
    assert "p@ss/w0rd%" not in out
