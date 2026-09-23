#!/usr/bin/env python3
"""The Gamebot demo's data for preferencespace.com (GAMEBOT_PLAN.md).

    uv run --with pyarrow --with pandas python scripts/export_site.py
    # from Gamebot/; writes ../preferencespace/static/survivor/gamebot/data/

Source: gamebot_lite/data/gamebot.sqlite, the packaged read-only slice of the
warehouse (every bronze, silver and gold table; scripts/export_sqlite.py writes
it from Postgres). Nothing here needs the warehouse running.

Writes, for the page:
  <layer>.<table>.parquet   one file per table, read in the browser by DuckDB-WASM
  schema.json               tables by layer with columns, types, descriptions, row counts,
                            and for silver/gold the dbt model SQL and its upstream tables
  castaways.json            the castaway card's picker: id, name, season, result
  SOURCE-LICENSE.txt        survivoR's MIT license, copied from the repo when present
and prints the totals the project page quotes (tables, rows, seasons, castaways, MB).

Layers: bronze is every table dbt/models/sources.yml lists plus the ingestion metadata
tables; silver and gold are dbt/models/<layer>/*.sql. Descriptions come from the dbt
schema.yml files (table and column level) and sources.yml; a column without one gets
none. Lineage is parsed from ref()/source() in the model SQL, so no dbt run is needed.
"""

from __future__ import annotations

import json
import re
import sqlite3
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "gamebot_lite/data/gamebot.sqlite"
MODELS = ROOT / "dbt/models"
OUT = ROOT.parent / "preferencespace/static/survivor/gamebot/data"
META_TABLES = {"ingestion_runs", "dataset_versions", "gamebot_ingestion_metadata"}


def yaml_light(path: Path) -> dict:
    """Table and column descriptions from a dbt schema/sources yml without a yaml
    dependency: `- name:` under models/tables, `description:` lines, `columns:` blocks."""
    out: dict[str, dict] = {}
    table = None
    in_cols = False
    for raw in path.read_text().splitlines():
        line = raw.rstrip()
        s = line.strip()
        ind = len(line) - len(line.lstrip())
        m = re.match(r"-\s*name:\s*(.+)", s)
        if m:
            name = m.group(1).strip().strip("'\"")
            if ind <= 6 and not in_cols or (ind <= 4):
                table = name
                out.setdefault(table, {"description": "", "columns": {}})
                in_cols = False
            elif table and in_cols:
                out[table]["columns"][name] = ""
                last_col = name
            continue
        if s.startswith("columns:"):
            in_cols = True
            continue
        m = re.match(r"description:\s*(.+)", s)
        if m and table:
            text = m.group(1).strip().strip("'\"")
            if in_cols and out[table]["columns"]:
                out[table]["columns"][last_col] = text
            elif not in_cols:
                out[table]["description"] = text
    return out


def main():
    if not DB.exists():
        sys.exit(f"{DB} missing; run scripts/export_sqlite.py --package first")
    OUT.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(DB)
    tables = [
        r[0]
        for r in con.execute(
            "select name from sqlite_master where type='table' order by name"
        )
    ]
    silver = {p.stem: p for p in (MODELS / "silver").glob("*.sql")}
    gold = {p.stem: p for p in (MODELS / "gold").glob("*.sql")}
    desc = {}
    for y in [
        MODELS / "sources.yml",
        MODELS / "silver/schema.yml",
        MODELS / "gold/schema.yml",
    ]:
        if y.exists():
            desc.update(yaml_light(y))

    def layer_of(t):
        return "gold" if t in gold else "silver" if t in silver else "bronze"

    schema = {"tables": [], "layers": ["bronze", "silver", "gold"]}
    total_rows = 0
    total_bytes = 0
    for t in tables:
        layer = layer_of(t)
        cols = con.execute(f'pragma table_info("{t}")').fetchall()
        rows = con.execute(f'select * from "{t}"').fetchall()
        names = [c[1] for c in cols]
        arrays = []
        for j, c in enumerate(cols):
            vals = [r[j] for r in rows]
            typ = (c[2] or "").upper()
            try:
                if "INT" in typ:
                    arrays.append(pa.array(vals, pa.int64()))
                elif any(k in typ for k in ("REAL", "FLOA", "DOUB", "NUM", "DEC")):
                    arrays.append(
                        pa.array(
                            [None if v is None else float(v) for v in vals],
                            pa.float64(),
                        )
                    )
                elif "BOOL" in typ:
                    arrays.append(
                        pa.array(
                            [None if v is None else bool(v) for v in vals], pa.bool_()
                        )
                    )
                else:
                    arrays.append(
                        pa.array(
                            [None if v is None else str(v) for v in vals], pa.string()
                        )
                    )
            except (pa.ArrowInvalid, pa.ArrowTypeError, ValueError):
                arrays.append(
                    pa.array([None if v is None else str(v) for v in vals], pa.string())
                )
        table = pa.Table.from_arrays(arrays, names=names)
        f = OUT / f"{layer}.{t}.parquet"
        pq.write_table(table, f, compression="zstd")
        total_rows += len(rows)
        total_bytes += f.stat().st_size
        d = desc.get(t, {"description": "", "columns": {}})
        entry = {
            "name": t,
            "layer": layer,
            "rows": len(rows),
            "bytes": f.stat().st_size,
            "file": f.name,
            "description": d["description"],
            "meta": t in META_TABLES,
            "columns": [
                {
                    "name": c[1],
                    "type": str(table.schema.field(c[1]).type),
                    "description": d["columns"].get(c[1], ""),
                }
                for c in cols
            ],
        }
        src = silver.get(t) or gold.get(t)
        if src:
            sql = src.read_text()
            entry["sql"] = sql
            entry["upstream"] = sorted(
                set(re.findall(r"ref\(\s*['\"](\w+)['\"]\s*\)", sql))
                | set(
                    re.findall(
                        r"source\(\s*['\"]\w+['\"]\s*,\s*['\"](\w+)['\"]\s*\)", sql
                    )
                )
            )
        else:
            entry["upstream"] = []
        schema["tables"].append(entry)
        print(
            f"  {layer:6s} {t:28s} {len(rows):7,d} rows {len(cols):4d} cols {f.stat().st_size / 1e6:5.2f} MB"
        )

    # the castaway picker: one entry per castaway-season the gold table has
    picker = con.execute("""
        select h.castaway_id, h.version_season, coalesce(d.full_name, c.full_name), s.season_name, c.result, c.place, c.castaway
        from (select distinct castaway_id, version_season from ml_features_hybrid) h
        join castaways c on c.castaway_id = h.castaway_id and c.version_season = h.version_season
        left join castaway_details d on d.castaway_id = h.castaway_id
        left join season_summary s on s.version_season = h.version_season
        group by h.castaway_id, h.version_season
        order by s.version_season, c.place""").fetchall()
    (OUT / "castaways.json").write_text(
        json.dumps(
            [
                {
                    "id": r[0],
                    "season": r[1],
                    "name": r[2],
                    "season_name": r[3],
                    "result": r[4],
                    "place": r[5],
                    # the name as shown that season (Boston Rob); name is the canonical one from castaway_details
                    "short": r[6],
                }
                for r in picker
            ],
            ensure_ascii=False,
        )
    )
    n_seasons = con.execute(
        "select count(distinct version_season) from castaways"
    ).fetchone()[0]
    n_us = con.execute(
        "select count(distinct version_season) from castaways where version = 'US'"
    ).fetchone()[0]
    n_cast = con.execute(
        "select count(distinct castaway_id) from castaways"
    ).fetchone()[0]
    gold_rows = con.execute("select count(*) from ml_features_hybrid").fetchone()[0]
    gold_keys = con.execute(
        "select count(*) from (select distinct castaway_id, version_season from ml_features_hybrid)"
    ).fetchone()[0]
    versions = [
        r[0] for r in con.execute("select distinct version from castaways order by 1")
    ]
    schema["totals"] = {
        "tables": len(tables),
        "bronze": sum(1 for t in tables if layer_of(t) == "bronze"),
        "silver": len(silver),
        "gold": len(gold),
        "rows": total_rows,
        "seasons": n_seasons,
        "us_seasons": n_us,
        "versions": versions,
        "castaways": n_cast,
        "castaway_seasons": len(picker),
        "gold_rows": gold_rows,
        "gold_keys": gold_keys,
        "bytes": total_bytes,
        "exported": json.loads((DB.parent / "manifest.json").read_text()).get(
            "exported_at"
        )
        if (DB.parent / "manifest.json").exists()
        else None,
    }
    (OUT / "schema.json").write_text(json.dumps(schema, ensure_ascii=False))
    # survivoR's license (the data's), which the site already serves beside Who Goes Home; Gamebot's own LICENSE is the code's
    lic = next(
        (
            p
            for p in [
                OUT.parent.parent / "who-goes-home/SOURCE-LICENSE.txt",
                ROOT / "docs/SOURCE-LICENSE.txt",
            ]
            if p.exists()
        ),
        None,
    )
    if lic:
        (OUT / "SOURCE-LICENSE.txt").write_text(lic.read_text())
    t = schema["totals"]
    print(
        f"wrote {OUT}: {t['tables']} tables ({t['bronze']} bronze, {t['silver']} silver, {t['gold']} gold), {t['rows']:,} rows, "
        f"{t['seasons']} seasons ({t['us_seasons']} US; versions {', '.join(versions)}), {t['castaways']:,} castaways, "
        f"{t['castaway_seasons']:,} castaway-seasons in the gold matrix ({t['gold_rows']:,} rows: {'one per key' if gold_rows == gold_keys else 'REPEATED KEYS, see GAMEBOT_PLAN.md'}), "
        f"{total_bytes / 1e6:.1f} MB of Parquet"
    )


if __name__ == "__main__":
    main()
