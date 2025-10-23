# Gamebot Lite

Gamebot Lite is a lightweight snapshot of the Survivor warehouse bundled as a SQLite database. It mirrors the `survivoR` R package experience so analysts can explore the data in python and python notebooks, while also including two additional layers of curated tables.

Looking for the full warehouse documentation? See the [Gamebot Warehouse schema guide](gamebot_warehouse_schema_guide.md) and the [join cheat sheet + IDE tips](gamebot_warehouse_cheatsheet.md).

## Installation

```bash
python -m pip install gamebot-lite

# Optional: include DuckDB support for SQL helpers
python -m pip install "gamebot-lite[duckdb]"
```

The optional `duckdb` extra installs DuckDB so that `gamebot_lite.duckdb_query` works out of the box.

## Layers

- **Bronze (Raw)** – Direct exports of the survivoR tables (e.g., `bronze.castaway_details`, `bronze.season_summary`). These retain their original column names and provide provenance-friendly raw data.
- **Silver (Curated)** – Analytics-ready tables with clearer names (e.g., `silver.castaway_profile`, `silver.challenge_results_curated`). These consolidate joins and add derived columns.
- **Gold (ML Feature)** – Feature-layer snapshots (e.g., `gold.features_castaway_episode`) tailored for modeling and longitudinal analysis.

All three schemas are included in the packaged SQLite file. Use schema-qualified table names (`bronze.foo`, `silver.bar`, `gold.baz`) when querying so it’s clear which layer you’re touching. The `gamebot_ingestion_metadata` table and metadata tables (e.g., `bronze.dataset_versions`) record the upstream commit, environment, and ingestion timestamps so you know how fresh the data is.

## Usage

```python
from gamebot_lite import load_table, duckdb_query

# Pandas example (bronze castaway details)
df_castaways = load_table("castaway_details")
print(df_castaways.head())

# DuckDB SQL example (requires `pip install duckdb`)
sql = """
SELECT original_tribe, COUNT(*) AS castaway_count
FROM castaway_season_profile
GROUP BY original_tribe
ORDER BY castaway_count DESC
"""

df_tribes = duckdb_query(sql)
print(df_tribes)
```

### Available table names

#### Bronze tables (Raw)

| Table | Description |
| --- | --- |
| `advantage_details` | Advantage inventory with type, owner, and metadata. |
| `advantage_movement` | Advantage lifecycle events (found, passed, played). |
| `boot_mapping` | Episode boot mapping to castaways and outcomes. |
| `castaway_details` | Master castaway information (matches survivoR). |
| `castaways` | Castaway-season relationship table. |
| `challenge_description` | Challenge catalog with type, recurring name, and description. |
| `challenge_results` | Raw challenge outcomes (team/individual). |
| `confessionals` | Raw confessional transcripts and metadata. |
| `episodes` | Episode metadata including numbers and air dates. |
| `jury_votes` | Raw jury vote outcomes. |
| `season_summary` | Season-level metadata. |
| `tribe_mapping` | Tribe membership timeline per castaway. |
| `vote_history` | Vote outcomes with round-by-round details. |
| `vote_history_extended` | Extended vote context (revotes, idols, etc.). |
| `gamebot_ingestion_metadata` | Loader run metadata (environment, git details). |

#### Silver tables (Curated)

| Table | Description |
| --- | --- |
| `dim_advantage` | Advantage dimension with canonical attributes. |
| `dim_castaway` | Dimension table with enriched castaway attributes. |
| `dim_challenge` | Challenge dimension with analytics-friendly columns. |
| `dim_episode` | Episode dimension with air dates and numbering. |
| `dim_season` | Season dimension capturing themes, twists, and geography. |
| `fact_advantage_movement` | Advantage lifecycle events (found, passed, played). |
| `fact_boot_mapping` | Mapping of episode boot events to castaways. |
| `fact_challenge_results` | Curated challenge results with keys to dimensions. |
| `fact_confessionals` | Aggregated confessional counts and airtime. |
| `fact_jury_votes` | Jury vote outcomes tied to castaway dimension keys. |
| `fact_tribe_membership` | Tribe membership timeline per castaway. |
| `fact_vote_history` | Curated vote history with consistent castaway keys. |
| `challenge_skill_bridge` | Bridge table connecting challenges to skill types. |
| `challenge_skill_lookup` | Lookup table of challenge skill taxonomy. |
| `bridge_castaway_season` | Bridge table linking castaways to seasons with roles. |

#### Gold tables (ML Feature)

| Table | Description |
| --- | --- |
| `features_castaway_episode` | Episode-level feature JSON for ML experiments. |
| `features_castaway_season` | Season-level feature JSON per castaway. |
| `features_season` | Season-wide feature JSON. |

Planned development: enhanced confessional text tables (ingestion + NLP features) will appear as additional silver models once complete.

### Column name mapping (silver → Gamebot Lite alias)

When exporting to SQLite, several silver tables are aliased with more analyst-friendly names. Use the table below when referencing the underlying warehouse objects:

| Warehouse table (`silver.*`) | Gamebot Lite table |
| --- | --- |
| `dim_castaway` | `castaway_profile` |
| `dim_season` | `season_profile` |
| `dim_episode` | `episode_profile` |
| `dim_advantage` | `advantage_catalog` |
| `dim_challenge` | `challenge_catalog` |
| `challenge_skill_lookup` | `challenge_skill` |
| `challenge_skill_bridge` | `challenge_skill_assignment` |
| `bridge_castaway_season` | `castaway_season_profile` |
| `fact_confessionals` | `confessional_summary` |
| `fact_challenge_results` | `challenge_results_curated` |
| `fact_vote_history` | `vote_history_curated` |
| `fact_advantage_movement` | `advantage_movement_curated` |
| `fact_boot_mapping` | `boot_mapping_curated` |
| `fact_tribe_membership` | `tribe_membership_curated` |
| `fact_jury_votes` | `jury_votes_curated` |

Gold tables retain their names (e.g., `features_castaway_episode`) in the Gamebot Lite export.

## Keeping Data Fresh

**For developers with PiPY API access for the package**
1. Run `pipenv run python scripts/export_sqlite.py --layer silver --package --output gamebot_lite/data/gamebot.sqlite`.
2. Bump `pyproject.toml` version, build (`pipenv run python -m build`), and upload via twine.

**Users of gamebot-lite wanting the most recent available data**
1. Users can simply run `python -m pip install --upgrade gamebot-lite` to fetch the latest snapshot.
