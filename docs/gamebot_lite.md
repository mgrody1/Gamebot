# Gamebot Lite

Gamebot Lite is a lightweight snapshot of the Survivor warehouse bundled as a SQLite database. It mirrors the `survivoR` R package experience so analysts can explore the data in python and python notebooks, while also including two additional layers of curated tables.

Looking for the full warehouse documentation? See the [Gamebot Warehouse schema guide](gamebot_warehouse_schema_guide.md) and the [join cheat sheet + IDE tips](gamebot_warehouse_cheatsheet.md).

## Installation

For most users (analysts and notebook authors):

```bash
python -m pip install --upgrade gamebot-lite
```

If you need DuckDB features (for `duckdb_query` and other helpers), install the optional extra:

```bash
python -m pip install --upgrade "gamebot-lite[duckdb]"
```

Notebook / Jupyter-friendly cell (copy straight into a notebook cell):

```python
# Default install inside a notebook kernel
%pip install --upgrade gamebot-lite

# Or, include DuckDB support
%pip install --upgrade "gamebot-lite[duckdb]"
```

Notes:
- Use the plain `gamebot-lite` install by default; include the `duckdb` extra when you need SQL helper functions that rely on DuckDB and do not have DuckDB already installed.
- The `%pip` form is recommended inside notebook cells so the kernel picks up newly-installed packages immediately.

## Layers

- **Bronze (Raw)** – Direct exports of the survivoR tables (e.g., `bronze.castaway_details`, `bronze.season_summary`). These retain their original column names and provide provenance-friendly raw data.
- **Silver (Curated)** – Analytics-ready tables with clearer names (e.g., `silver.castaway_profile`, `silver.challenge_results_curated`). These consolidate joins and add derived columns.
- **Gold (ML Feature)** – Feature-layer snapshots (e.g., `gold.features_castaway_episode`) tailored for modeling and longitudinal analysis.

Inside the SQLite file every table is stored with its Gamebot Lite-friendly name. The Python helpers keep the warehouse layer visible: `load_table` accepts either `layer="silver"` or a fully-qualified identifier, and `duckdb_query` registers `bronze.*`, `silver.*`, and `gold.*` views automatically so layer prefixes stay in your SQL. Metadata about the export itself lives in `metadata.gamebot_ingestion_metadata`.

## Usage

```python
from gamebot_lite import GamebotClient, get_default_client, load_table, duckdb_query

# Pandas example (bronze layer) — explicit layer keeps intent obvious
df_castaways = load_table("castaway_details", layer="bronze")
print(df_castaways.head())

# You can also work with the client directly
client = get_default_client()
silver_tables = client.list_tables(layer="silver")

# DuckDB SQL example (requires `pip install duckdb`)
sql = """
SELECT original_tribe, COUNT(*) AS castaway_count
FROM silver.castaway_season_profile
GROUP BY original_tribe
ORDER BY castaway_count DESC
"""

df_tribes = duckdb_query(sql)
print(df_tribes)
```

Need a different SQLite file? Instantiate `GamebotClient(Path(...))` with your custom export path (`from pathlib import Path`).

Each dataframe returned by `load_table` includes `df.attrs["gamebot_layer"]` and `df.attrs["warehouse_table"]` so you can audit which warehouse object produced the data in downstream notebooks.

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

#### Metadata

| Table | Description |
| --- | --- |
| `metadata.gamebot_ingestion_metadata` | Loader run metadata (environment, git details). |

Load metadata with `load_table("gamebot_ingestion_metadata", layer="metadata")` or via SQL `SELECT * FROM metadata.gamebot_ingestion_metadata`.

#### Silver tables (Curated)

| Gamebot Lite table | Description |
| --- | --- |
| `advantage_catalog` | Advantage dimension with canonical attributes. |
| `castaway_profile` | Dimension table with enriched castaway attributes. |
| `challenge_catalog` | Challenge dimension with analytics-friendly columns. |
| `episode_profile` | Episode dimension with air dates and numbering. |
| `season_profile` | Season dimension capturing themes, twists, and geography. |
| `advantage_movement_curated` | Advantage lifecycle events (found, passed, played). |
| `boot_mapping_curated` | Mapping of episode boot events to castaways. |
| `challenge_results_curated` | Curated challenge results with keys to dimensions. |
| `confessional_summary` | Aggregated confessional counts and airtime. |
| `jury_votes_curated` | Jury vote outcomes tied to castaway dimension keys. |
| `tribe_membership_curated` | Tribe membership timeline per castaway. |
| `vote_history_curated` | Curated vote history with consistent castaway keys. |
| `challenge_skill_assignment` | Bridge table connecting challenges to skill types. |
| `challenge_skill` | Lookup table of challenge skill taxonomy. |
| `castaway_season_profile` | Bridge table linking castaways to seasons with roles. |

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
