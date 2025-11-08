"""Catalog metadata describing Gamebot Lite tables and warehouse mappings."""

from __future__ import annotations

from typing import Dict, Iterable, Mapping, MutableMapping

VALID_LAYERS = ("bronze", "silver", "gold")

# Bronze tables ship with their warehouse names.
BRONZE_TABLES = [
    "advantage_details",
    "advantage_movement",
    "boot_mapping",
    "castaway_details",
    "castaways",
    "challenge_description",
    "challenge_results",
    "confessionals",
    "episodes",
    "jury_votes",
    "season_summary",
    "tribe_mapping",
    "vote_history",
    "vote_history_extended",
]

# Metadata tables bundled with the export (not tied to a single layer).
METADATA_TABLES = ["gamebot_ingestion_metadata"]

# Silver tables are renamed to friendlier aliases inside the SQLite export.
SILVER_FRIENDLY_NAME_OVERRIDES = {
    "dim_castaway": "castaway_profile",
    "dim_season": "season_profile",
    "dim_episode": "episode_profile",
    "dim_advantage": "advantage_catalog",
    "dim_challenge": "challenge_catalog",
    "challenge_skill_lookup": "challenge_skill",
    "challenge_skill_bridge": "challenge_skill_assignment",
    "bridge_castaway_season": "castaway_season_profile",
    "fact_confessionals": "confessional_summary",
    "fact_challenge_results": "challenge_results_curated",
    "fact_vote_history": "vote_history_curated",
    "fact_advantage_movement": "advantage_movement_curated",
    "fact_boot_mapping": "boot_mapping_curated",
    "fact_tribe_membership": "tribe_membership_curated",
    "fact_jury_votes": "jury_votes_curated",
}

# Gold tables likewise receive clearer aliases.
GOLD_FRIENDLY_NAME_OVERRIDES = {
    "feature_snapshots": "feature_snapshots",
    "castaway_season_features": "features_castaway_season",
    "castaway_episode_features": "features_castaway_episode",
    "season_features": "features_season",
}


def friendly_name_overrides(schema: str) -> Mapping[str, str]:
    """Return the warehouse → Gamebot Lite friendly table name overrides."""

    if schema == "silver":
        return SILVER_FRIENDLY_NAME_OVERRIDES
    if schema == "gold":
        return GOLD_FRIENDLY_NAME_OVERRIDES
    return {}


def friendly_tables_for_layer(layer: str) -> Iterable[str]:
    """Return the tables exposed to analysts for the requested layer."""

    if layer not in VALID_LAYERS:
        raise ValueError(f"Unknown layer '{layer}'. Expected one of {VALID_LAYERS}.")

    if layer == "bronze":
        return tuple(BRONZE_TABLES)
    if layer == "silver":
        return tuple(SILVER_FRIENDLY_NAME_OVERRIDES.values())
    return tuple(GOLD_FRIENDLY_NAME_OVERRIDES.values())


def build_layer_lookup() -> Dict[str, str]:
    """Return a mapping of friendly table name → layer."""

    lookup: MutableMapping[str, str] = {}
    for table in BRONZE_TABLES:
        lookup[table] = "bronze"
    for friendly in SILVER_FRIENDLY_NAME_OVERRIDES.values():
        lookup[friendly] = "silver"
    for friendly in GOLD_FRIENDLY_NAME_OVERRIDES.values():
        lookup[friendly] = "gold"
    for table in METADATA_TABLES:
        lookup[table] = "metadata"
    return dict(lookup)


TABLE_LAYER_MAP: Dict[str, str] = build_layer_lookup()


def build_warehouse_lookup() -> Dict[str, str]:
    """Return friendly table name → fully qualified warehouse table."""

    lookup: MutableMapping[str, str] = {}
    lookup.update({table: f"bronze.{table}" for table in BRONZE_TABLES})
    for warehouse_table, friendly in SILVER_FRIENDLY_NAME_OVERRIDES.items():
        lookup[friendly] = f"silver.{warehouse_table}"
    for warehouse_table, friendly in GOLD_FRIENDLY_NAME_OVERRIDES.items():
        lookup[friendly] = f"gold.{warehouse_table}"
    return dict(lookup)


WAREHOUSE_TABLE_MAP: Dict[str, str] = build_warehouse_lookup()
