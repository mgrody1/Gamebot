"""Catalog metadata describing Gamebot Lite tables and warehouse mappings."""

from __future__ import annotations

from typing import Dict, Iterable, Mapping, MutableMapping

VALID_LAYERS = ("bronze", "silver", "gold")

# Bronze tables ship with their warehouse names.
BRONZE_TABLES = [
    "ingestion_runs",
    "dataset_versions",
    "castaway_details",
    "season_summary",
    "advantage_details",
    "challenge_description",
    "challenge_summary",
    "episodes",
    "castaways",
    "advantage_movement",
    "boot_mapping",
    "boot_order",
    "auction_details",
    "survivor_auction",
    "castaway_scores",
    "journeys",
    "tribe_mapping",
    "confessionals",
    "challenge_results",
    "vote_history",
    "jury_votes",
]

# Metadata tables bundled with the export (not tied to a single layer).
METADATA_TABLES = ["gamebot_ingestion_metadata"]


# Silver tables: match dbt/models/silver/*.sql
SILVER_FRIENDLY_NAME_OVERRIDES = {
    "advantage_strategy": "advantage_strategy",
    "season_context": "season_context",
    "vote_dynamics": "vote_dynamics",
    "edit_features": "edit_features",
    "jury_analysis": "jury_analysis",
    "castaway_profile": "castaway_profile",
    "social_positioning": "social_positioning",
    "challenge_performance": "challenge_performance",
}

# Gold tables: match dbt/models/gold/*.sql
GOLD_FRIENDLY_NAME_OVERRIDES = {
    "ml_features_hybrid": "ml_features_hybrid",
    "ml_features_non_edit": "ml_features_non_edit",
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
