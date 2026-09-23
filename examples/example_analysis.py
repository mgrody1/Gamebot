# %% [markdown]
# Example quick analysis using gamebot-lite (Jupytext percent script).

# %%
from gamebot_lite import duckdb_query


# Use a valid gold table: ml_features_hybrid (joined to silver for names)
query = """
SELECT
  cp.full_name,
  cp.season_name,
  ml.target_placement,
  ml.challenges_won,
  ml.votes_correct,
  ml.total_confessional_count,
  ml.jury_votes_received
FROM gold.ml_features_hybrid AS ml
JOIN silver.castaway_profile AS cp
  ON cp.castaway_id = ml.castaway_id
  AND cp.version_season = ml.version_season
ORDER BY ml.castaway_id
LIMIT 10
"""

print(duckdb_query(query).to_string(index=False))
