# %% [markdown]
# Example quick analysis using gamebot-lite (Jupytext percent script).

# %%
from gamebot_lite import duckdb_query

query = """
SELECT
  season_name,
  castaway,
  total_votes_received
FROM gold.castaway_season_features
ORDER BY total_votes_received DESC
LIMIT 10
"""

print(duckdb_query(query))
