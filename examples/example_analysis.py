# %% [markdown]
# Example quick analysis using gamebot-lite (Jupytext percent script).

# %%
from gamebot_lite import duckdb_query


# Use a valid gold table: ml_features_hybrid
query = """
SELECT
  *
FROM gold.ml_features_hybrid
ORDER BY castaway_id
LIMIT 10
"""

print(duckdb_query(query))
