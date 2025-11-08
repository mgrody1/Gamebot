import streamlit as st
from gamebot_lite import duckdb_query

st.set_page_config(page_title="Gamebot Lite quick look", layout="wide")
st.title("Survivor insights in seconds")

query = """
SELECT
  season_name,
  castaway,
  total_votes_received,
  total_immunity_wins
FROM gold.castaway_season_features
ORDER BY total_votes_received DESC
LIMIT 25
"""

data = duckdb_query(query)

st.metric("Seasons", data["season_name"].nunique())
st.metric("Rows returned", len(data))

st.dataframe(data)
