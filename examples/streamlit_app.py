import streamlit as st
from gamebot_lite import duckdb_query

st.set_page_config(page_title="Gamebot Lite quick look", layout="wide")
st.title("Survivor insights in seconds")


# Use a valid gold table: ml_features_hybrid
query = """
SELECT
  *
FROM gold.ml_features_hybrid
ORDER BY castaway_id
LIMIT 25
"""

data = duckdb_query(query)

st.metric("Seasons", data["season_name"].nunique())
st.metric("Rows returned", len(data))

st.dataframe(data)
