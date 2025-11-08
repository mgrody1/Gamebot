# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: gamebot
#     language: python
#     name: gamebot
# ---

# %% [markdown]
# # Ad-hoc Analysis Notebook
#
# Use this space to explore bronze or silver tables. The kernel `gamebot` (installed via Dev Container/Pipenv) already has the project dependencies.
#

# ruff: noqa: E402

# %%
import sys
from pathlib import Path

# make repo modules importable
NOTEBOOK_DIR = (
    Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
)
REPO_ROOT = (
    NOTEBOOK_DIR if (NOTEBOOK_DIR / "params.py").exists() else NOTEBOOK_DIR.parent
)
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

import pandas as pd
import plotly.express as px
import seaborn as sns
import matplotlib.pyplot as plt

from gamebot_core.db_utils import create_sql_engine

engine = create_sql_engine()

# example: load a table
df_castaways = pd.read_sql("select * from bronze.castaway_details", con=engine)
df_castaways.head()


# %% [markdown]
# ## Visual exploration
#

# %%
plt.figure(figsize=(10, 4))
sns.countplot(data=df_castaways, x="gender")
plt.title("Castaways by gender")
plt.show()

# %%
# Plotly example for interactive exploration
px_fig = px.histogram(
    df_castaways,
    x="age",
    title="Castaway age distribution",
    nbins=15,
)
px_fig.show()
