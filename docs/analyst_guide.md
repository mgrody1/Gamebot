# Gamebot for Data Analysts & Scientists

**Quick Start**: [Install gamebot-lite](#installation) → [Load data](#basic-usage) → [Explore tables](#available-data) → [Advanced queries](#advanced-analytics)

This guide covers everything analysts need to get productive with Survivor data analysis using Gamebot Lite.

---

## Installation

**Recommended**: Install with pandas for immediate data access:
```bash
pip install gamebot-lite
```

**For SQL Analytics**: Add DuckDB for complex queries on larger datasets:
```bash
pip install gamebot-lite[duckdb]
```

**Requirements**: Python 3.8+, no other dependencies needed.

---

## Basic Usage

### Load Any Table
```python
from gamebot_lite import load_table

# Load as pandas DataFrame
vote_history = load_table("vote_history_curated")
jury_votes = load_table("jury_votes")
castaways = load_table("castaways")

# Explore structure
print(vote_history.columns.tolist())
print(f"Rows: {len(vote_history)}")
```

### SQL-Style Queries (Optional)
If you installed the `duckdb` extra, you can run complex SQL queries:

```python
from gamebot_lite import duckdb_query

# Find seasons with the most tie votes
results = duckdb_query("""
    SELECT season, COUNT(*) as tie_votes
    FROM vote_history_curated
    WHERE vote_order = 'Tie'
    GROUP BY season
    ORDER BY tie_votes DESC
    LIMIT 10
""")
```

---

## Available Data

Gamebot Lite contains 30+ tables organized in three layers:

### Bronze Layer (Raw Data)
Direct mirrors of the survivoR dataset with minimal processing:

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `castaways` | Contestant demographics & background | `castaway_id`, `version_season`, `full_name` |
| `episodes` | Season & episode metadata | `version_season`, `episode`, `air_date` |
| `vote_history` | Tribal council voting records | `castaway_id`, `vote`, `voted_out` |
| `jury_votes` | Final tribal council votes | `castaway_id`, `finalist`, `vote` |
| `challenge_results` | Individual challenge performance | `castaway_id`, `challenge_id`, `result` |
| `confessionals` | Edit/narrative content analysis | `castaway_id`, `confessional_count`, `index_count` |

**+ 15 more bronze tables** covering advantages, tribe mapping, season summaries, and detailed challenge data.

### Silver Layer (ML Features)
Curated feature engineering tables designed for analysis and modeling:

| Table | Description | Use Cases |
|-------|-------------|-----------|
| `castaway_profile_curated` | Demographics + background features | Winner prediction, casting analysis |
| `challenge_performance_curated` | Physical & mental game metrics | Performance analysis, challenge design |
| `voting_dynamics_curated` | Strategic voting patterns | Alliance tracking, betrayal analysis |
| `social_positioning_curated` | Tribe & alliance relationships | Social network analysis |
| `advantage_strategy_curated` | Hidden immunity & advantage usage | Strategic gameplay analysis |
| `edit_analysis_curated` | Confessional & screen time metrics | Winner prediction, edit theory |

**These tables don't exist in the original survivoR dataset** - they're engineered specifically for ML and advanced analytics.

### Gold Layer (ML-Ready Matrices)
Production-ready feature matrices for machine learning:

| Table | Description | Observations | Features |
|-------|-------------|--------------|----------|
| `ml_features_gameplay` | Gameplay-only features | 4,248 | Challenge, voting, advantage, social |
| `ml_features_hybrid` | Gameplay + edit features | 4,248 | Above + confessionals, screen time |

Each row represents one castaway-season combination with comprehensive features for winner prediction modeling.

---

## Common Analysis Patterns

### Winner Prediction Research
```python
# Load ML-ready features
features = load_table("ml_features_gameplay")

# Basic winner analysis
winners = features[features['winner'] == True]
print(f"Winners dataset: {len(winners)} observations")
print(winners['challenge_win_rate'].describe())
```

### Strategic Voting Analysis
```python
# Voting dynamics
voting = load_table("voting_dynamics_curated")

# Find players with most strategic moves
strategic_players = voting.groupby('castaway_id').agg({
    'votes_cast_total': 'sum',
    'votes_received_total': 'sum',
    'alliance_betrayals': 'sum'
}).sort_values('alliance_betrayals', ascending=False)
```

### Challenge Performance Trends
```python
# Challenge performance over time
challenges = load_table("challenge_performance_curated")

# Performance by season
season_performance = challenges.groupby('version_season').agg({
    'individual_immunity_wins': 'mean',
    'individual_reward_wins': 'mean',
    'challenge_win_rate': 'mean'
})
```

---

## Advanced Analytics

### Time Series Analysis
```python
# Episode-by-episode confessional tracking
confessionals = load_table("confessionals")

# Track edit prominence over season
edit_tracking = confessionals.groupby(['version_season', 'episode']).agg({
    'confessional_count': 'sum',
    'index_count': 'sum'
}).reset_index()
```

### Network Analysis
```python
# Social positioning and alliances
social = load_table("social_positioning_curated")

# Alliance network analysis
alliance_networks = social.groupby(['version_season', 'episode']).agg({
    'tribe_size': 'first',
    'alliance_size': 'mean',
    'cross_tribal_connections': 'sum'
})
```

### Complex SQL Analytics (with DuckDB)
```python
from gamebot_lite import duckdb_query

# Multi-table analysis: voting patterns vs challenge performance
complex_analysis = duckdb_query("""
WITH player_stats AS (
  SELECT
    c.castaway_id,
    c.version_season,
    c.full_name,
    ch.challenge_win_rate,
    v.strategic_vote_percentage,
    cp.winner
  FROM challenge_performance_curated ch
  JOIN voting_dynamics_curated v ON ch.castaway_id = v.castaway_id
  JOIN castaway_profile_curated cp ON ch.castaway_id = cp.castaway_id
  JOIN castaways c ON ch.castaway_id = c.castaway_id
)
SELECT
  winner,
  AVG(challenge_win_rate) as avg_challenge_performance,
  AVG(strategic_vote_percentage) as avg_strategic_voting,
  COUNT(*) as players
FROM player_stats
GROUP BY winner
ORDER BY winner DESC
""")
```

---

## Schema Reference

### Key Relationships
- **Join Key**: `castaway_id` connects most tables
- **Season Filter**: `version_season` (format: "US43", "AUS07", etc.)
- **Episode Tracking**: `episode` for temporal analysis

### Data Quality Notes
- **Coverage**: Seasons 1-47 (US), plus international seasons
- **Missing Data**: Early seasons have limited confessional/edit data
- **Updates**: Data refreshed when new survivoR releases are available

### Data Dictionary
For complete column descriptions and data types, see the [official survivoR documentation](https://cran.r-project.org/web/packages/survivoR/survivoR.pdf).

---

## Getting Help

### Troubleshooting
- **Import Error**: Ensure `pip install gamebot-lite` completed successfully
- **Missing Table**: Check table name spelling (case-sensitive)
- **DuckDB Error**: Install with `pip install gamebot-lite[duckdb]`

### Advanced Usage
- **Custom Analysis**: Use pandas operations on loaded DataFrames
- **Large Datasets**: Use DuckDB queries for memory efficiency
- **Export Results**: Save with `df.to_csv()`, `df.to_excel()`, etc.

### Community & Support
- **Issues**: [GitHub Issues](https://github.com/mgrody1/Gamebot/issues)
- **Examples**: See `examples/` directory in the repository
- **Updates**: Follow releases for new data and features

---

## Example Analyses

### Research Applications
- **Winner Prediction**: Use gold layer ML features with scikit-learn
- **Game Theory**: Analyze voting coalitions and betrayal patterns
- **Edit Analysis**: Study narrative construction vs actual gameplay
- **Challenge Design**: Examine challenge types vs player performance
- **Casting Analysis**: Demographics vs success patterns

### Academic Research
The dataset supports research in:
- Social network analysis
- Game theory and strategic decision-making
- Media narrative construction
- Competitive psychology
- Group dynamics and leadership

---

## Upgrading to Full Warehouse

For teams needing:
- **Real-time updates** from survivoR releases
- **Custom feature engineering** pipelines
- **Team collaboration** via shared database
- **BI tool integration** (Tableau, PowerBI)

Consider upgrading to [Gamebot Warehouse](../README.md#gamebot-warehouse---production-deployment) for a full PostgreSQL deployment with Airflow orchestration.
