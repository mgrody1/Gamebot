-- Refresh Survivor gold-layer feature tables derived from the silver layer.

TRUNCATE TABLE gold.castaway_episode_features RESTART IDENTITY;
TRUNCATE TABLE gold.season_prediction_features RESTART IDENTITY;


WITH episodes AS (
    SELECT
        bcs.castaway_key,
        bcs.season_key,
        de.episode_key,
        de.version_season,
        de.episode_in_season,
        bcs.castaway_id,
        bcs.winner,
        bcs.finalist,
        bcs.result_number
    FROM silver.bridge_castaway_season bcs
    JOIN silver.dim_episode de
      ON de.version_season = bcs.version_season
),
confessional AS (
    SELECT
        castaway_key,
        episode_key,
        SUM(confessional_count) AS confessional_count,
        SUM(confessional_time) AS confessional_time,
        SUM(expected_count) AS expected_count,
        SUM(expected_time) AS expected_time
    FROM silver.fact_confessionals
    GROUP BY castaway_key, episode_key
),
challenge_summary AS (
    SELECT
        castaway_key,
        episode_key,
        COUNT(*) FILTER (WHERE LOWER(result) LIKE 'win%') AS challenge_wins,
        COUNT(*) FILTER (WHERE sit_out) AS challenge_sit_outs
    FROM silver.fact_challenge_results
    GROUP BY castaway_key, episode_key
),
vote_summary AS (
    SELECT
        castaway_key,
        episode_key,
        COUNT(*) AS votes_cast,
        COUNT(*) FILTER (WHERE voted_out_castaway_id = castaway_id) AS votes_against,
        COUNT(*) FILTER (WHERE immunity) AS immunity_votes
    FROM silver.fact_vote_history
    GROUP BY castaway_key, episode_key
),
advantage_summary AS (
    SELECT
        castaway_key,
        episode_key,
        COUNT(*) AS advantages_played,
        COUNT(*) FILTER (WHERE success) AS advantages_successful
    FROM silver.fact_advantage_movement
    WHERE castaway_key IS NOT NULL
    GROUP BY castaway_key, episode_key
),
tribe_summary AS (
    SELECT
        castaway_key,
        episode_key,
        COUNT(DISTINCT tribe) AS tribe_count
    FROM silver.fact_tribe_membership
    GROUP BY castaway_key, episode_key
),
boot_status AS (
    SELECT
        e.castaway_key,
        e.episode_key,
        CASE
            WHEN EXISTS (
                SELECT 1
                FROM silver.fact_boot_mapping bm
                WHERE bm.castaway_key = e.castaway_key
                  AND bm.episode_in_season <= e.episode_in_season
                  AND LOWER(COALESCE(bm.game_status, '')) IN (
                      'voted out', 'booted', 'eliminated', 'medically evacuated', 'quit'
                  )
            ) THEN FALSE
            ELSE TRUE
        END AS still_in_game
    FROM episodes e
)
INSERT INTO gold.castaway_episode_features (
    castaway_key,
    season_key,
    episode_key,
    version_season,
    episode_in_season,
    features,
    label_winner,
    label_finalist,
    label_place
)
SELECT
    e.castaway_key,
    e.season_key,
    e.episode_key,
    e.version_season,
    e.episode_in_season,
    jsonb_build_object(
        'confessional_count', COALESCE(cf.confessional_count, 0),
        'confessional_time', COALESCE(cf.confessional_time, 0),
        'expected_confessional_count', COALESCE(cf.expected_count, 0),
        'expected_confessional_time', COALESCE(cf.expected_time, 0),
        'challenge_wins', COALESCE(ch.challenge_wins, 0),
        'challenge_sit_outs', COALESCE(ch.challenge_sit_outs, 0),
        'votes_cast', COALESCE(vh.votes_cast, 0),
        'votes_against', COALESCE(vh.votes_against, 0),
        'immunity_votes', COALESCE(vh.immunity_votes, 0),
        'advantages_played', COALESCE(am.advantages_played, 0),
        'advantages_successful', COALESCE(am.advantages_successful, 0),
        'tribe_count', COALESCE(tm.tribe_count, 0),
        'still_in_game', COALESCE(bs.still_in_game, TRUE)
    ),
    COALESCE(e.winner, FALSE),
    COALESCE(e.finalist, FALSE),
    e.result_number
FROM episodes e
LEFT JOIN confessional cf
  ON cf.castaway_key = e.castaway_key
 AND cf.episode_key = e.episode_key
LEFT JOIN challenge_summary ch
  ON ch.castaway_key = e.castaway_key
 AND ch.episode_key = e.episode_key
LEFT JOIN vote_summary vh
  ON vh.castaway_key = e.castaway_key
 AND vh.episode_key = e.episode_key
LEFT JOIN advantage_summary am
  ON am.castaway_key = e.castaway_key
 AND am.episode_key = e.episode_key
LEFT JOIN tribe_summary tm
  ON tm.castaway_key = e.castaway_key
 AND tm.episode_key = e.episode_key
LEFT JOIN boot_status bs
  ON bs.castaway_key = e.castaway_key
 AND bs.episode_key = e.episode_key;


INSERT INTO gold.season_prediction_features (
    season_key,
    version_season,
    feature_snapshot_episode,
    features,
    label_winner_castaway_id
)
SELECT
    ds.season_key,
    ds.version_season,
    de.episode_in_season,
    jsonb_build_object(
        'mean_confessional_count', AVG((cef.features->>'confessional_count')::NUMERIC),
        'mean_votes_against', AVG((cef.features->>'votes_against')::NUMERIC),
        'total_advantages_played', SUM((cef.features->>'advantages_played')::NUMERIC),
        'remaining_players', COUNT(*) FILTER (WHERE (cef.features->>'still_in_game')::BOOLEAN)
    ),
    (
        SELECT bc.castaway_id
        FROM silver.bridge_castaway_season bc
        WHERE bc.season_key = ds.season_key
          AND bc.winner IS TRUE
        LIMIT 1
    )
FROM gold.castaway_episode_features cef
JOIN silver.dim_season ds
  ON ds.season_key = cef.season_key
JOIN silver.dim_episode de
  ON de.episode_key = cef.episode_key
GROUP BY
    ds.season_key,
    ds.version_season,
    de.episode_in_season
ORDER BY
    ds.version_season,
    de.episode_in_season;
