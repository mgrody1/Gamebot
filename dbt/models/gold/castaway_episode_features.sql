{{ config(materialized='table', schema='gold') }}

with current_snapshot as (
    select snapshot_id
    from {{ ref('feature_snapshots') }}
    order by run_started_at desc nulls last
    limit 1
),
episode_confessionals AS (
    SELECT castaway_key, season_key, episode_key,
           SUM(confessional_count) AS confessional_count,
           SUM(confessional_time) AS confessional_time
    FROM {{ ref('fact_confessionals') }}
    GROUP BY castaway_key, season_key, episode_key
),
episode_challenges AS (
    SELECT castaway_key, season_key, episode_key,
           COUNT(*) FILTER (WHERE LOWER(result) LIKE 'win%') AS wins,
           SUM(CASE WHEN chosen_for_reward THEN 1 ELSE 0 END) AS rewards,
           SUM(CASE WHEN sit_out THEN 1 ELSE 0 END) AS sitouts
    FROM {{ ref('fact_challenge_results') }}
    GROUP BY castaway_key, season_key, episode_key
),
episode_votes_cast AS (
    SELECT castaway_key, season_key, episode_key,
           COUNT(*) AS votes_cast,
           COUNT(*) FILTER (WHERE voted_out_castaway_id = target_castaway_id) AS votes_correct,
           COUNT(*) FILTER (WHERE voted_out_castaway_id IS DISTINCT FROM target_castaway_id OR voted_out_castaway_id IS NULL) AS votes_incorrect
    FROM {{ ref('fact_vote_history') }}
    GROUP BY castaway_key, season_key, episode_key
),
episode_votes_received AS (
    SELECT castaway_key, season_key, episode_key,
           COUNT(*) AS votes_received
    FROM {{ ref('fact_vote_history') }}
    GROUP BY castaway_key, season_key, episode_key
),
episode_union AS (
    SELECT castaway_key, season_key, episode_key FROM episode_confessionals
    UNION
    SELECT castaway_key, season_key, episode_key FROM episode_challenges
    UNION
    SELECT castaway_key, season_key, episode_key FROM episode_votes_cast
    UNION
    SELECT castaway_key, season_key, episode_key FROM episode_votes_received
),
episode_features_raw AS (
    SELECT
        eu.castaway_key,
        eu.season_key,
        eu.episode_key,
        de.version_season,
        de.episode_in_season,
        COALESCE(ec.confessional_count, 0) AS confessional_count_episode,
        COALESCE(ec.confessional_time, 0) AS confessional_time_episode,
        COALESCE(ech.wins, 0) AS challenge_wins_episode,
        COALESCE(ech.rewards, 0) AS chosen_for_reward_episode,
        COALESCE(ech.sitouts, 0) AS sitouts_episode,
        COALESCE(ev.votes_cast, 0) AS votes_cast_episode,
        COALESCE(ev.votes_correct, 0) AS votes_correct_episode,
        COALESCE(ev.votes_incorrect, 0) AS votes_incorrect_episode,
        COALESCE(evr.votes_received, 0) AS votes_received_episode
    FROM episode_union eu
    JOIN {{ ref('dim_episode') }} de ON de.episode_key = eu.episode_key
    LEFT JOIN episode_confessionals ec ON ec.castaway_key = eu.castaway_key AND ec.season_key = eu.season_key AND ec.episode_key = eu.episode_key
    LEFT JOIN episode_challenges ech ON ech.castaway_key = eu.castaway_key AND ech.season_key = eu.season_key AND ech.episode_key = eu.episode_key
    LEFT JOIN episode_votes_cast ev ON ev.castaway_key = eu.castaway_key AND ev.season_key = eu.season_key AND ev.episode_key = eu.episode_key
    LEFT JOIN episode_votes_received evr ON evr.castaway_key = eu.castaway_key AND evr.season_key = eu.season_key AND evr.episode_key = eu.episode_key
),
episode_features_enriched AS (
    SELECT
        efr.*,
        SUM(confessional_count_episode) OVER (PARTITION BY castaway_key ORDER BY episode_in_season ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS confessional_count_cumulative,
        SUM(confessional_time_episode) OVER (PARTITION BY castaway_key ORDER BY episode_in_season ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS confessional_time_cumulative,
        SUM(challenge_wins_episode) OVER (PARTITION BY castaway_key ORDER BY episode_in_season ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS challenge_wins_cumulative,
        SUM(chosen_for_reward_episode) OVER (PARTITION BY castaway_key ORDER BY episode_in_season ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS chosen_for_reward_cumulative,
        SUM(sitouts_episode) OVER (PARTITION BY castaway_key ORDER BY episode_in_season ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sitouts_cumulative,
        SUM(votes_cast_episode) OVER (PARTITION BY castaway_key ORDER BY episode_in_season ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS votes_cast_cumulative,
        SUM(votes_correct_episode) OVER (PARTITION BY castaway_key ORDER BY episode_in_season ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS votes_correct_cumulative,
        SUM(votes_incorrect_episode) OVER (PARTITION BY castaway_key ORDER BY episode_in_season ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS votes_incorrect_cumulative,
        SUM(votes_received_episode) OVER (PARTITION BY castaway_key ORDER BY episode_in_season ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS votes_received_cumulative
    FROM episode_features_raw efr
)

SELECT
    cs.snapshot_id,
    efee.castaway_key,
    efee.season_key,
    efee.episode_key,
    dc.castaway_id,
    efee.version_season,
    efee.episode_in_season,
    jsonb_build_object(
        'episode_confessionals', jsonb_build_object(
            'count', efee.confessional_count_episode,
            'time', efee.confessional_time_episode,
            'cumulative_count', efee.confessional_count_cumulative,
            'cumulative_time', efee.confessional_time_cumulative
        ),
        'episode_challenges', jsonb_build_object(
            'wins', efee.challenge_wins_episode,
            'chosen_for_reward', efee.chosen_for_reward_episode,
            'sitouts', efee.sitouts_episode,
            'wins_cumulative', efee.challenge_wins_cumulative,
            'chosen_for_reward_cumulative', efee.chosen_for_reward_cumulative,
            'sitouts_cumulative', efee.sitouts_cumulative
        ),
        'episode_votes', jsonb_build_object(
            'votes_cast', efee.votes_cast_episode,
            'votes_correct', efee.votes_correct_episode,
            'votes_incorrect', efee.votes_incorrect_episode,
            'votes_received', efee.votes_received_episode,
            'votes_cast_cumulative', efee.votes_cast_cumulative,
            'votes_correct_cumulative', efee.votes_correct_cumulative,
            'votes_incorrect_cumulative', efee.votes_incorrect_cumulative,
            'votes_received_cumulative', efee.votes_received_cumulative
        )
    ) AS feature_payload
FROM episode_features_enriched efee
CROSS JOIN current_snapshot cs
JOIN {{ ref('dim_castaway') }} dc ON dc.castaway_key = efee.castaway_key;
