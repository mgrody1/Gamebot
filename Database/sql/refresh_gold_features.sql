WITH latest_run AS (
    SELECT run_id, environment, git_branch, git_commit
    FROM bronze.ingestion_runs
    ORDER BY run_started_at DESC
    LIMIT 1
),
snapshot AS (
    INSERT INTO gold.feature_snapshots (ingest_run_id, environment, git_branch, git_commit, notes)
    SELECT run_id, environment, git_branch, git_commit, 'Automated feature snapshot'
    FROM latest_run
    UNION ALL
    SELECT NULL,
           current_setting('survivor.environment', true),
           current_setting('survivor.git_branch', true),
           current_setting('survivor.git_commit', true),
           'Automated feature snapshot'
    WHERE NOT EXISTS (SELECT 1 FROM latest_run)
    RETURNING snapshot_id
),
active_snapshot AS (
    SELECT snapshot_id FROM snapshot
),
season_bounds AS (
    SELECT MIN(season_number) AS min_season, MAX(season_number) AS max_season
    FROM silver.dim_season
),
castaway_base AS (
    SELECT
        bcs.castaway_key,
        bcs.season_key,
        dc.castaway_id,
        dc.full_name,
        dc.gender,
        dc.race,
        dc.ethnicity,
        dc.occupation,
        dc.personality_type,
        ds.version_season,
        ds.season_number,
        ds.season_name,
        ds.tribe_setup,
        ds.cast_size,
        ds.finalist_count,
        ds.jury_count,
        ds.premiered,
        ds.ended,
        bcs.original_tribe,
        bcs.result,
        bcs.place,
        bcs.jury,
        bcs.finalist,
        bcs.winner,
        (COUNT(*) OVER (PARTITION BY dc.castaway_id)) > 1 AS is_returning_player
    FROM silver.bridge_castaway_season bcs
    JOIN silver.dim_castaway dc ON dc.castaway_key = bcs.castaway_key
    JOIN silver.dim_season ds ON ds.season_key = bcs.season_key
),
castaway_age AS (
    SELECT
        castaway_id,
        version_season,
        AVG(age) FILTER (WHERE age IS NOT NULL) AS average_age,
        MIN(age) FILTER (WHERE age IS NOT NULL) AS first_recorded_age
    FROM bronze.castaways
    GROUP BY castaway_id, version_season
),
challenge_events AS (
    SELECT
        fcr.castaway_key,
        fcr.season_key,
        fcr.episode_key,
        fcr.castaway_id,
        dch.challenge_type,
        LOWER(fcr.result) AS result_lower,
        fcr.chosen_for_reward,
        fcr.sit_out
    FROM silver.fact_challenge_results fcr
    JOIN silver.dim_challenge dch ON dch.challenge_key = fcr.challenge_key
),
challenge_totals AS (
    SELECT
        castaway_key,
        season_key,
        COUNT(*) FILTER (WHERE result_lower LIKE 'win%') AS challenges_won_total,
        COUNT(*) FILTER (WHERE result_lower LIKE 'win%' AND (challenge_type ILIKE '%individual%')) AS challenges_won_individual,
        COUNT(*) FILTER (WHERE result_lower LIKE 'win%' AND (challenge_type ILIKE '%tribe%' OR challenge_type ILIKE '%team%')) AS challenges_won_team,
        SUM(CASE WHEN chosen_for_reward THEN 1 ELSE 0 END) AS chosen_for_reward_count,
        SUM(CASE WHEN sit_out THEN 1 ELSE 0 END) AS sit_out_count
    FROM challenge_events
    GROUP BY castaway_key, season_key
),
challenge_wins_by_type AS (
    SELECT castaway_key, season_key,
           jsonb_object_agg(challenge_type, wins) AS wins_by_type
    FROM (
        SELECT castaway_key, season_key, COALESCE(challenge_type, 'unknown') AS challenge_type,
               COUNT(*) FILTER (WHERE result_lower LIKE 'win%') AS wins
        FROM challenge_events
        GROUP BY castaway_key, season_key, challenge_type
    ) s
    GROUP BY castaway_key, season_key
),
advantage_events AS (
    SELECT
        fam.castaway_key,
        fam.season_key,
        fam.castaway_id,
        fam.target_castaway_id,
        LOWER(COALESCE(da.advantage_type, 'unknown')) AS advantage_type,
        LOWER(COALESCE(fam.event, '')) AS event_lower,
        fam.success
    FROM silver.fact_advantage_movement fam
    LEFT JOIN silver.dim_advantage da ON da.advantage_key = fam.advantage_key
),
advantage_totals AS (
    SELECT
        castaway_key,
        season_key,
        COUNT(*) FILTER (WHERE event_lower LIKE 'found%') AS advantages_found,
        COUNT(*) FILTER (WHERE event_lower LIKE 'played%') AS advantages_played,
        COUNT(*) FILTER (
            WHERE event_lower LIKE 'played%'
              AND advantage_type LIKE '%idol%'
              AND success IS TRUE
        ) AS idols_played_correctly,
        COUNT(*) FILTER (
            WHERE event_lower LIKE 'played%'
              AND advantage_type LIKE '%idol%'
              AND COALESCE(success, FALSE) IS FALSE
        ) AS idols_played_incorrectly,
        COUNT(*) FILTER (
            WHERE event_lower LIKE 'played%'
              AND advantage_type LIKE '%idol%'
              AND (target_castaway_id IS NULL OR target_castaway_id = castaway_id)
        ) AS idols_played_for_self,
        COUNT(*) FILTER (
            WHERE event_lower LIKE 'played%'
              AND advantage_type LIKE '%idol%'
              AND target_castaway_id IS NOT NULL
              AND target_castaway_id <> castaway_id
        ) AS idols_played_for_others
    FROM advantage_events
    GROUP BY castaway_key, season_key
),
vote_events AS (
    SELECT
        fvh.castaway_key,
        fvh.season_key,
        fvh.episode_key,
        fvh.castaway_id,
        fvh.target_castaway_id,
        fvh.voted_out_castaway_id,
        fvh.tribe_status
    FROM silver.fact_vote_history fvh
),
vote_totals AS (
    SELECT
        castaway_key,
        season_key,
        COUNT(*) AS total_votes_cast,
        COUNT(*) FILTER (WHERE voted_out_castaway_id = target_castaway_id) AS votes_correct,
        COUNT(*) FILTER (WHERE voted_out_castaway_id IS DISTINCT FROM target_castaway_id OR voted_out_castaway_id IS NULL) AS votes_incorrect,
        COUNT(DISTINCT episode_key) AS tribal_councils_attended,
        COUNT(DISTINCT episode_key) FILTER (WHERE tribe_status ILIKE 'merge%') AS tribal_councils_post_merge,
        COUNT(DISTINCT episode_key) FILTER (WHERE tribe_status NOT ILIKE 'merge%') AS tribal_councils_pre_merge
    FROM vote_events
    GROUP BY castaway_key, season_key
),
votes_received AS (
    SELECT
        dc.castaway_key,
        ve.season_key,
        COUNT(*) AS total_votes_received
    FROM vote_events ve
    JOIN silver.dim_castaway dc ON dc.castaway_id = ve.target_castaway_id
    GROUP BY dc.castaway_key, ve.season_key
),
vote_alignment AS (
    SELECT
        v.castaway_key,
        v.season_key,
        AVG(alignment) AS avg_vote_alignment
    FROM (
        SELECT
            voter.castaway_key,
            voter.season_key,
            voter.episode_key,
            CASE
                WHEN COUNT(*) FILTER (WHERE ally.target_castaway_id = voter.target_castaway_id) = 0
                     OR COUNT(*) = 0 THEN 0
                ELSE COUNT(*) FILTER (WHERE ally.target_castaway_id = voter.target_castaway_id)::NUMERIC
                     / NULLIF(COUNT(*), 0)
            END AS alignment
        FROM vote_events voter
        JOIN vote_events ally
          ON ally.season_key = voter.season_key
         AND ally.episode_key = voter.episode_key
         AND ally.castaway_key <> voter.castaway_key
        GROUP BY voter.castaway_key, voter.season_key, voter.episode_key
    ) v
    GROUP BY v.castaway_key, v.season_key
),
merge_vote_status AS (
    SELECT
        mv.castaway_key,
        mv.season_key,
        CASE
            WHEN mv.first_merge_episode IS NULL THEN NULL
            ELSE bool_or(me.voted_out_castaway_id = me.target_castaway_id)
        END AS merge_vote_correct
    FROM (
        SELECT
            castaway_key,
            season_key,
            MIN(episode_key) FILTER (WHERE tribe_status ILIKE 'merge%') AS first_merge_episode
        FROM vote_events
        GROUP BY castaway_key, season_key
    ) mv
    LEFT JOIN vote_events me
           ON me.castaway_key = mv.castaway_key
          AND me.season_key = mv.season_key
          AND me.episode_key = mv.first_merge_episode
    GROUP BY mv.castaway_key, mv.season_key, mv.first_merge_episode
),
confessional_totals AS (
    SELECT
        castaway_key,
        season_key,
        SUM(confessional_count) AS total_confessional_count,
        SUM(confessional_time) AS total_confessional_time
    FROM silver.fact_confessionals
    GROUP BY castaway_key, season_key
),
tribe_composition AS (
    SELECT
        tm.version_season,
        tm.episode_key,
        tm.tribe,
        COUNT(*) FILTER (WHERE dc.gender ILIKE 'male')::NUMERIC / NULLIF(COUNT(*), 0) AS male_ratio,
        COUNT(*) FILTER (WHERE dc.gender ILIKE 'female')::NUMERIC / NULLIF(COUNT(*), 0) AS female_ratio
    FROM silver.fact_tribe_membership tm
    JOIN silver.dim_castaway dc ON dc.castaway_key = tm.castaway_key
    GROUP BY tm.version_season, tm.episode_key, tm.tribe
),
tribe_balance AS (
    SELECT
        tm.castaway_key,
        tm.season_key,
        AVG(comp.female_ratio) AS avg_female_ratio,
        AVG(comp.male_ratio) AS avg_male_ratio
    FROM silver.fact_tribe_membership tm
    JOIN tribe_composition comp
      ON comp.version_season = tm.version_season
     AND comp.episode_key = tm.episode_key
     AND comp.tribe = tm.tribe
    GROUP BY tm.castaway_key, tm.season_key
),
post_merge_original_counts AS (
    SELECT
        tm.castaway_key,
        tm.season_key,
        MAX(count_same_original) AS original_tribe_post_merge
    FROM (
        SELECT
            tm.castaway_key,
            tm.season_key,
            tm.episode_key,
            COUNT(*) FILTER (WHERE peer_bc.original_tribe = self_bc.original_tribe) AS count_same_original
        FROM silver.fact_tribe_membership tm
        JOIN silver.bridge_castaway_season self_bc
          ON self_bc.castaway_key = tm.castaway_key
         AND self_bc.season_key = tm.season_key
        JOIN silver.fact_tribe_membership peer
          ON peer.version_season = tm.version_season
         AND peer.episode_key = tm.episode_key
         AND peer.tribe = tm.tribe
        JOIN silver.bridge_castaway_season peer_bc
          ON peer_bc.castaway_key = peer.castaway_key
         AND peer_bc.season_key = peer.season_key
        WHERE tm.tribe_status ILIKE 'merge%'
        GROUP BY tm.castaway_key, tm.season_key, tm.episode_key
    ) stats
    GROUP BY stats.castaway_key, stats.season_key
),
season_returnee_ratio AS (
    SELECT
        season_key,
        SUM(CASE WHEN is_returning_player THEN 1 ELSE 0 END)::NUMERIC / NULLIF(COUNT(*), 0) AS returnee_ratio
    FROM castaway_base
    GROUP BY season_key
),
season_misc AS (
    SELECT
        ds.season_key,
        ds.version_season,
        ds.season_number,
        ds.tribe_setup,
        ds.cast_size,
        ds.finalist_count,
        ds.jury_count,
        ds.location,
        ds.country,
        CASE
            WHEN bounds.max_season > bounds.min_season THEN 1 - (bounds.max_season - ds.season_number)::NUMERIC / NULLIF(bounds.max_season - bounds.min_season, 0)
            ELSE 1
        END AS season_weight,
        (ds.cast_size IS NOT NULL AND ds.cast_size <= 18) AS is_new_era,
        ds.tribe_setup ILIKE '%Edge%' AS twist_edge_of_extinction,
        ds.tribe_setup ILIKE '%Redemption%' AS twist_redemption_island
    FROM silver.dim_season ds
    CROSS JOIN season_bounds bounds
),
season_misc_enriched AS (
    SELECT
        sm.*,
        COALESCE(rr.returnee_ratio, 0) AS returnee_ratio
    FROM season_misc sm
    LEFT JOIN season_returnee_ratio rr ON rr.season_key = sm.season_key
),
jury_support AS (
    SELECT
        finalist_castaway_key AS castaway_key,
        season_key,
        COUNT(*) AS jury_votes_received
    FROM silver.fact_jury_votes
    GROUP BY finalist_castaway_key, season_key
),
jury_original_tribe AS (
    SELECT
        finalist.castaway_key,
        finalist.season_key,
        CASE
            WHEN COUNT(*) = 0 THEN NULL
            ELSE
                SUM(CASE WHEN juror_bc.original_tribe = finalist.original_tribe THEN 1 ELSE 0 END)::NUMERIC
                / NULLIF(COUNT(*), 0)
        END AS jury_original_tribe_proportion
    FROM silver.fact_jury_votes jv
    JOIN silver.bridge_castaway_season finalist ON finalist.castaway_key = jv.finalist_castaway_key AND finalist.season_key = jv.season_key
    JOIN silver.bridge_castaway_season juror_bc ON juror_bc.castaway_key = jv.juror_castaway_key AND juror_bc.season_key = jv.season_key
    GROUP BY finalist.castaway_key, finalist.season_key, finalist.original_tribe
),
episode_confessionals AS (
    SELECT castaway_key, season_key, episode_key,
           SUM(confessional_count) AS confessional_count,
           SUM(confessional_time) AS confessional_time
    FROM silver.fact_confessionals
    GROUP BY castaway_key, season_key, episode_key
),
episode_challenges AS (
    SELECT castaway_key, season_key, episode_key,
           COUNT(*) FILTER (WHERE LOWER(result) LIKE 'win%') AS wins,
           SUM(CASE WHEN chosen_for_reward THEN 1 ELSE 0 END) AS rewards,
           SUM(CASE WHEN sit_out THEN 1 ELSE 0 END) AS sitouts
    FROM silver.fact_challenge_results
    GROUP BY castaway_key, season_key, episode_key
),
episode_votes_cast AS (
    SELECT castaway_key, season_key, episode_key,
           COUNT(*) AS votes_cast,
           COUNT(*) FILTER (WHERE voted_out_castaway_id = target_castaway_id) AS votes_correct,
           COUNT(*) FILTER (WHERE voted_out_castaway_id IS DISTINCT FROM target_castaway_id OR voted_out_castaway_id IS NULL) AS votes_incorrect
    FROM silver.fact_vote_history
    GROUP BY castaway_key, season_key, episode_key
),
episode_votes_received AS (
    SELECT dc.castaway_key, fvh.season_key, fvh.episode_key,
           COUNT(*) AS votes_received
    FROM silver.fact_vote_history fvh
    JOIN silver.dim_castaway dc ON dc.castaway_id = fvh.target_castaway_id
    GROUP BY dc.castaway_key, fvh.season_key, fvh.episode_key
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
    JOIN silver.dim_episode de ON de.episode_key = eu.episode_key
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

INSERT INTO gold.castaway_season_features (
    snapshot_id,
    castaway_key,
    season_key,
    castaway_id,
    version_season,
    feature_payload
)
SELECT
    snap.snapshot_id,
    cb.castaway_key,
    cb.season_key,
    cb.castaway_id,
    cb.version_season,
    jsonb_build_object(
        'profile', jsonb_build_object(
            'full_name', cb.full_name,
            'gender', cb.gender,
            'race', cb.race,
            'ethnicity', cb.ethnicity,
            'occupation', cb.occupation,
            'personality_type', cb.personality_type,
            'age', to_jsonb(ca.first_recorded_age),
            'returning_player', cb.is_returning_player
        ),
        'challenges', jsonb_build_object(
            'wins_total', COALESCE(ct.challenges_won_total, 0),
            'wins_individual', COALESCE(ct.challenges_won_individual, 0),
            'wins_team', COALESCE(ct.challenges_won_team, 0),
            'wins_by_type', COALESCE(cwt.wins_by_type, '{}'::jsonb),
            'chosen_for_reward', COALESCE(ct.chosen_for_reward_count, 0),
            'sit_outs', COALESCE(ct.sit_out_count, 0)
        ),
        'advantages', jsonb_build_object(
            'advantages_found', COALESCE(at.advantages_found, 0),
            'advantages_played', COALESCE(at.advantages_played, 0),
            'idols_played_correctly', COALESCE(at.idols_played_correctly, 0),
            'idols_played_incorrectly', COALESCE(at.idols_played_incorrectly, 0),
            'idols_played_for_self', COALESCE(at.idols_played_for_self, 0),
            'idols_played_for_others', COALESCE(at.idols_played_for_others, 0)
        ),
        'vote_history', jsonb_build_object(
            'total_votes_cast', COALESCE(vt.total_votes_cast, 0),
            'total_votes_received', COALESCE(vr.total_votes_received, 0),
            'votes_in_majority', COALESCE(vt.votes_correct, 0),
            'votes_incorrect', COALESCE(vt.votes_incorrect, 0),
            'tribal_councils_attended', COALESCE(vt.tribal_councils_attended, 0),
            'tribal_councils_pre_merge', COALESCE(vt.tribal_councils_pre_merge, 0),
            'tribal_councils_post_merge', COALESCE(vt.tribal_councils_post_merge, 0),
            'average_vote_alignment', COALESCE(va.avg_vote_alignment, 0),
            'merge_vote_correct', mv.merge_vote_correct
        ),
        'tv_stats', jsonb_build_object(
            'confessional_count_total', COALESCE(ctv.total_confessional_count, 0),
            'confessional_time_total', COALESCE(ctv.total_confessional_time, 0)
        ),
        'misc', jsonb_build_object(
            'tribe_setup', cb.tribe_setup,
            'average_female_ratio', COALESCE(tb.avg_female_ratio, 0),
            'average_male_ratio', COALESCE(tb.avg_male_ratio, 0),
            'returnee_ratio', COALESCE(sm.returnee_ratio, 0),
            'season_weight', COALESCE(sm.season_weight, 1),
            'is_new_era', sm.is_new_era,
            'twist_edge_of_extinction', sm.twist_edge_of_extinction,
            'twist_redemption_island', sm.twist_redemption_island,
            'original_tribe_post_merge', COALESCE(pm.original_tribe_post_merge, 0),
            'tribals_attended_pre_merge', COALESCE(vt.tribal_councils_pre_merge, 0)
        ),
        'jury', jsonb_build_object(
            'jury_votes_received', COALESCE(js.jury_votes_received, 0),
            'jury_original_tribe_proportion', COALESCE(jot.jury_original_tribe_proportion, 0)
        )
    )
FROM castaway_base cb
CROSS JOIN active_snapshot snap
LEFT JOIN castaway_age ca ON ca.castaway_id = cb.castaway_id AND ca.version_season = cb.version_season
LEFT JOIN challenge_totals ct ON ct.castaway_key = cb.castaway_key AND ct.season_key = cb.season_key
LEFT JOIN challenge_wins_by_type cwt ON cwt.castaway_key = cb.castaway_key AND cwt.season_key = cb.season_key
LEFT JOIN advantage_totals at ON at.castaway_key = cb.castaway_key AND at.season_key = cb.season_key
LEFT JOIN vote_totals vt ON vt.castaway_key = cb.castaway_key AND vt.season_key = cb.season_key
LEFT JOIN votes_received vr ON vr.castaway_key = cb.castaway_key AND vr.season_key = cb.season_key
LEFT JOIN vote_alignment va ON va.castaway_key = cb.castaway_key AND va.season_key = cb.season_key
LEFT JOIN merge_vote_status mv ON mv.castaway_key = cb.castaway_key AND mv.season_key = cb.season_key
LEFT JOIN confessional_totals ctv ON ctv.castaway_key = cb.castaway_key AND ctv.season_key = cb.season_key
LEFT JOIN tribe_balance tb ON tb.castaway_key = cb.castaway_key AND tb.season_key = cb.season_key
LEFT JOIN post_merge_original_counts pm ON pm.castaway_key = cb.castaway_key AND pm.season_key = cb.season_key
LEFT JOIN season_misc_enriched sm ON sm.season_key = cb.season_key
LEFT JOIN jury_support js ON js.castaway_key = cb.castaway_key AND js.season_key = cb.season_key
LEFT JOIN jury_original_tribe jot ON jot.castaway_key = cb.castaway_key AND jot.season_key = cb.season_key;

INSERT INTO gold.castaway_episode_features (
    snapshot_id,
    castaway_key,
    season_key,
    episode_key,
    castaway_id,
    version_season,
    episode_in_season,
    feature_payload
)
SELECT
    snap.snapshot_id,
    cb.castaway_key,
    efee.season_key,
    efee.episode_key,
    cb.castaway_id,
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
    )
FROM episode_features_enriched efee
JOIN castaway_base cb ON cb.castaway_key = efee.castaway_key AND cb.season_key = efee.season_key
CROSS JOIN active_snapshot snap;

INSERT INTO gold.season_features (
    snapshot_id,
    season_key,
    version_season,
    feature_payload
)
SELECT
    snap.snapshot_id,
    sm.season_key,
    sm.version_season,
    jsonb_build_object(
        'season_number', sm.season_number,
        'tribe_setup', sm.tribe_setup,
        'cast_size', sm.cast_size,
        'finalist_count', sm.finalist_count,
        'jury_count', sm.jury_count,
        'season_weight', sm.season_weight,
        'is_new_era', sm.is_new_era,
        'twist_edge_of_extinction', sm.twist_edge_of_extinction,
        'twist_redemption_island', sm.twist_redemption_island,
        'returnee_ratio', sm.returnee_ratio
    )
FROM season_misc_enriched sm
CROSS JOIN active_snapshot snap;
