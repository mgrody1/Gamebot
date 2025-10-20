-- Refresh Survivor medallion fact tables from the bronze layer.

TRUNCATE TABLE silver.fact_confessionals RESTART IDENTITY;
TRUNCATE TABLE silver.fact_challenge_results RESTART IDENTITY;
TRUNCATE TABLE silver.fact_vote_history RESTART IDENTITY;
TRUNCATE TABLE silver.fact_advantage_movement RESTART IDENTITY;
TRUNCATE TABLE silver.fact_boot_mapping RESTART IDENTITY;
TRUNCATE TABLE silver.fact_tribe_membership RESTART IDENTITY;
TRUNCATE TABLE silver.fact_jury_votes RESTART IDENTITY;


INSERT INTO silver.fact_confessionals (
    castaway_key,
    season_key,
    episode_key,
    castaway_id,
    version_season,
    episode_in_season,
    confessional_count,
    confessional_time,
    expected_count,
    expected_time,
    source_confessional_id
)
SELECT
    dc.castaway_key,
    ds.season_key,
    de.episode_key,
    cf.castaway_id,
    cf.version_season,
    cf.episode,
    cf.confessional_count,
    cf.confessional_time,
    cf.exp_count,
    cf.exp_time,
    cf.confessional_id
FROM bronze.confessionals cf
JOIN silver.dim_castaway dc
  ON dc.castaway_id = cf.castaway_id
JOIN silver.dim_season ds
  ON ds.version_season = cf.version_season
LEFT JOIN silver.dim_episode de
  ON de.version_season = cf.version_season
 AND de.episode_in_season = cf.episode;


INSERT INTO silver.fact_challenge_results (
    castaway_key,
    season_key,
    episode_key,
    challenge_key,
    advantage_key,
    castaway_id,
    version_season,
    challenge_id,
    sog_id,
    outcome_type,
    result,
    result_notes,
    chosen_for_reward,
    sit_out,
    order_of_finish,
    source_challenge_result_id
)
SELECT
    dc.castaway_key,
    ds.season_key,
    de.episode_key,
    dch.challenge_key,
    NULL,
    cr.castaway_id,
    cr.version_season,
    cr.challenge_id,
    cr.sog_id,
    cr.outcome_type,
    cr.result,
    cr.result_notes,
    cr.chosen_for_reward,
    cr.sit_out,
    cr.order_of_finish,
    cr.challenge_results_id
FROM bronze.challenge_results cr
JOIN silver.dim_castaway dc
  ON dc.castaway_id = cr.castaway_id
JOIN silver.dim_season ds
  ON ds.version_season = cr.version_season
LEFT JOIN silver.dim_episode de
  ON de.version_season = cr.version_season
 AND de.episode_in_season = cr.episode
LEFT JOIN silver.dim_challenge dch
  ON dch.version_season = cr.version_season
 AND dch.challenge_id = cr.challenge_id;


INSERT INTO silver.fact_vote_history (
    castaway_key,
    season_key,
    episode_key,
    challenge_key,
    castaway_id,
    target_castaway_id,
    voted_out_castaway_id,
    version_season,
    episode_in_season,
    immunity,
    vote,
    vote_event,
    vote_event_outcome,
    split_vote,
    nullified,
    tie,
    vote_order,
    sog_id,
    source_vote_history_id
)
SELECT
    dc.castaway_key,
    ds.season_key,
    de.episode_key,
    dch.challenge_key,
    vh.castaway_id,
    vh.vote_id,
    vh.voted_out_id,
    vh.version_season,
    vh.episode,
    vh.immunity,
    vh.vote,
    vh.vote_event,
    vh.vote_event_outcome,
    vh.split_vote,
    vh.nullified,
    vh.tie,
    vh.vote_order,
    vh.sog_id,
    vh.vote_history_id
FROM bronze.vote_history vh
JOIN silver.dim_castaway dc
  ON dc.castaway_id = vh.castaway_id
JOIN silver.dim_season ds
  ON ds.version_season = vh.version_season
LEFT JOIN silver.dim_episode de
  ON de.version_season = vh.version_season
 AND de.episode_in_season = vh.episode
LEFT JOIN silver.dim_challenge dch
  ON dch.version_season = vh.version_season
 AND dch.challenge_id = vh.challenge_id;


INSERT INTO silver.fact_advantage_movement (
    castaway_key,
    target_castaway_key,
    season_key,
    episode_key,
    advantage_key,
    castaway_id,
    target_castaway_id,
    version_season,
    sequence_id,
    advantage_id,
    day,
    episode_in_season,
    event,
    success,
    votes_nullified,
    sog_id,
    source_advantage_movement_id
)
SELECT
    holder.castaway_key,
    target.castaway_key,
    ds.season_key,
    de.episode_key,
    da.advantage_key,
    am.castaway_id,
    am.played_for_id,
    am.version_season,
    am.sequence_id,
    am.advantage_id,
    am.day,
    am.episode,
    am.event,
    am.success,
    am.votes_nullified,
    am.sog_id,
    am.advantage_movement_id
FROM bronze.advantage_movement am
LEFT JOIN silver.dim_castaway holder
  ON holder.castaway_id = am.castaway_id
LEFT JOIN silver.dim_castaway target
  ON target.castaway_id = am.played_for_id
JOIN silver.dim_season ds
  ON ds.version_season = am.version_season
LEFT JOIN silver.dim_episode de
  ON de.version_season = am.version_season
 AND de.episode_in_season = am.episode
JOIN silver.dim_advantage da
  ON da.version_season = am.version_season
 AND da.advantage_id = am.advantage_id;


INSERT INTO silver.fact_boot_mapping (
    castaway_key,
    season_key,
    episode_key,
    castaway_id,
    version_season,
    episode_in_season,
    boot_mapping_order,
    n_boots,
    final_n,
    sog_id,
    tribe,
    tribe_status,
    game_status,
    source_boot_mapping_id
)
SELECT
    dc.castaway_key,
    ds.season_key,
    de.episode_key,
    bm.castaway_id,
    bm.version_season,
    bm.episode,
    bm.boot_mapping_order,
    bm.n_boots,
    bm.final_n,
    bm.sog_id,
    bm.tribe,
    bm.tribe_status,
    bm.game_status,
    bm.boot_mapping_id
FROM bronze.boot_mapping bm
LEFT JOIN silver.dim_castaway dc
  ON dc.castaway_id = bm.castaway_id
JOIN silver.dim_season ds
  ON ds.version_season = bm.version_season
LEFT JOIN silver.dim_episode de
  ON de.version_season = bm.version_season
 AND de.episode_in_season = bm.episode;


INSERT INTO silver.fact_tribe_membership (
    castaway_key,
    season_key,
    episode_key,
    castaway_id,
    version_season,
    episode_in_season,
    day,
    tribe,
    tribe_status,
    source_tribe_mapping_id
)
SELECT
    dc.castaway_key,
    ds.season_key,
    de.episode_key,
    tm.castaway_id,
    tm.version_season,
    tm.episode,
    tm.day,
    tm.tribe,
    tm.tribe_status,
    tm.tribe_map_id
FROM bronze.tribe_mapping tm
LEFT JOIN silver.dim_castaway dc
  ON dc.castaway_id = tm.castaway_id
JOIN silver.dim_season ds
  ON ds.version_season = tm.version_season
LEFT JOIN silver.dim_episode de
  ON de.version_season = tm.version_season
 AND de.episode_in_season = tm.episode;


INSERT INTO silver.fact_jury_votes (
    juror_castaway_key,
    finalist_castaway_key,
    season_key,
    juror_castaway_id,
    finalist_castaway_id,
    version_season,
    vote,
    source_jury_vote_id
)
SELECT
    juror.castaway_key,
    finalist.castaway_key,
    ds.season_key,
    jv.castaway_id,
    jv.finalist_id,
    jv.version_season,
    jv.vote,
    jv.jury_vote_id
FROM bronze.jury_votes jv
LEFT JOIN silver.dim_castaway juror
  ON juror.castaway_id = jv.castaway_id
LEFT JOIN silver.dim_castaway finalist
  ON finalist.castaway_id = jv.finalist_id
JOIN silver.dim_season ds
  ON ds.version_season = jv.version_season;
