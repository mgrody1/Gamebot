-- Refresh Survivor medallion dimension tables from the bronze layer.

INSERT INTO silver.dim_castaway (
    castaway_id,
    full_name,
    gender,
    date_of_birth,
    date_of_death,
    collar,
    occupation,
    personality_type,
    race,
    ethnicity,
    is_african,
    is_asian,
    is_latin_american,
    is_native_american,
    is_bipoc,
    is_lgbt
)
SELECT
    cd.castaway_id,
    cd.full_name,
    cd.gender,
    cd.date_of_birth,
    cd.date_of_death,
    cd.collar,
    cd.occupation,
    cd.personality_type,
    cd.race,
    cd.ethnicity,
    cd.african,
    cd.asian,
    cd.latin_american,
    cd.native_american,
    cd.bipoc,
    cd.lgbt
FROM bronze.castaway_details cd
ON CONFLICT (castaway_id) DO UPDATE SET
    full_name = EXCLUDED.full_name,
    gender = EXCLUDED.gender,
    date_of_birth = EXCLUDED.date_of_birth,
    date_of_death = EXCLUDED.date_of_death,
    collar = EXCLUDED.collar,
    occupation = EXCLUDED.occupation,
    personality_type = EXCLUDED.personality_type,
    race = EXCLUDED.race,
    ethnicity = EXCLUDED.ethnicity,
    is_african = EXCLUDED.is_african,
    is_asian = EXCLUDED.is_asian,
    is_latin_american = EXCLUDED.is_latin_american,
    is_native_american = EXCLUDED.is_native_american,
    is_bipoc = EXCLUDED.is_bipoc,
    is_lgbt = EXCLUDED.is_lgbt,
    updated_at = NOW();


INSERT INTO silver.dim_season (
    version,
    version_season,
    season_name,
    season_number,
    location,
    country,
    tribe_setup,
    cast_size,
    tribe_count,
    finalist_count,
    jury_count,
    premiered,
    ended,
    filming_started,
    filming_ended,
    winner_castaway_id,
    viewers_reunion,
    viewers_premiere,
    viewers_finale,
    viewers_mean,
    rank
)
SELECT
    ss.version,
    ss.version_season,
    ss.season_name,
    ss.season,
    ss.location,
    ss.country,
    ss.tribe_setup,
    ss.n_cast,
    ss.n_tribes,
    ss.n_finalists,
    ss.n_jury,
    ss.premiered,
    ss.ended,
    ss.filming_started,
    ss.filming_ended,
    ss.winner_id,
    ss.viewers_reunion,
    ss.viewers_premiere,
    ss.viewers_finale,
    ss.viewers_mean,
    ss.rank
FROM bronze.season_summary ss
ON CONFLICT (version_season) DO UPDATE SET
    version = EXCLUDED.version,
    season_name = EXCLUDED.season_name,
    season_number = EXCLUDED.season_number,
    location = EXCLUDED.location,
    country = EXCLUDED.country,
    tribe_setup = EXCLUDED.tribe_setup,
    cast_size = EXCLUDED.cast_size,
    tribe_count = EXCLUDED.tribe_count,
    finalist_count = EXCLUDED.finalist_count,
    jury_count = EXCLUDED.jury_count,
    premiered = EXCLUDED.premiered,
    ended = EXCLUDED.ended,
    filming_started = EXCLUDED.filming_started,
    filming_ended = EXCLUDED.filming_ended,
    winner_castaway_id = EXCLUDED.winner_castaway_id,
    viewers_reunion = EXCLUDED.viewers_reunion,
    viewers_premiere = EXCLUDED.viewers_premiere,
    viewers_finale = EXCLUDED.viewers_finale,
    viewers_mean = EXCLUDED.viewers_mean,
    rank = EXCLUDED.rank,
    updated_at = NOW();


INSERT INTO silver.dim_episode (
    episode_key,
    version_season,
    episode_in_season,
    episode_title,
    episode_date,
    episode_description,
    total_episode_count,
    previous_episode_key,
    next_episode_key
)
SELECT
    CONCAT(ss.version_season, '-', LPAD(ep.episode::text, 2, '0')) AS episode_key,
    ss.version_season,
    ep.episode,
    ep.episode_title,
    ep.episode_date,
    ep.episode_summary,
    ep.total_episode_count,
    CASE
        WHEN ep.episode > 1 THEN CONCAT(ss.version_season, '-', LPAD((ep.episode - 1)::text, 2, '0'))
        ELSE NULL
    END AS previous_episode_key,
    CASE
        WHEN ep.episode < ep.total_episode_count THEN CONCAT(ss.version_season, '-', LPAD((ep.episode + 1)::text, 2, '0'))
        ELSE NULL
    END AS next_episode_key
FROM bronze.episodes ep
JOIN bronze.season_summary ss
  ON ss.version_season = ep.version_season
ON CONFLICT (episode_key) DO UPDATE SET
    version_season = EXCLUDED.version_season,
    episode_in_season = EXCLUDED.episode_in_season,
    episode_title = EXCLUDED.episode_title,
    episode_date = EXCLUDED.episode_date,
    episode_description = EXCLUDED.episode_description,
    total_episode_count = EXCLUDED.total_episode_count,
    previous_episode_key = EXCLUDED.previous_episode_key,
    next_episode_key = EXCLUDED.next_episode_key,
    updated_at = NOW();


INSERT INTO silver.dim_challenge (
    challenge_key,
    challenge_id,
    version_season,
    challenge_name,
    recurring_name,
    challenge_type,
    short_description,
    long_description,
    location,
    season_episode_key,
    reward,
    immunity,
    reward_details,
    immunity_details,
    source_challenge_id
)
SELECT
    CONCAT(cd.version_season, '-', cd.challenge_id) AS challenge_key,
    cd.challenge_id,
    cd.version_season,
    cd.challenge_name,
    cd.recurring_name,
    cd.challenge_type,
    cd.short_description,
    cd.long_description,
    cd.location,
    CONCAT(cd.version_season, '-', LPAD(cd.episode::text, 2, '0')) AS season_episode_key,
    cd.reward,
    cd.immunity,
    cd.reward_details,
    cd.immunity_details,
    cd.challenge_id
FROM bronze.challenge_description cd
ON CONFLICT (challenge_key) DO UPDATE SET
    challenge_id = EXCLUDED.challenge_id,
    version_season = EXCLUDED.version_season,
    challenge_name = EXCLUDED.challenge_name,
    recurring_name = EXCLUDED.recurring_name,
    challenge_type = EXCLUDED.challenge_type,
    short_description = EXCLUDED.short_description,
    long_description = EXCLUDED.long_description,
    location = EXCLUDED.location,
    season_episode_key = EXCLUDED.season_episode_key,
    reward = EXCLUDED.reward,
    immunity = EXCLUDED.immunity,
    reward_details = EXCLUDED.reward_details,
    immunity_details = EXCLUDED.immunity_details,
    source_challenge_id = EXCLUDED.source_challenge_id,
    updated_at = NOW();


INSERT INTO silver.dim_advantage (
    advantage_key,
    advantage_id,
    version_season,
    advantage_name,
    advantage_type,
    advantage_subtype,
    first_appearance_episode_key,
    reentry_episode_key,
    notes,
    source_advantage_id
)
SELECT
    CONCAT(ad.version_season, '-', ad.advantage_id) AS advantage_key,
    ad.advantage_id,
    ad.version_season,
    ad.advantage_name,
    ad.advantage_type,
    ad.advantage_subtype,
    CONCAT(ad.version_season, '-', LPAD(ad.first_ep::text, 2, '0')) AS first_appearance_episode_key,
    CONCAT(ad.version_season, '-', LPAD(ad.reentry_ep::text, 2, '0')) AS reentry_episode_key,
    ad.notes,
    ad.advantage_id
FROM bronze.advantage_details ad
ON CONFLICT (advantage_key) DO UPDATE SET
    advantage_id = EXCLUDED.advantage_id,
    version_season = EXCLUDED.version_season,
    advantage_name = EXCLUDED.advantage_name,
    advantage_type = EXCLUDED.advantage_type,
    advantage_subtype = EXCLUDED.advantage_subtype,
    first_appearance_episode_key = EXCLUDED.first_appearance_episode_key,
    reentry_episode_key = EXCLUDED.reentry_episode_key,
    notes = EXCLUDED.notes,
    source_advantage_id = EXCLUDED.source_advantage_id,
    updated_at = NOW();


WITH skill_flags AS (
    SELECT
        dc.challenge_key,
        jsonb_build_object(
            'balance', cd.balance,
            'balance_ball', cd.balance_ball,
            'balance_beam', cd.balance_beam,
            'endurance', cd.endurance,
            'fire', cd.fire,
            'food', cd.food,
            'knowledge', cd.knowledge,
            'memory', cd.memory,
            'mud', cd.mud,
            'obstacle_blindfolded', cd.obstacle_blindfolded,
            'obstacle_cargo_net', cd.obstacle_cargo_net,
            'obstacle_chopping', cd.obstacle_chopping,
            'obstacle_combination_lock', cd.obstacle_combination_lock,
            'obstacle_digging', cd.obstacle_digging,
            'obstacle_knots', cd.obstacle_knots,
            'obstacle_padlocks', cd.obstacle_padlocks,
            'precision', cd.precision,
            'precision_catch', cd.precision_catch,
            'precision_roll_ball', cd.precision_roll_ball,
            'precision_slingshot', cd.precision_slingshot,
            'precision_throw_balls', cd.precision_throw_balls,
            'precision_throw_coconuts', cd.precision_throw_coconuts,
            'precision_throw_rings', cd.precision_throw_rings,
            'precision_throw_sandbags', cd.precision_throw_sandbags,
            'puzzle', cd.puzzle,
            'puzzle_slide', cd.puzzle_slide,
            'puzzle_word', cd.puzzle_word,
            'race', cd.race,
            'strength', cd.strength,
            'turn_based', cd.turn_based,
            'water', cd.water,
            'water_paddling', cd.water_paddling,
            'water_swim', cd.water_swim
        ) AS flag_map
    FROM silver.dim_challenge dc
    JOIN bronze.challenge_description cd
      ON cd.version_season = dc.version_season
     AND cd.challenge_id = dc.challenge_id
),
active_skills AS (
    SELECT DISTINCT
        sf.challenge_key,
        flag.skill_name
    FROM skill_flags sf
         CROSS JOIN LATERAL jsonb_each_text(sf.flag_map) AS flag(skill_name, flag_value)
    WHERE flag_value = 'true'
)
INSERT INTO silver.challenge_skill_lookup (skill_name)
SELECT DISTINCT skill_name
FROM active_skills
ON CONFLICT (skill_name) DO NOTHING;


DELETE FROM silver.challenge_skill_bridge;

INSERT INTO silver.challenge_skill_bridge (challenge_key, skill_key)
SELECT
    a.challenge_key,
    l.skill_key
FROM active_skills a
JOIN silver.challenge_skill_lookup l
  ON l.skill_name = a.skill_name
ON CONFLICT DO NOTHING;


INSERT INTO silver.bridge_castaway_season (
    castaway_key,
    season_key,
    castaway_id,
    version_season,
    original_tribe,
    result,
    place,
    jury_status,
    jury,
    finalist,
    winner,
    acknowledge,
    ack_look,
    ack_speak,
    ack_gesture,
    ack_smile,
    ack_quote,
    ack_score
)
SELECT
    dc.castaway_key,
    ds.season_key,
    c.castaway_id,
    c.version_season,
    c.original_tribe,
    c.result,
    c.place,
    c.jury_status,
    c.jury,
    c.finalist,
    c.winner,
    c.acknowledge,
    c.ack_look,
    c.ack_speak,
    c.ack_gesture,
    c.ack_smile,
    c.ack_quote,
    c.ack_score
FROM bronze.castaways c
JOIN silver.dim_castaway dc
  ON dc.castaway_id = c.castaway_id
JOIN silver.dim_season ds
  ON ds.version_season = c.version_season
ON CONFLICT (castaway_key, season_key) DO UPDATE SET
    castaway_id = EXCLUDED.castaway_id,
    version_season = EXCLUDED.version_season,
    original_tribe = EXCLUDED.original_tribe,
    result = EXCLUDED.result,
    place = EXCLUDED.place,
    jury_status = EXCLUDED.jury_status,
    jury = EXCLUDED.jury,
    finalist = EXCLUDED.finalist,
    winner = EXCLUDED.winner,
    acknowledge = EXCLUDED.acknowledge,
    ack_look = EXCLUDED.ack_look,
    ack_speak = EXCLUDED.ack_speak,
    ack_gesture = EXCLUDED.ack_gesture,
    ack_smile = EXCLUDED.ack_smile,
    ack_quote = EXCLUDED.ack_quote,
    ack_score = EXCLUDED.ack_score,
    updated_at = NOW();
