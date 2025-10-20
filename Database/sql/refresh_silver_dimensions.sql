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
    season_key,
    version_season,
    episode_in_season,
    episode_number_overall,
    episode_title,
    episode_label,
    episode_date,
    episode_length,
    viewers,
    imdb_rating,
    n_ratings
)
SELECT
    ds.season_key,
    e.version_season,
    e.episode,
    e.episode_number_overall,
    e.episode_title,
    e.episode_label,
    e.episode_date,
    e.episode_length,
    e.viewers,
    e.imdb_rating,
    e.n_ratings
FROM bronze.episodes e
JOIN silver.dim_season ds
  ON ds.version_season = e.version_season
ON CONFLICT (version_season, episode_in_season) DO UPDATE SET
    season_key = EXCLUDED.season_key,
    episode_number_overall = EXCLUDED.episode_number_overall,
    episode_title = EXCLUDED.episode_title,
    episode_label = EXCLUDED.episode_label,
    episode_date = EXCLUDED.episode_date,
    episode_length = EXCLUDED.episode_length,
    viewers = EXCLUDED.viewers,
    imdb_rating = EXCLUDED.imdb_rating,
    n_ratings = EXCLUDED.n_ratings,
    updated_at = NOW();


INSERT INTO silver.dim_advantage (
    version_season,
    advantage_id,
    advantage_type,
    clue_details,
    location_found,
    conditions
)
SELECT
    ad.version_season,
    ad.advantage_id,
    ad.advantage_type,
    ad.clue_details,
    ad.location_found,
    ad.conditions
FROM bronze.advantage_details ad
ON CONFLICT (version_season, advantage_id) DO UPDATE SET
    advantage_type = EXCLUDED.advantage_type,
    clue_details = EXCLUDED.clue_details,
    location_found = EXCLUDED.location_found,
    conditions = EXCLUDED.conditions,
    updated_at = NOW();


INSERT INTO silver.dim_challenge (
    version_season,
    challenge_id,
    episode_in_season,
    challenge_number,
    challenge_type,
    name,
    recurring_name,
    description,
    reward,
    additional_stipulation
)
SELECT
    cd.version_season,
    cd.challenge_id,
    cd.episode,
    cd.challenge_number,
    cd.challenge_type,
    cd.name,
    cd.recurring_name,
    cd.description,
    cd.reward,
    cd.additional_stipulation
FROM bronze.challenge_description cd
ON CONFLICT (version_season, challenge_id) DO UPDATE SET
    episode_in_season = EXCLUDED.episode_in_season,
    challenge_number = EXCLUDED.challenge_number,
    challenge_type = EXCLUDED.challenge_type,
    name = EXCLUDED.name,
    recurring_name = EXCLUDED.recurring_name,
    description = EXCLUDED.description,
    reward = EXCLUDED.reward,
    additional_stipulation = EXCLUDED.additional_stipulation,
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
    result_number,
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
    c.result_number,
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
    result_number = EXCLUDED.result_number,
    acknowledge = EXCLUDED.acknowledge,
    ack_look = EXCLUDED.ack_look,
    ack_speak = EXCLUDED.ack_speak,
    ack_gesture = EXCLUDED.ack_gesture,
    ack_smile = EXCLUDED.ack_smile,
    ack_quote = EXCLUDED.ack_quote,
    ack_score = EXCLUDED.ack_score,
    updated_at = NOW();
