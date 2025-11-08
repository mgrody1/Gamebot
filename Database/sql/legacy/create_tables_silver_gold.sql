-- Survivor Prediction Warehouse DDL
-- Implements a Medallion (bronze/silver/gold) architecture for the survivoR dataset.

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================================
-- Bronze Layer metadata
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.ingestion_runs (
    run_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    environment TEXT NOT NULL,
    git_branch TEXT,
    git_commit TEXT,
    source_url TEXT,
    run_started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    run_finished_at TIMESTAMPTZ,
    status TEXT DEFAULT 'running',
    notes TEXT
);

CREATE TABLE IF NOT EXISTS bronze.dataset_versions (
    dataset_name TEXT PRIMARY KEY,
    signature TEXT,
    commit_sha TEXT,
    commit_url TEXT,
    committed_at TIMESTAMPTZ,
    source_type TEXT,
    last_ingest_run_id UUID REFERENCES bronze.ingestion_runs(run_id),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- Bronze Layer: Raw, schema-on-write copies of the survivoR datasets
-- ============================================================================

CREATE TABLE IF NOT EXISTS bronze.castaway_details (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    castaway_id TEXT PRIMARY KEY,
    full_name TEXT,
    full_name_detailed TEXT,
    castaway TEXT,
    last_name TEXT,
    collar TEXT,
    date_of_birth DATE,
    date_of_death DATE,
    gender TEXT,
    african BOOLEAN,
    asian BOOLEAN,
    latin_american BOOLEAN,
    native_american BOOLEAN,
    bipoc BOOLEAN,
    lgbt BOOLEAN,
    personality_type TEXT,
    occupation TEXT,
    three_words TEXT,
    hobbies TEXT,
    pet_peeves TEXT,
    race TEXT,
    ethnicity TEXT,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_castaway_details_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS bronze.season_summary (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    version TEXT NOT NULL,
    version_season TEXT PRIMARY KEY,
    season_name TEXT,
    season INT,
    location TEXT,
    country TEXT,
    tribe_setup TEXT,
    n_cast INT,
    n_tribes INT,
    n_finalists INT,
    n_jury INT,
    full_name TEXT,
    winner_id TEXT,
    winner TEXT,
    runner_ups TEXT,
    final_vote TEXT,
    timeslot TEXT,
    premiered DATE,
    ended DATE,
    filming_started DATE,
    filming_ended DATE,
    viewers_reunion DOUBLE PRECISION,
    viewers_premiere INT,
    viewers_finale INT,
    viewers_mean DOUBLE PRECISION,
    rank INT,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_season_positive CHECK (season IS NULL OR season > 0),
    CONSTRAINT fk_season_summary_winner FOREIGN KEY (winner_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_season_summary_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS bronze.advantage_details (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    version TEXT NOT NULL,
    version_season TEXT NOT NULL,
    season INT,
    advantage_id INT NOT NULL,
    advantage_type TEXT,
    clue_details TEXT,
    location_found TEXT,
    conditions TEXT,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_advantage_details PRIMARY KEY (version_season, advantage_id),
    CONSTRAINT fk_advantage_details_season FOREIGN KEY (version_season) REFERENCES bronze.season_summary (version_season),
    CONSTRAINT fk_advantage_details_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS bronze.challenge_description (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    version TEXT NOT NULL,
    version_season TEXT NOT NULL,
    season INT,
    episode INT,
    challenge_id INT NOT NULL,
    challenge_number INT,
    challenge_type TEXT,
    name TEXT,
    recurring_name TEXT,
    description TEXT,
    reward TEXT,
    additional_stipulation TEXT,
    balance BOOLEAN,
    balance_ball BOOLEAN,
    balance_beam BOOLEAN,
    endurance BOOLEAN,
    fire BOOLEAN,
    food BOOLEAN,
    knowledge BOOLEAN,
    memory BOOLEAN,
    mud BOOLEAN,
    obstacle_blindfolded BOOLEAN,
    obstacle_cargo_net BOOLEAN,
    obstacle_chopping BOOLEAN,
    obstacle_combination_lock BOOLEAN,
    obstacle_digging BOOLEAN,
    obstacle_knots BOOLEAN,
    obstacle_padlocks BOOLEAN,
    precision BOOLEAN,
    precision_catch BOOLEAN,
    precision_roll_ball BOOLEAN,
    precision_slingshot BOOLEAN,
    precision_throw_balls BOOLEAN,
    precision_throw_coconuts BOOLEAN,
    precision_throw_rings BOOLEAN,
    precision_throw_sandbags BOOLEAN,
    puzzle BOOLEAN,
    puzzle_slide BOOLEAN,
    puzzle_word BOOLEAN,
    race BOOLEAN,
    strength BOOLEAN,
    turn_based BOOLEAN,
    water BOOLEAN,
    water_paddling BOOLEAN,
    water_swim BOOLEAN,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_challenge_description PRIMARY KEY (version_season, challenge_id),
    CONSTRAINT fk_challenge_description_season FOREIGN KEY (version_season) REFERENCES bronze.season_summary (version_season),
    CONSTRAINT fk_challenge_description_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS bronze.challenge_summary (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    category TEXT,
    version TEXT NOT NULL,
    version_season TEXT NOT NULL,
    season INT,
    episode INT,
    challenge_id INT NOT NULL,
    challenge_type TEXT,
    outcome_type TEXT,
    tribe TEXT,
    castaway_id TEXT,
    castaway TEXT,
    n_entities INT,
    n_winners INT,
    won DOUBLE PRECISION,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Intentionally omit UNIQUE constraint: upstream categorises the same outcome across multiple analytic buckets.
    CONSTRAINT fk_challenge_summary_season FOREIGN KEY (version_season) REFERENCES bronze.season_summary (version_season),
    CONSTRAINT fk_challenge_summary_castaway FOREIGN KEY (castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_challenge_summary_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS bronze.episodes (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    version TEXT NOT NULL,
    version_season TEXT NOT NULL,
    season INT,
    episode_number_overall INT,
    episode INT NOT NULL,
    episode_title TEXT,
    episode_label TEXT,
    episode_date DATE,
    episode_length DOUBLE PRECISION,
    viewers DOUBLE PRECISION,
    imdb_rating DOUBLE PRECISION,
    n_ratings INT,
    episode_summary TEXT,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT pk_episodes PRIMARY KEY (version_season, episode),
    CONSTRAINT fk_episodes_season FOREIGN KEY (version_season) REFERENCES bronze.season_summary (version_season),
    CONSTRAINT fk_episodes_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS bronze.castaways (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    castaway_unique_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version TEXT NOT NULL,
    version_season TEXT NOT NULL,
    season INT,
    full_name TEXT,
    castaway_id TEXT NOT NULL,
    castaway TEXT,
    age INT,
    city TEXT,
    state TEXT,
    episode INT,
    day INT,
    castaways_order INT,
    result TEXT,
    place TEXT,
    jury_status TEXT,
    original_tribe TEXT,
    jury BOOLEAN,
    finalist BOOLEAN,
    winner BOOLEAN,
    acknowledge BOOLEAN,
    ack_look BOOLEAN,
    ack_speak BOOLEAN,
    ack_gesture BOOLEAN,
    ack_smile BOOLEAN,
    ack_quote TEXT,
    ack_score INT,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_castaways UNIQUE (castaway_id, version_season),
    CONSTRAINT fk_castaways_season FOREIGN KEY (version_season) REFERENCES bronze.season_summary (version_season),
    CONSTRAINT fk_castaways_castaway FOREIGN KEY (castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_castaways_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS bronze.advantage_movement (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    advantage_movement_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version TEXT NOT NULL,
    version_season TEXT NOT NULL,
    season INT,
    castaway TEXT,
    castaway_id TEXT,
    advantage_id INT,
    sequence_id INT,
    day INT,
    episode INT,
    event TEXT,
    played_for TEXT,
    played_for_id TEXT,
    co_castaway_ids TEXT,
    joint_play BOOLEAN DEFAULT FALSE,
    multi_target_play BOOLEAN DEFAULT FALSE,
    success TEXT,
    votes_nullified DOUBLE PRECISION,
    sog_id INT,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_advantage_movement UNIQUE (version_season, castaway_id, advantage_id, sequence_id, played_for_id),
    CONSTRAINT fk_advantage_movement_season FOREIGN KEY (version_season) REFERENCES bronze.season_summary (version_season),
    CONSTRAINT fk_advantage_movement_advantage FOREIGN KEY (version_season, advantage_id) REFERENCES bronze.advantage_details (version_season, advantage_id),
    CONSTRAINT fk_advantage_movement_holder FOREIGN KEY (castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_advantage_movement_target FOREIGN KEY (played_for_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_advantage_movement_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS bronze.boot_mapping (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    boot_mapping_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version TEXT NOT NULL,
    version_season TEXT NOT NULL,
    season INT,
    episode INT,
    boot_mapping_order INT,
    n_boots INT,
    final_n INT,
    sog_id INT,
    castaway_id TEXT,
    castaway TEXT,
    tribe TEXT,
    tribe_status TEXT,
    game_status TEXT,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_boot_mapping UNIQUE (sog_id, castaway_id, version_season),
    CONSTRAINT fk_boot_mapping_season FOREIGN KEY (version_season) REFERENCES bronze.season_summary (version_season),
    CONSTRAINT fk_boot_mapping_castaway FOREIGN KEY (castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_boot_mapping_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS bronze.boot_order (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    boot_order_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version TEXT NOT NULL,
    version_season TEXT NOT NULL,
    season INT,
    castaway_id TEXT NOT NULL,
    castaway TEXT,
    episode INT,
    day INT,
    boot_order_position INT,
    result TEXT,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Upstream publishes multiple rows per castaway when they return, and order/boot_order_position can be null mid-season.
    CONSTRAINT uq_boot_order UNIQUE (version_season, boot_order_position, castaway_id),
    CONSTRAINT fk_boot_order_season FOREIGN KEY (version_season) REFERENCES bronze.season_summary (version_season),
    CONSTRAINT fk_boot_order_castaway FOREIGN KEY (castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_boot_order_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS bronze.auction_details (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    auction_detail_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version TEXT NOT NULL,
    version_season TEXT NOT NULL,
    season INT,
    auction_num INT,
    item INT NOT NULL,
    item_description TEXT,
    category TEXT,
    castaway TEXT,
    castaway_id TEXT,
    cost INT,
    covered BOOLEAN,
    money_remaining INT,
    participated TEXT,
    notes TEXT,
    alternative_offered BOOLEAN,
    alternative_accepted BOOLEAN,
    other_item TEXT,
    other_item_category TEXT,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_auction_details UNIQUE (version_season, auction_num, item, castaway_id),
    CONSTRAINT fk_auction_details_season FOREIGN KEY (version_season) REFERENCES bronze.season_summary (version_season),
    CONSTRAINT fk_auction_details_castaway FOREIGN KEY (castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_auction_details_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS bronze.survivor_auction (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    survivor_auction_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version TEXT NOT NULL,
    version_season TEXT NOT NULL,
    season INT,
    episode INT,
    n_boots INT,
    castaway_id TEXT NOT NULL,
    castaway TEXT,
    tribe TEXT,
    tribe_status TEXT,
    total INT,
    currency TEXT,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_survivor_auction UNIQUE (version_season, episode, castaway_id),
    CONSTRAINT fk_survivor_auction_season FOREIGN KEY (version_season) REFERENCES bronze.season_summary (version_season),
    CONSTRAINT fk_survivor_auction_castaway FOREIGN KEY (castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_survivor_auction_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS bronze.castaway_scores (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    castaway_score_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version TEXT NOT NULL,
    version_season TEXT NOT NULL,
    season INT,
    castaway TEXT,
    castaway_id TEXT NOT NULL,
    score_overall DOUBLE PRECISION,
    score_outwit DOUBLE PRECISION,
    score_outplay DOUBLE PRECISION,
    score_outlast DOUBLE PRECISION,
    score_result DOUBLE PRECISION,
    score_jury DOUBLE PRECISION,
    score_vote DOUBLE PRECISION,
    score_adv DOUBLE PRECISION,
    score_inf DOUBLE PRECISION,
    r_score_chal_all DOUBLE PRECISION,
    r_score_chal_immunity DOUBLE PRECISION,
    r_score_chal_individual DOUBLE PRECISION,
    r_score_chal_individual_immunity DOUBLE PRECISION,
    r_score_chal_reward DOUBLE PRECISION,
    r_score_chal_team DOUBLE PRECISION,
    r_score_chal_team_reward DOUBLE PRECISION,
    r_score_chal_tribal DOUBLE PRECISION,
    r_score_chal_tribal_immunity DOUBLE PRECISION,
    r_score_chal_tribal_reward DOUBLE PRECISION,
    r_score_chal_individual_reward DOUBLE PRECISION,
    r_score_chal_team_immunity DOUBLE PRECISION,
    r_score_chal_duel DOUBLE PRECISION,
    p_score_chal_all DOUBLE PRECISION,
    p_score_chal_immunity DOUBLE PRECISION,
    p_score_chal_individual DOUBLE PRECISION,
    p_score_chal_individual_immunity DOUBLE PRECISION,
    p_score_chal_reward DOUBLE PRECISION,
    p_score_chal_team DOUBLE PRECISION,
    p_score_chal_team_reward DOUBLE PRECISION,
    p_score_chal_tribal DOUBLE PRECISION,
    p_score_chal_tribal_immunity DOUBLE PRECISION,
    p_score_chal_tribal_reward DOUBLE PRECISION,
    p_score_chal_individual_reward DOUBLE PRECISION,
    p_score_chal_team_immunity DOUBLE PRECISION,
    p_score_chal_duel DOUBLE PRECISION,
    r_score_vote DOUBLE PRECISION,
    p_score_vote DOUBLE PRECISION,
    n_votes_received INT,
    n_successful_boots INT,
    p_successful_boot DOUBLE PRECISION,
    n_tribals INT,
    n_tribals_with_vote INT,
    r_score_adv DOUBLE PRECISION,
    p_score_adv DOUBLE PRECISION,
    n_adv_played INT,
    n_adv_not_played INT,
    n_adv_found INT,
    n_idols_found INT,
    n_voted_out_with_adv INT,
    n_voted_out_with_idol INT,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_castaway_scores UNIQUE (version_season, castaway_id),
    CONSTRAINT fk_castaway_scores_season FOREIGN KEY (version_season) REFERENCES bronze.season_summary (version_season),
    CONSTRAINT fk_castaway_scores_castaway FOREIGN KEY (castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_castaway_scores_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS bronze.journeys (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    journey_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version TEXT NOT NULL,
    version_season TEXT NOT NULL,
    season INT,
    episode INT,
    sog_id INT,
    castaway_id TEXT NOT NULL,
    castaway TEXT,
    reward TEXT,
    lost_vote BOOLEAN,
    season_name TEXT,
    game_played TEXT,
    chose_to_play TEXT,
    event TEXT,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_journeys UNIQUE (version_season, episode, sog_id, castaway_id),
    CONSTRAINT fk_journeys_season FOREIGN KEY (version_season) REFERENCES bronze.season_summary (version_season),
    CONSTRAINT fk_journeys_castaway FOREIGN KEY (castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_journeys_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS bronze.tribe_mapping (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    tribe_map_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version TEXT NOT NULL,
    version_season TEXT NOT NULL,
    season INT,
    episode INT,
    day INT,
    castaway_id TEXT,
    castaway TEXT,
    tribe TEXT,
    tribe_status TEXT,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_tribe_mapping UNIQUE (castaway_id, version_season, episode, tribe, day),
    CONSTRAINT fk_tribe_mapping_season FOREIGN KEY (version_season) REFERENCES bronze.season_summary (version_season),
    CONSTRAINT fk_tribe_mapping_castaway FOREIGN KEY (castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_tribe_mapping_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS bronze.confessionals (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    confessional_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version TEXT NOT NULL,
    version_season TEXT NOT NULL,
    season INT,
    episode INT,
    castaway TEXT,
    castaway_id TEXT,
    confessional_count INT,
    confessional_time DOUBLE PRECISION,
    exp_count DOUBLE PRECISION,
    exp_time DOUBLE PRECISION,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_confessionals UNIQUE (castaway_id, version_season, episode),
    CONSTRAINT fk_confessionals_season FOREIGN KEY (version_season) REFERENCES bronze.season_summary (version_season),
    CONSTRAINT fk_confessionals_castaway FOREIGN KEY (castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_confessionals_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS bronze.challenge_results (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    challenge_results_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version TEXT NOT NULL,
    version_season TEXT NOT NULL,
    season INT,
    episode INT,
    n_boots INT,
    castaway_id TEXT,
    castaway TEXT,
    tribe TEXT,
    tribe_status TEXT,
    challenge_type TEXT,
    outcome_type TEXT,
    team TEXT,
    result TEXT,
    result_notes TEXT,
    chosen_for_reward BOOLEAN,
    challenge_id INT,
    sit_out BOOLEAN,
    order_of_finish INT,
    sog_id INT,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_challenge_results UNIQUE (castaway_id, challenge_id, sog_id, version_season),
    CONSTRAINT fk_challenge_results_season FOREIGN KEY (version_season) REFERENCES bronze.season_summary (version_season),
    CONSTRAINT fk_challenge_results_castaway FOREIGN KEY (castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_challenge_results_challenge FOREIGN KEY (version_season, challenge_id) REFERENCES bronze.challenge_description (version_season, challenge_id),
    CONSTRAINT fk_challenge_results_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS bronze.vote_history (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    vote_history_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version TEXT NOT NULL,
    version_season TEXT NOT NULL,
    season INT,
    episode INT,
    day INT,
    tribe_status TEXT,
    tribe TEXT,
    castaway TEXT,
    immunity TEXT,
    vote TEXT,
    vote_event TEXT,
    vote_event_outcome TEXT,
    split_vote TEXT,
    nullified BOOLEAN,
    tie BOOLEAN,
    voted_out TEXT,
    vote_history_order INT,
    vote_order INT,
    castaway_id TEXT,
    vote_id TEXT,
    voted_out_id TEXT,
    sog_id INT,
    challenge_id INT,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_vote_history_season FOREIGN KEY (version_season) REFERENCES bronze.season_summary (version_season),
    CONSTRAINT fk_vote_history_castaway FOREIGN KEY (castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_vote_history_vote_id FOREIGN KEY (vote_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_vote_history_voted_out FOREIGN KEY (voted_out_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_vote_history_challenge FOREIGN KEY (version_season, challenge_id) REFERENCES bronze.challenge_description (version_season, challenge_id),
    CONSTRAINT fk_vote_history_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

CREATE TABLE IF NOT EXISTS bronze.jury_votes (
    ingest_run_id UUID NOT NULL DEFAULT uuid_generate_v4(),
    jury_vote_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version TEXT NOT NULL,
    version_season TEXT NOT NULL,
    season INT,
    castaway TEXT,
    finalist TEXT,
    vote TEXT,
    castaway_id TEXT,
    finalist_id TEXT,
    source_dataset TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_jury_votes UNIQUE (version_season, castaway_id, vote, finalist_id),
    CONSTRAINT fk_jury_votes_season FOREIGN KEY (version_season) REFERENCES bronze.season_summary (version_season),
    CONSTRAINT fk_jury_votes_castaway FOREIGN KEY (castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_jury_votes_finalist FOREIGN KEY (finalist_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_jury_votes_ingest FOREIGN KEY (ingest_run_id) REFERENCES bronze.ingestion_runs(run_id)
);

-- Helpful indexes for bronze layer joins
CREATE INDEX IF NOT EXISTS idx_bronze_castaway_details_ingest ON bronze.castaway_details (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_bronze_season_summary_ingest ON bronze.season_summary (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_bronze_advantage_details_ingest ON bronze.advantage_details (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_bronze_challenge_description_ingest ON bronze.challenge_description (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_bronze_episodes_ingest ON bronze.episodes (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_bronze_castaways_ingest ON bronze.castaways (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_bronze_advantage_movement_ingest ON bronze.advantage_movement (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_bronze_boot_mapping_ingest ON bronze.boot_mapping (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_bronze_boot_order_ingest ON bronze.boot_order (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_bronze_castaway_scores_ingest ON bronze.castaway_scores (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_bronze_journeys_ingest ON bronze.journeys (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_bronze_tribe_mapping_ingest ON bronze.tribe_mapping (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_bronze_confessionals_ingest ON bronze.confessionals (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_bronze_challenge_results_ingest ON bronze.challenge_results (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_bronze_vote_history_ingest ON bronze.vote_history (ingest_run_id);
CREATE INDEX IF NOT EXISTS idx_bronze_jury_votes_ingest ON bronze.jury_votes (ingest_run_id);

CREATE INDEX IF NOT EXISTS idx_bronze_castaways_version_season ON bronze.castaways (version_season, castaway_id);
CREATE INDEX IF NOT EXISTS idx_bronze_boot_mapping_version_season ON bronze.boot_mapping (version_season, episode);
CREATE INDEX IF NOT EXISTS idx_bronze_boot_order_version_season ON bronze.boot_order (version_season, boot_order_position);
CREATE INDEX IF NOT EXISTS idx_bronze_castaway_scores_version_season ON bronze.castaway_scores (version_season, castaway_id);
CREATE INDEX IF NOT EXISTS idx_bronze_journeys_version_season ON bronze.journeys (version_season, episode);
CREATE INDEX IF NOT EXISTS idx_bronze_boot_mapping_stage ON bronze.boot_mapping (version_season, sog_id, castaway_id);
CREATE INDEX IF NOT EXISTS idx_bronze_confessionals_version_season ON bronze.confessionals (version_season, castaway_id, episode);
CREATE INDEX IF NOT EXISTS idx_bronze_challenge_results_version_season ON bronze.challenge_results (version_season, challenge_id);
CREATE INDEX IF NOT EXISTS idx_bronze_vote_history_version_season ON bronze.vote_history (version_season, episode);
CREATE INDEX IF NOT EXISTS idx_bronze_challenge_results_stage ON bronze.challenge_results (version_season, sog_id, castaway_id);
CREATE INDEX IF NOT EXISTS idx_bronze_vote_history_stage ON bronze.vote_history (version_season, sog_id, castaway_id);
CREATE INDEX IF NOT EXISTS idx_bronze_advantage_movement_version_season ON bronze.advantage_movement (version_season, advantage_id);
CREATE INDEX IF NOT EXISTS idx_bronze_auction_details_version_season ON bronze.auction_details (version_season, auction_num, item);
CREATE INDEX IF NOT EXISTS idx_bronze_survivor_auction_version_season ON bronze.survivor_auction (version_season, episode);

-- ============================================================================
-- Silver Layer: Curated, analytics-friendly dimensions and facts
-- ============================================================================

CREATE TABLE IF NOT EXISTS silver.dim_castaway (
    castaway_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    castaway_id TEXT NOT NULL,
    full_name TEXT,
    gender TEXT,
    date_of_birth DATE,
    date_of_death DATE,
    collar TEXT,
    occupation TEXT,
    personality_type TEXT,
    race TEXT,
    ethnicity TEXT,
    is_african BOOLEAN,
    is_asian BOOLEAN,
    is_latin_american BOOLEAN,
    is_native_american BOOLEAN,
    is_bipoc BOOLEAN,
    is_lgbt BOOLEAN,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_dim_castaway UNIQUE (castaway_id),
    CONSTRAINT fk_dim_castaway_source FOREIGN KEY (castaway_id) REFERENCES bronze.castaway_details (castaway_id)
);

CREATE TABLE IF NOT EXISTS silver.dim_season (
    season_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version TEXT NOT NULL,
    version_season TEXT NOT NULL,
    season_name TEXT,
    season_number INT,
    location TEXT,
    country TEXT,
    tribe_setup TEXT,
    cast_size INT,
    tribe_count INT,
    finalist_count INT,
    jury_count INT,
    premiered DATE,
    ended DATE,
    filming_started DATE,
    filming_ended DATE,
    winner_castaway_id TEXT,
    viewers_reunion DOUBLE PRECISION,
    viewers_premiere INT,
    viewers_finale INT,
    viewers_mean DOUBLE PRECISION,
    rank INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_dim_season UNIQUE (version_season),
    CONSTRAINT fk_dim_season_source FOREIGN KEY (version_season) REFERENCES bronze.season_summary (version_season),
    CONSTRAINT fk_dim_season_winner FOREIGN KEY (winner_castaway_id) REFERENCES bronze.castaway_details (castaway_id)
);

CREATE TABLE IF NOT EXISTS silver.dim_episode (
    episode_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    season_key BIGINT NOT NULL,
    version_season TEXT NOT NULL,
    episode_in_season INT NOT NULL,
    episode_number_overall INT,
    episode_title TEXT,
    episode_label TEXT,
    episode_date DATE,
    episode_length DOUBLE PRECISION,
    viewers DOUBLE PRECISION,
    imdb_rating DOUBLE PRECISION,
    n_ratings INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_dim_episode UNIQUE (version_season, episode_in_season),
    CONSTRAINT fk_dim_episode_season FOREIGN KEY (season_key) REFERENCES silver.dim_season (season_key) ON DELETE CASCADE,
    CONSTRAINT fk_dim_episode_source FOREIGN KEY (version_season, episode_in_season) REFERENCES bronze.episodes (version_season, episode)
);

CREATE TABLE IF NOT EXISTS silver.dim_advantage (
    advantage_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version_season TEXT NOT NULL,
    advantage_id INT NOT NULL,
    advantage_type TEXT,
    clue_details TEXT,
    location_found TEXT,
    conditions TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_dim_advantage UNIQUE (version_season, advantage_id),
    CONSTRAINT fk_dim_advantage_source FOREIGN KEY (version_season, advantage_id) REFERENCES bronze.advantage_details (version_season, advantage_id)
);

CREATE TABLE IF NOT EXISTS silver.dim_challenge (
    challenge_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    version_season TEXT NOT NULL,
    challenge_id INT NOT NULL,
    episode_in_season INT,
    challenge_number INT,
    challenge_type TEXT,
    name TEXT,
    recurring_name TEXT,
    description TEXT,
    reward TEXT,
    additional_stipulation TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_dim_challenge UNIQUE (version_season, challenge_id),
    CONSTRAINT fk_dim_challenge_source FOREIGN KEY (version_season, challenge_id) REFERENCES bronze.challenge_description (version_season, challenge_id),
    CONSTRAINT fk_dim_challenge_episode FOREIGN KEY (version_season, episode_in_season) REFERENCES silver.dim_episode (version_season, episode_in_season)
);

CREATE TABLE IF NOT EXISTS silver.challenge_skill_lookup (
    skill_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    skill_name TEXT NOT NULL,
    skill_category TEXT,
    CONSTRAINT uq_skill_lookup UNIQUE (skill_name)
);

CREATE TABLE IF NOT EXISTS silver.challenge_skill_bridge (
    challenge_key BIGINT NOT NULL,
    skill_key BIGINT NOT NULL,
    PRIMARY KEY (challenge_key, skill_key),
    CONSTRAINT fk_challenge_skill_challenge FOREIGN KEY (challenge_key) REFERENCES silver.dim_challenge (challenge_key) ON DELETE CASCADE,
    CONSTRAINT fk_challenge_skill_lookup FOREIGN KEY (skill_key) REFERENCES silver.challenge_skill_lookup (skill_key) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS silver.bridge_castaway_season (
    castaway_season_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    castaway_key BIGINT NOT NULL,
    season_key BIGINT NOT NULL,
    castaway_id TEXT NOT NULL,
    version_season TEXT NOT NULL,
    original_tribe TEXT,
    result TEXT,
    place TEXT,
    jury_status TEXT,
    jury BOOLEAN,
    finalist BOOLEAN,
    winner BOOLEAN,
    acknowledge BOOLEAN,
    ack_look BOOLEAN,
    ack_speak BOOLEAN,
    ack_gesture BOOLEAN,
    ack_smile BOOLEAN,
    ack_quote TEXT,
    ack_score INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_bridge_castaway_season UNIQUE (castaway_key, season_key),
    CONSTRAINT fk_bridge_castaway FOREIGN KEY (castaway_key) REFERENCES silver.dim_castaway (castaway_key),
    CONSTRAINT fk_bridge_season FOREIGN KEY (season_key) REFERENCES silver.dim_season (season_key),
    CONSTRAINT fk_bridge_castaway_source FOREIGN KEY (castaway_id) REFERENCES bronze.castaway_details (castaway_id)
);

CREATE TABLE IF NOT EXISTS silver.fact_confessionals (
    confessional_fact_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    castaway_key BIGINT NOT NULL,
    season_key BIGINT NOT NULL,
    episode_key BIGINT NOT NULL,
    castaway_id TEXT NOT NULL,
    version_season TEXT NOT NULL,
    episode_in_season INT NOT NULL,
    confessional_count INT,
    confessional_time DOUBLE PRECISION,
    expected_count DOUBLE PRECISION,
    expected_time DOUBLE PRECISION,
    source_confessional_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_confessionals UNIQUE (castaway_key, episode_key),
    CONSTRAINT fk_fact_confessionals_castaway FOREIGN KEY (castaway_key) REFERENCES silver.dim_castaway (castaway_key),
    CONSTRAINT fk_fact_confessionals_season FOREIGN KEY (season_key) REFERENCES silver.dim_season (season_key),
    CONSTRAINT fk_fact_confessionals_episode FOREIGN KEY (episode_key) REFERENCES silver.dim_episode (episode_key),
    CONSTRAINT fk_fact_confessionals_source FOREIGN KEY (source_confessional_id) REFERENCES bronze.confessionals (confessional_id)
);

CREATE TABLE IF NOT EXISTS silver.fact_challenge_results (
    challenge_fact_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    castaway_key BIGINT NOT NULL,
    season_key BIGINT NOT NULL,
    episode_key BIGINT,
    challenge_key BIGINT NOT NULL,
    advantage_key BIGINT,
    castaway_id TEXT NOT NULL,
    version_season TEXT NOT NULL,
    challenge_id INT NOT NULL,
    sog_id INT,
    outcome_type TEXT,
    result TEXT,
    result_notes TEXT,
    chosen_for_reward BOOLEAN,
    sit_out BOOLEAN,
    order_of_finish INT,
    source_challenge_result_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_challenge_results UNIQUE (castaway_key, challenge_key, sog_id),
    CONSTRAINT fk_fact_challenge_castaway FOREIGN KEY (castaway_key) REFERENCES silver.dim_castaway (castaway_key),
    CONSTRAINT fk_fact_challenge_season FOREIGN KEY (season_key) REFERENCES silver.dim_season (season_key),
    CONSTRAINT fk_fact_challenge_episode FOREIGN KEY (episode_key) REFERENCES silver.dim_episode (episode_key),
    CONSTRAINT fk_fact_challenge_challenge FOREIGN KEY (challenge_key) REFERENCES silver.dim_challenge (challenge_key),
    CONSTRAINT fk_fact_challenge_advantage FOREIGN KEY (advantage_key) REFERENCES silver.dim_advantage (advantage_key),
    CONSTRAINT fk_fact_challenge_source FOREIGN KEY (source_challenge_result_id) REFERENCES bronze.challenge_results (challenge_results_id)
);

CREATE TABLE IF NOT EXISTS silver.fact_vote_history (
    vote_fact_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    castaway_key BIGINT NOT NULL,
    season_key BIGINT NOT NULL,
    episode_key BIGINT NOT NULL,
    challenge_key BIGINT,
    castaway_id TEXT NOT NULL,
    target_castaway_id TEXT,
    voted_out_castaway_id TEXT,
    version_season TEXT NOT NULL,
    episode_in_season INT NOT NULL,
    immunity TEXT,
    vote TEXT,
    vote_event TEXT,
    vote_event_outcome TEXT,
    split_vote TEXT,
    nullified BOOLEAN,
    tie BOOLEAN,
    vote_order INT,
    sog_id INT,
    source_vote_history_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_fact_vote_castaway FOREIGN KEY (castaway_key) REFERENCES silver.dim_castaway (castaway_key),
    CONSTRAINT fk_fact_vote_season FOREIGN KEY (season_key) REFERENCES silver.dim_season (season_key),
    CONSTRAINT fk_fact_vote_episode FOREIGN KEY (episode_key) REFERENCES silver.dim_episode (episode_key),
    CONSTRAINT fk_fact_vote_challenge FOREIGN KEY (challenge_key) REFERENCES silver.dim_challenge (challenge_key),
    CONSTRAINT fk_fact_vote_castaway_source FOREIGN KEY (castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_fact_vote_target_source FOREIGN KEY (target_castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_fact_vote_voted_out_source FOREIGN KEY (voted_out_castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_fact_vote_source FOREIGN KEY (source_vote_history_id) REFERENCES bronze.vote_history (vote_history_id)
);

CREATE TABLE IF NOT EXISTS silver.fact_advantage_movement (
    advantage_fact_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    castaway_key BIGINT,
    target_castaway_key BIGINT,
    season_key BIGINT NOT NULL,
    episode_key BIGINT,
    advantage_key BIGINT NOT NULL,
    castaway_id TEXT,
    target_castaway_id TEXT,
    version_season TEXT NOT NULL,
    sequence_id INT NOT NULL,
    advantage_id INT NOT NULL,
    day INT,
    episode_in_season INT,
    event TEXT,
    success TEXT,
    votes_nullified DOUBLE PRECISION,
    sog_id INT,
    source_advantage_movement_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_advantage_movement UNIQUE (version_season, advantage_id, sequence_id),
    CONSTRAINT fk_fact_advantage_castaway FOREIGN KEY (castaway_key) REFERENCES silver.dim_castaway (castaway_key),
    CONSTRAINT fk_fact_advantage_target FOREIGN KEY (target_castaway_key) REFERENCES silver.dim_castaway (castaway_key),
    CONSTRAINT fk_fact_advantage_season FOREIGN KEY (season_key) REFERENCES silver.dim_season (season_key),
    CONSTRAINT fk_fact_advantage_episode FOREIGN KEY (episode_key) REFERENCES silver.dim_episode (episode_key),
    CONSTRAINT fk_fact_advantage_advantage FOREIGN KEY (advantage_key) REFERENCES silver.dim_advantage (advantage_key),
    CONSTRAINT fk_fact_advantage_source FOREIGN KEY (source_advantage_movement_id) REFERENCES bronze.advantage_movement (advantage_movement_id)
);

CREATE TABLE IF NOT EXISTS silver.fact_boot_mapping (
    boot_fact_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    castaway_key BIGINT,
    season_key BIGINT NOT NULL,
    episode_key BIGINT,
    castaway_id TEXT,
    version_season TEXT NOT NULL,
    episode_in_season INT,
    boot_mapping_order INT,
    n_boots INT,
    final_n INT,
    sog_id INT,
    tribe TEXT,
    tribe_status TEXT,
    game_status TEXT,
    source_boot_mapping_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_fact_boot_castaway FOREIGN KEY (castaway_key) REFERENCES silver.dim_castaway (castaway_key),
    CONSTRAINT fk_fact_boot_season FOREIGN KEY (season_key) REFERENCES silver.dim_season (season_key),
    CONSTRAINT fk_fact_boot_episode FOREIGN KEY (episode_key) REFERENCES silver.dim_episode (episode_key),
    CONSTRAINT fk_fact_boot_source FOREIGN KEY (source_boot_mapping_id) REFERENCES bronze.boot_mapping (boot_mapping_id)
);

CREATE TABLE IF NOT EXISTS silver.fact_tribe_membership (
    tribe_fact_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    castaway_key BIGINT,
    season_key BIGINT NOT NULL,
    episode_key BIGINT,
    castaway_id TEXT,
    version_season TEXT NOT NULL,
    episode_in_season INT,
    day INT,
    tribe TEXT,
    tribe_status TEXT,
    source_tribe_mapping_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_fact_tribe_castaway FOREIGN KEY (castaway_key) REFERENCES silver.dim_castaway (castaway_key),
    CONSTRAINT fk_fact_tribe_season FOREIGN KEY (season_key) REFERENCES silver.dim_season (season_key),
    CONSTRAINT fk_fact_tribe_episode FOREIGN KEY (episode_key) REFERENCES silver.dim_episode (episode_key),
    CONSTRAINT fk_fact_tribe_source FOREIGN KEY (source_tribe_mapping_id) REFERENCES bronze.tribe_mapping (tribe_map_id)
);

CREATE TABLE IF NOT EXISTS silver.fact_jury_votes (
    jury_fact_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    juror_castaway_key BIGINT,
    finalist_castaway_key BIGINT,
    season_key BIGINT NOT NULL,
    juror_castaway_id TEXT,
    finalist_castaway_id TEXT,
    version_season TEXT NOT NULL,
    vote TEXT,
    source_jury_vote_id BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_jury_votes UNIQUE (juror_castaway_key, finalist_castaway_key, season_key),
    CONSTRAINT fk_fact_jury_juror FOREIGN KEY (juror_castaway_key) REFERENCES silver.dim_castaway (castaway_key),
    CONSTRAINT fk_fact_jury_finalist FOREIGN KEY (finalist_castaway_key) REFERENCES silver.dim_castaway (castaway_key),
    CONSTRAINT fk_fact_jury_season FOREIGN KEY (season_key) REFERENCES silver.dim_season (season_key),
    CONSTRAINT fk_fact_jury_juror_source FOREIGN KEY (juror_castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_fact_jury_finalist_source FOREIGN KEY (finalist_castaway_id) REFERENCES bronze.castaway_details (castaway_id),
    CONSTRAINT fk_fact_jury_source FOREIGN KEY (source_jury_vote_id) REFERENCES bronze.jury_votes (jury_vote_id)
);

CREATE INDEX IF NOT EXISTS idx_silver_dim_episode_season ON silver.dim_episode (season_key, episode_in_season);
CREATE INDEX IF NOT EXISTS idx_silver_bridge_castaway_season ON silver.bridge_castaway_season (castaway_key, season_key);
CREATE INDEX IF NOT EXISTS idx_silver_fact_confessionals_episode ON silver.fact_confessionals (episode_key);
CREATE INDEX IF NOT EXISTS idx_silver_fact_challenge_results_challenge ON silver.fact_challenge_results (challenge_key);
CREATE INDEX IF NOT EXISTS idx_silver_fact_vote_history_episode ON silver.fact_vote_history (episode_key);
CREATE INDEX IF NOT EXISTS idx_silver_fact_advantage_movement_advantage ON silver.fact_advantage_movement (advantage_key);
CREATE INDEX IF NOT EXISTS idx_silver_fact_boot_mapping_episode ON silver.fact_boot_mapping (episode_key);
CREATE INDEX IF NOT EXISTS idx_silver_fact_tribe_membership_episode ON silver.fact_tribe_membership (episode_key);

-- ============================================================================
-- Gold Layer: Feature snapshots for ML readiness
-- ============================================================================

CREATE TABLE IF NOT EXISTS gold.feature_snapshots (
    snapshot_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ingest_run_id UUID REFERENCES bronze.ingestion_runs(run_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    environment TEXT,
    git_branch TEXT,
    git_commit TEXT,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS gold.castaway_season_features (
    snapshot_id UUID NOT NULL REFERENCES gold.feature_snapshots(snapshot_id) ON DELETE CASCADE,
    castaway_key BIGINT NOT NULL,
    season_key BIGINT NOT NULL,
    castaway_id TEXT,
    version_season TEXT,
    feature_payload JSONB NOT NULL,
    PRIMARY KEY (snapshot_id, castaway_key)
);

CREATE TABLE IF NOT EXISTS gold.castaway_episode_features (
    snapshot_id UUID NOT NULL REFERENCES gold.feature_snapshots(snapshot_id) ON DELETE CASCADE,
    castaway_key BIGINT NOT NULL,
    season_key BIGINT NOT NULL,
    episode_key BIGINT NOT NULL,
    castaway_id TEXT,
    version_season TEXT,
    episode_in_season INT,
    feature_payload JSONB NOT NULL,
    PRIMARY KEY (snapshot_id, castaway_key, episode_key)
);

CREATE TABLE IF NOT EXISTS gold.season_features (
    snapshot_id UUID NOT NULL REFERENCES gold.feature_snapshots(snapshot_id) ON DELETE CASCADE,
    season_key BIGINT NOT NULL,
    version_season TEXT,
    feature_payload JSONB NOT NULL,
    PRIMARY KEY (snapshot_id, season_key)
);

CREATE INDEX IF NOT EXISTS idx_gold_castaway_season_features_castaway ON gold.castaway_season_features (castaway_key);
CREATE INDEX IF NOT EXISTS idx_gold_castaway_episode_features_castaway ON gold.castaway_episode_features (castaway_key);
CREATE INDEX IF NOT EXISTS idx_gold_castaway_episode_features_episode ON gold.castaway_episode_features (episode_key);
CREATE INDEX IF NOT EXISTS idx_gold_season_features_version ON gold.season_features (version_season);
