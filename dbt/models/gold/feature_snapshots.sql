{{ config(materialized='table', schema='gold') }}

select
    run_id as snapshot_id,
    run_id as ingest_run_id,
    environment,
    git_branch,
    git_commit,
    source_url,
    run_started_at,
    run_finished_at,
    status,
    notes
from {{ source('bronze', 'ingestion_runs') }};
