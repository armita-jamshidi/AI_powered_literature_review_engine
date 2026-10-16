-- Export activity per ingest_date (the day the bronze export ran), from the run manifests.
-- Row counts are from that day's last run, since every run is a full snapshot.
with runs as (
    select
        run_id,
        ingest_date,
        exported_at,
        sum(case when source_table = 'papers' then n_rows else 0 end) as papers_rows,
        sum(case when source_table = 'claim_evidence' then n_rows else 0 end) as claims_rows
    from {{ source('silver', 'bronze_runs') }}
    group by run_id, ingest_date, exported_at
),

ranked as (
    select
        *,
        row_number() over (partition by ingest_date order by run_id desc) as run_rank,
        count(*) over (partition by ingest_date) as n_runs,
        min(exported_at) over (partition by ingest_date) as first_exported_at
    from runs
),

per_day as (
    select
        ingest_date,
        n_runs,
        first_exported_at,
        exported_at as last_exported_at,
        papers_rows,
        claims_rows
    from ranked
    where run_rank = 1
)

select
    ingest_date,
    n_runs,
    first_exported_at,
    last_exported_at,
    papers_rows,
    claims_rows,
    -- Change since the previous day that had an export (null on the first day).
    papers_rows - lag(papers_rows) over (order by ingest_date) as papers_added,
    claims_rows - lag(claims_rows) over (order by ingest_date) as claims_added,
    datediff(current_date(), ingest_date) as days_ago
from per_day
