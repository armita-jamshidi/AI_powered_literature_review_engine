-- One row of headline numbers for the whole pipeline: a KPI strip for a dashboard.
with runs as (
    select
        max(run_id) as latest_run_id,
        max(ingest_date) as latest_ingest_date,
        max(exported_at) as last_exported_at,
        count(distinct run_id) as n_export_runs
    from {{ source('silver', 'bronze_runs') }}
),

layers as (
    select
        max(case when table_name = 'claim_evidence' and layer = 'bronze' then n_rows end) as bronze_claims,
        max(case when table_name = 'papers' and layer = 'bronze' then n_rows end) as bronze_papers,
        max(case when table_name = 'claims_rejects' then n_rows end) as quarantined_claims,
        max(case when table_name = 'dim_papers' then n_rows end) as gold_papers,
        max(case when table_name = 'fct_claims' then n_rows end) as gold_claims
    from {{ ref('metrics_layer_row_counts') }}
),

papers as (
    select
        count(*) as n_papers,
        sum(case when ingest_status = 'failed' then 1 else 0 end) as failed_papers,
        sum(case when has_evidence then 1 else 0 end) as papers_with_evidence,
        avg(case when ingest_status = 'processed' then n_claims end) as avg_claims_per_processed_paper
    from {{ ref('metrics_evidence_per_paper') }}
)

select
    r.latest_run_id,
    r.latest_ingest_date,
    r.last_exported_at,
    r.n_export_runs,
    round((unix_timestamp(current_timestamp()) - unix_timestamp(r.last_exported_at)) / 3600.0, 1)
        as hours_since_last_export,
    l.bronze_papers,
    l.bronze_claims,
    l.quarantined_claims,
    round(l.quarantined_claims / nullif(l.bronze_claims, 0), 4) as quarantine_rate,
    l.gold_papers,
    l.gold_claims,
    p.failed_papers,
    round(p.failed_papers / nullif(p.n_papers, 0), 4) as failed_paper_rate,
    p.papers_with_evidence,
    round(p.avg_claims_per_processed_paper, 2) as avg_claims_per_processed_paper,
    (select count(distinct claim_pair) from {{ ref('fct_claims') }}) as n_claim_pairs,
    current_timestamp() as measured_at
from runs as r
cross join layers as l
cross join papers as p
