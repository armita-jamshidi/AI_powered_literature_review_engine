-- How many rows each layer holds for the current state of the data, in pipeline order.
-- Reading top to bottom shows where rows go: bronze -> silver splits claims into
-- passed and quarantined, and gold merges re-uploads and repeated findings.
-- "Live" means present in the latest export (not deleted in Postgres).
with latest_run as (
    select max(run_id) as run_id from {{ source('silver', 'bronze_runs') }}
),

bronze as (
    select r.source_table, r.n_rows
    from {{ source('silver', 'bronze_runs') }} as r
    inner join latest_run as l
        on r.run_id = l.run_id
)

select 1 as step, 'bronze' as layer, 'papers' as table_name,
       'papers in the latest export' as description,
       (select n_rows from bronze where source_table = 'papers') as n_rows
union all
select 2, 'bronze', 'claim_evidence', 'claims in the latest export',
       (select n_rows from bronze where source_table = 'claim_evidence')
union all
select 3, 'silver', 'papers', 'live papers',
       (select count(*) from {{ source('silver', 'papers') }} where not is_deleted)
union all
select 4, 'silver', 'claims', 'live claims that passed the quality checks',
       (select count(*) from {{ source('silver', 'claims') }} where not is_deleted)
union all
select 5, 'silver', 'claims_rejects', 'live claims quarantined by the quality checks',
       (select count(*) from {{ source('silver', 'claims_rejects') }} where not is_deleted)
union all
select 6, 'gold', 'dim_papers', 'papers after merging re-uploads',
       (select count(*) from {{ ref('dim_papers') }})
union all
select 7, 'gold', 'fct_claims', 'claims after removing repeated findings',
       (select count(*) from {{ ref('fct_claims') }})
