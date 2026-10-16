-- Every row in the latest bronze export must land in silver: each exported paper is a
-- live silver paper, and each exported claim either passed or was quarantined.
-- Returns a row (= test failure) when a count doesn't match.
select *
from (
    select
        max(case when table_name = 'papers' and layer = 'bronze' then n_rows end) as bronze_papers,
        max(case when table_name = 'papers' and layer = 'silver' then n_rows end) as silver_papers,
        max(case when table_name = 'claim_evidence' then n_rows end) as bronze_claims,
        max(case when table_name = 'claims' then n_rows end)
            + max(case when table_name = 'claims_rejects' then n_rows end) as silver_claims_checked
    from {{ ref('metrics_layer_row_counts') }}
) as counts
where bronze_papers <> silver_papers
   or bronze_claims <> silver_claims_checked
