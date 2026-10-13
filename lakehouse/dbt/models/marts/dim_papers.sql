-- One row per live canonical paper, with how many claims it contributes.
select
    p.paper_id,
    p.title,
    p.source_filename,
    p.ingest_status,
    p.created_at,
    p.is_synthetic,
    coalesce(c.n_claims, 0) as n_claims
from {{ ref('stg_papers') }} as p
left join (
    select paper_id, count(*) as n_claims
    from {{ ref('stg_claims') }}
    group by paper_id
) as c
    on p.paper_id = c.paper_id
