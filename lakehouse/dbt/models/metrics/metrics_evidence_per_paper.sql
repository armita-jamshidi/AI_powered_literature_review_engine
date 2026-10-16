-- How much usable evidence each live paper contributes after cleaning.
-- evidence_weight adds up the study-design weights of the paper's claims, so one
-- RCT claim (1.0) counts more than one case-report claim (0.3).
select
    p.paper_id,
    p.title,
    p.ingest_status,
    p.created_at,
    p.is_synthetic,
    count(c.claim_id) as n_claims,
    count(distinct c.claim_pair) as n_claim_pairs,
    round(coalesce(sum(c.study_weight), 0), 4) as evidence_weight,
    round(avg(c.confidence), 4) as avg_confidence,
    min(c.evidence_rank) as best_evidence_rank,
    count(c.claim_id) > 0 as has_evidence
from {{ ref('dim_papers') }} as p
left join {{ ref('fct_claims') }} as c
    on p.paper_id = c.paper_id
group by p.paper_id, p.title, p.ingest_status, p.created_at, p.is_synthetic
