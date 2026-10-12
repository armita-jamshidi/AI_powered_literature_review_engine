-- One row per live claim, with its study-type weight attached. This is the fact
-- table the dashboard filters and drills into.
select
    c.claim_id,
    c.paper_id,
    c.cluster_id,
    c.population,
    c.intervention,
    c.outcome,
    c.intervention || ' -> ' || c.outcome as claim_pair,
    c.result,
    c.study_type,
    w.weight as study_weight,
    w.evidence_rank,
    c.confidence,
    c.confidence_rescaled,
    -- Ten equal-width buckets: [0.0, 0.1), [0.1, 0.2), ... [0.9, 1.0]
    least(floor(c.confidence * 10), 9) / 10.0 as confidence_bucket
from {{ ref('stg_claims') }} as c
inner join {{ ref('study_type_weights') }} as w
    on c.study_type = w.study_type
