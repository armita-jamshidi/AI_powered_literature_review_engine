-- Live, de-duplicated claims. Claims are attributed to the canonical paper, so a
-- re-uploaded paper's findings count once.
select
    claim_id,
    canonical_paper_id as paper_id,
    cluster_id,
    population,
    intervention,
    outcome,
    result,
    study_type,
    confidence,
    confidence_rescaled
from {{ source('silver', 'claims') }}
where not is_deleted
  and not is_duplicate
