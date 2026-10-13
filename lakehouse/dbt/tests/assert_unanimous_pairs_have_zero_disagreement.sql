-- A pair whose claims all report the same result has nothing to disagree about.
select d.claim_pair, d.weighted_disagreement_score
from {{ ref('gold_weighted_disagreement') }} as d
inner join {{ ref('gold_claims_by_intervention_outcome') }} as c
    on d.claim_pair = c.claim_pair
where (
        (case when c.n_positive > 0 then 1 else 0 end)
      + (case when c.n_negative > 0 then 1 else 0 end)
      + (case when c.n_null > 0 then 1 else 0 end)
      + (case when c.n_mixed > 0 then 1 else 0 end)
    ) = 1
  and d.weighted_disagreement_score <> 0
