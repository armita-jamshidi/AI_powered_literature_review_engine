-- Study-type-weighted disagreement per intervention x outcome pair.
--
-- Each claim votes for its result with its study design's weight (an RCT counts
-- 1.0, a case report 0.3). disagreement = 1 - (weight of the winning result /
-- total weight): 0 means every study agrees, values near 0.75 mean the four
-- results are split evenly. Same formula as weighted_majority_fraction() in
-- backend/app/pipeline.py, so it can be reconciled against GET /heatmap.
with votes as (
    select
        intervention,
        outcome,
        result,
        sum(study_weight) as result_weight,
        count(*) as result_claims
    from {{ ref('fct_claims') }}
    group by intervention, outcome, result
),

ranked as (
    select
        *,
        sum(result_weight) over (partition by intervention, outcome) as total_weight,
        sum(result_claims) over (partition by intervention, outcome) as total_claims,
        max(result_claims) over (partition by intervention, outcome) as top_result_claims,
        row_number() over (
            partition by intervention, outcome
            order by result_weight desc, result
        ) as weight_rank
    from votes
)

select
    intervention,
    outcome,
    intervention || ' -> ' || outcome as claim_pair,
    total_claims as n_claims,
    round(total_weight, 4) as total_weight,
    result as weighted_majority_result,
    round(result_weight / total_weight, 4) as weighted_majority_fraction,
    round(1 - result_weight / total_weight, 4) as weighted_disagreement_score,
    -- The unweighted version, to show what study-type weighting changes.
    round(1 - top_result_claims / total_claims, 4) as unweighted_disagreement_score
from ranked
where weight_rank = 1
