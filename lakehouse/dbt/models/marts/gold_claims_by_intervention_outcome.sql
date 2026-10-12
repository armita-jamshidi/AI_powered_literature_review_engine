-- Claims per intervention x outcome pair, split by reported result.
select
    intervention,
    outcome,
    claim_pair,
    count(*) as n_claims,
    count(distinct paper_id) as n_papers,
    sum(case when result = 'positive' then 1 else 0 end) as n_positive,
    sum(case when result = 'negative' then 1 else 0 end) as n_negative,
    sum(case when result = 'null' then 1 else 0 end) as n_null,
    sum(case when result = 'mixed' then 1 else 0 end) as n_mixed,
    round(avg(confidence), 3) as avg_confidence
from {{ ref('fct_claims') }}
group by intervention, outcome, claim_pair
