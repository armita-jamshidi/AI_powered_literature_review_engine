-- Every live claim must be counted exactly once in each aggregate gold table.
-- Returns a row (= test failure) when a total doesn't match the fact table.
with totals as (
    select
        (select count(*) from {{ ref('fct_claims') }}) as fact_claims,
        (select sum(n_claims) from {{ ref('gold_claims_by_intervention_outcome') }}) as by_pair_claims,
        (select sum(n_claims) from {{ ref('gold_weighted_disagreement') }}) as disagreement_claims,
        (select sum(n_claims) from {{ ref('gold_confidence_distribution') }}) as histogram_claims
)
select *
from totals
where by_pair_claims <> fact_claims
   or disagreement_claims <> fact_claims
   or histogram_claims <> fact_claims
