-- Why claims were quarantined in silver, and what share of all live claims each reason is.
-- The 'passed' row holds the claims that made it through, so the shares add up to 1.
with checked as (
    select 'passed' as check_outcome from {{ source('silver', 'claims') }} where not is_deleted
    union all
    select reject_reason from {{ source('silver', 'claims_rejects') }} where not is_deleted
)

select
    check_outcome,
    check_outcome <> 'passed' as is_quarantined,
    count(*) as n_claims,
    round(count(*) / sum(count(*)) over (), 4) as share_of_claims
from checked
group by check_outcome
