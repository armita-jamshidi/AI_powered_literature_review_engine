-- Histogram of extractor confidence by study design and result.
select
    study_type,
    evidence_rank,
    result,
    confidence_bucket,
    cast(confidence_bucket * 100 as int) || '-' || cast(confidence_bucket * 100 + 10 as int) || '%'
        as confidence_bucket_label,
    count(*) as n_claims,
    round(avg(confidence), 3) as avg_confidence
from {{ ref('fct_claims') }}
group by study_type, evidence_rank, result, confidence_bucket
