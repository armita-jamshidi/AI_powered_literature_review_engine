-- Live, canonical papers only: drops rows deleted in Postgres and second copies of re-uploads.
select
    paper_id,
    title,
    source_filename,
    ingest_status,
    ingest_error,
    created_at,
    is_synthetic
from {{ source('silver', 'papers') }}
where not is_deleted
  and not is_duplicate_upload
