"""Explicit Parquet schemas for the bronze layer.

Parquet files carry their schema inside the file. Declaring it here, rather than
letting pyarrow infer it from the first rows, means an empty table or a column
that happens to be all NULL in one run still produces the same types every run.
That keeps the silver job's reads predictable.
"""
from __future__ import annotations

import pyarrow as pa

# Lineage columns added to every bronze row. The leading underscore marks them
# as pipeline metadata rather than source data.
METADATA_FIELDS = [
    pa.field('_run_id', pa.string(), nullable=False),
    pa.field('_exported_at', pa.timestamp('us', tz='UTC'), nullable=False),
    pa.field('_source_table', pa.string(), nullable=False),
]

PAPERS = pa.schema([
    pa.field('id', pa.int64(), nullable=False),
    pa.field('title', pa.string()),
    pa.field('source_filename', pa.string()),
    pa.field('ingest_status', pa.string()),
    pa.field('ingest_error', pa.string()),
    # Postgres stores datetime.utcnow() without a zone; we tag it as UTC here.
    pa.field('created_at', pa.timestamp('us', tz='UTC')),
    *METADATA_FIELDS,
])

CLAIM_EVIDENCE = pa.schema([
    pa.field('id', pa.int64(), nullable=False),
    pa.field('paper_id', pa.int64()),
    pa.field('claim_key', pa.string()),
    pa.field('population', pa.string()),
    pa.field('intervention', pa.string()),
    pa.field('outcome', pa.string()),
    pa.field('result', pa.string()),
    pa.field('study_type', pa.string()),
    pa.field('confidence', pa.float64()),
    *METADATA_FIELDS,
])
