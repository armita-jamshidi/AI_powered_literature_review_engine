"""Bronze export: copy Evidentia's Postgres tables into the data lake as Parquet.

This is the first hop of the lakehouse. It is a batch job that runs next to the
FastAPI app, not inside it: it only issues SELECTs, inside one read-only
REPEATABLE READ transaction, so every table in a run comes from the same
consistent snapshot and the app keeps ingesting papers while it runs.

Each run is a full snapshot of each table, written to

    <prefix>/<table>/ingest_date=YYYY-MM-DD/run_id=<run_id>/part-00000.parquet

`ingest_date=...` is Hive-style partitioning: Spark (and most lake engines)
turn the folder names into a column, and a query filtered on ingest_date only
opens the matching folders. After all data files are written, a manifest is
written to <prefix>/_manifests/run_id=<run_id>.json. Readers should only trust
runs that have a manifest, so a run that crashed halfway is never read.

Usage (from the repo root):
    python -m lakehouse.export.bronze_export --target local   # writes ./lake/bronze/...
    python -m lakehouse.export.bronze_export --target s3      # needs S3_BUCKET + AWS credentials
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Protocol

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

from lakehouse.db import make_engine
from lakehouse.export import schemas

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExportTable:
    name: str
    query: str
    schema: pa.Schema


# papers.full_text is left out on purpose: it is the whole paper body, much
# larger than everything else combined, and none of the analytics need it.
TABLES = (
    ExportTable(
        'papers',
        'SELECT id, title, source_filename, ingest_status, ingest_error, created_at FROM papers ORDER BY id',
        schemas.PAPERS,
    ),
    ExportTable(
        'claim_evidence',
        'SELECT id, paper_id, claim_key, population, intervention, outcome, result, study_type, confidence '
        'FROM claim_evidence ORDER BY id',
        schemas.CLAIM_EVIDENCE,
    ),
)


class Writer(Protocol):
    def put(self, key: str, data: bytes) -> str:
        """Store bytes under a lake-relative key and return the full URI written."""


class LocalWriter:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def put(self, key: str, data: bytes) -> str:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        return str(path)


class S3Writer:
    def __init__(self, bucket: str, client=None):
        import boto3

        self.bucket = bucket
        # No keys are passed here: boto3 finds credentials itself, from the
        # AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars or ~/.aws/credentials.
        self.client = client or boto3.client('s3')

    def put(self, key: str, data: bytes) -> str:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data)
        return f's3://{self.bucket}/{key}'


def make_run_id(now: datetime) -> str:
    return now.strftime('%Y%m%dT%H%M%SZ')


def partition_key(prefix: str, table: str, ingest_date: str, run_id: str, part: int = 0) -> str:
    return f'{prefix}/{table}/ingest_date={ingest_date}/run_id={run_id}/part-{part:05d}.parquet'


def manifest_key(prefix: str, run_id: str) -> str:
    return f'{prefix}/_manifests/run_id={run_id}.json'


def rows_to_batch(rows: Iterable[dict], schema: pa.Schema, run_id: str, exported_at: datetime) -> pa.RecordBatch:
    """Turn DB rows into an Arrow batch with the bronze schema and lineage columns."""
    rows = list(rows)
    columns = {}
    for f in schema:
        if f.name == '_run_id':
            values = [run_id] * len(rows)
        elif f.name == '_exported_at':
            values = [exported_at] * len(rows)
        elif f.name == '_source_table':
            values = [schema.metadata[b'source_table'].decode()] * len(rows)
        else:
            values = [r[f.name] for r in rows]
        columns[f.name] = pa.array(values, type=f.type)
    return pa.RecordBatch.from_pydict(columns, schema=schema)


def table_to_parquet(conn: Connection, table: ExportTable, run_id: str, exported_at: datetime,
                     batch_size: int = 5000) -> tuple[bytes, int]:
    """Stream a table out of Postgres into an in-memory Parquet file, batch_size rows at a time."""
    schema = table.schema.with_metadata({'source_table': table.name})
    buffer = io.BytesIO()
    n_rows = 0
    result = conn.execution_options(stream_results=True).execute(text(table.query)).mappings()
    # Snappy is Parquet's default codec: fast to decode, decent compression.
    with pq.ParquetWriter(buffer, schema, compression='snappy') as writer:
        while chunk := result.fetchmany(batch_size):
            writer.write_batch(rows_to_batch(chunk, schema, run_id, exported_at))
            n_rows += len(chunk)
        if n_rows == 0:
            # Still write an empty file so the schema is recorded for this run.
            writer.write_table(schema.empty_table())
    return buffer.getvalue(), n_rows


def run_export(engine: Engine, writer: Writer, prefix: str = 'bronze', now: datetime | None = None,
               tables: tuple[ExportTable, ...] = TABLES) -> dict:
    now = now or datetime.now(timezone.utc)
    run_id = make_run_id(now)
    ingest_date = now.strftime('%Y-%m-%d')
    manifest = {'run_id': run_id, 'ingest_date': ingest_date, 'exported_at': now.isoformat(), 'tables': {}}

    with engine.connect() as conn:
        if conn.dialect.name == 'postgresql':
            # One snapshot for all tables, so every exported claim's paper_id
            # points at a paper that is in the same run. Read-only means this
            # job can never modify the app's data.
            conn = conn.execution_options(isolation_level='REPEATABLE READ', postgresql_readonly=True)
        with conn.begin():
            for table in tables:
                data, n_rows = table_to_parquet(conn, table, run_id, now)
                uri = writer.put(partition_key(prefix, table.name, ingest_date, run_id), data)
                manifest['tables'][table.name] = {'rows': n_rows, 'uri': uri}
                logger.info('Exported %s rows from %s to %s', n_rows, table.name, uri)

    # Written last: its presence marks the run as complete.
    manifest['manifest_uri'] = writer.put(manifest_key(prefix, run_id), json.dumps(manifest, indent=2).encode())
    return manifest


def build_writer(target: str) -> tuple[Writer, str]:
    prefix = os.getenv('LAKE_PREFIX', 'bronze')
    if target == 'local':
        return LocalWriter(os.getenv('LOCAL_LAKE_DIR', './lake')), prefix
    if target == 's3':
        bucket = os.getenv('S3_BUCKET')
        if not bucket:
            raise SystemExit('S3_BUCKET is not set. Add it to your .env (see lakehouse/.env.example).')
        return S3Writer(bucket), prefix
    raise SystemExit(f'Unknown target {target!r}; use "local" or "s3".')


def main(argv: list[str] | None = None) -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description='Export Evidentia tables to the bronze layer as Parquet.')
    parser.add_argument('--target', choices=('local', 's3'), default=os.getenv('LAKE_TARGET', 'local'))
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')

    writer, prefix = build_writer(args.target)
    manifest = run_export(make_engine(), writer, prefix)
    print(json.dumps(manifest, indent=2))


if __name__ == '__main__':
    main()
