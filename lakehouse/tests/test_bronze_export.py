import io
import json
import os
from datetime import datetime, timezone

import boto3
import pyarrow.parquet as pq
import pytest
from moto import mock_aws
from sqlalchemy import text

from lakehouse.db import normalize_database_url
from lakehouse.export import schemas
from lakehouse.export.bronze_export import (
    LocalWriter,
    S3Writer,
    build_writer,
    make_run_id,
    partition_key,
    run_export,
)
from lakehouse.seed.seed_demo_data import generate_dataset, insert_dataset

NOW = datetime(2026, 10, 9, 16, 30, 5, tzinfo=timezone.utc)
RUN_ID = '20261009T163005Z'


def read_parquet(source):
    # ParquetFile reads just the file. pq.read_table on a path would also turn the
    # ingest_date=/run_id= folder names into extra columns (Hive partition discovery).
    return pq.ParquetFile(io.BytesIO(source) if isinstance(source, bytes) else source).read()


@pytest.fixture
def seeded(engine):
    insert_dataset(engine, generate_dataset(n_papers=10))
    return engine


def test_partition_key_is_hive_style():
    assert make_run_id(NOW) == RUN_ID
    assert partition_key('bronze', 'claim_evidence', '2026-10-09', RUN_ID) == (
        'bronze/claim_evidence/ingest_date=2026-10-09/run_id=20261009T163005Z/part-00000.parquet'
    )


@pytest.mark.parametrize('url', [
    'postgresql+asyncpg://u:p@h:5432/db',
    'postgresql://u:p@h:5432/db',
    'postgres://u:p@h:5432/db',
])
def test_database_url_uses_sync_driver(url):
    assert normalize_database_url(url) == 'postgresql+psycopg://u:p@h:5432/db'


def test_local_export_writes_parquet_matching_source(seeded, tmp_path):
    manifest = run_export(seeded, LocalWriter(tmp_path), now=NOW)

    with seeded.connect() as conn:
        n_papers = conn.execute(text('SELECT count(*) FROM papers')).scalar_one()
        n_claims = conn.execute(text('SELECT count(*) FROM claim_evidence')).scalar_one()
    assert manifest['tables']['papers']['rows'] == n_papers
    assert manifest['tables']['claim_evidence']['rows'] == n_claims

    claims_file = tmp_path / partition_key('bronze', 'claim_evidence', '2026-10-09', RUN_ID)
    claims = read_parquet(claims_file)
    assert claims.num_rows == n_claims
    # Read back schema must be exactly the declared bronze schema (plus our table tag).
    assert claims.schema.remove_metadata().equals(schemas.CLAIM_EVIDENCE)
    assert set(claims.column('_run_id').to_pylist()) == {RUN_ID}
    assert set(claims.column('_source_table').to_pylist()) == {'claim_evidence'}


def test_papers_export_leaves_out_full_text(seeded, tmp_path):
    run_export(seeded, LocalWriter(tmp_path), now=NOW)
    papers = read_parquet(tmp_path / partition_key('bronze', 'papers', '2026-10-09', RUN_ID))
    assert 'full_text' not in papers.column_names
    assert str(papers.schema.field('created_at').type) == 'timestamp[us, tz=UTC]'


def test_empty_tables_still_write_typed_files(engine, tmp_path):
    manifest = run_export(engine, LocalWriter(tmp_path), now=NOW)
    assert manifest['tables']['claim_evidence']['rows'] == 0
    claims = read_parquet(tmp_path / partition_key('bronze', 'claim_evidence', '2026-10-09', RUN_ID))
    assert claims.num_rows == 0
    assert claims.schema.remove_metadata().equals(schemas.CLAIM_EVIDENCE)


def test_manifest_is_written_after_data(seeded, tmp_path):
    written = []

    class RecordingWriter(LocalWriter):
        def put(self, key, data):
            written.append(key)
            return super().put(key, data)

    manifest = run_export(seeded, RecordingWriter(tmp_path), now=NOW)
    assert written[-1] == f'bronze/_manifests/run_id={RUN_ID}.json'
    on_disk = json.loads((tmp_path / written[-1]).read_text())
    assert on_disk['tables'] == manifest['tables']


def test_batches_cover_every_row(seeded, tmp_path, monkeypatch):
    from lakehouse.export import bronze_export

    original = bronze_export.table_to_parquet
    monkeypatch.setattr(
        bronze_export, 'table_to_parquet',
        lambda conn, table, run_id, exported_at: original(conn, table, run_id, exported_at, batch_size=7),
    )
    manifest = run_export(seeded, LocalWriter(tmp_path), now=NOW)
    claims = read_parquet(tmp_path / partition_key('bronze', 'claim_evidence', '2026-10-09', RUN_ID))
    ids = claims.column('id').to_pylist()
    assert len(ids) == len(set(ids)) == manifest['tables']['claim_evidence']['rows']


def test_export_does_not_modify_source(seeded, tmp_path):
    def snapshot():
        with seeded.connect() as conn:
            return (conn.execute(text('SELECT * FROM papers ORDER BY id')).all(),
                    conn.execute(text('SELECT * FROM claim_evidence ORDER BY id')).all())

    before = snapshot()
    run_export(seeded, LocalWriter(tmp_path), now=NOW)
    assert snapshot() == before


@mock_aws
def test_s3_export_uploads_parquet_and_manifest(seeded, monkeypatch):
    # moto intercepts boto3 calls in-process, so no real AWS account is touched.
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')
    client = boto3.client('s3', region_name='us-east-1')
    client.create_bucket(Bucket='evidentia-lake-test')

    manifest = run_export(seeded, S3Writer('evidentia-lake-test', client=client), now=NOW)

    keys = sorted(o['Key'] for o in client.list_objects_v2(Bucket='evidentia-lake-test')['Contents'])
    assert keys == [
        f'bronze/_manifests/run_id={RUN_ID}.json',
        partition_key('bronze', 'claim_evidence', '2026-10-09', RUN_ID),
        partition_key('bronze', 'papers', '2026-10-09', RUN_ID),
    ]
    body = client.get_object(Bucket='evidentia-lake-test',
                             Key=partition_key('bronze', 'claim_evidence', '2026-10-09', RUN_ID))['Body'].read()
    assert read_parquet(body).num_rows == manifest['tables']['claim_evidence']['rows']
    assert manifest['tables']['papers']['uri'].startswith('s3://evidentia-lake-test/bronze/papers/')


def test_s3_target_requires_bucket(monkeypatch):
    monkeypatch.delenv('S3_BUCKET', raising=False)
    with pytest.raises(SystemExit, match='S3_BUCKET'):
        build_writer('s3')


@pytest.mark.skipif(not os.getenv('EVIDENTIA_TEST_DATABASE_URL'),
                    reason='set EVIDENTIA_TEST_DATABASE_URL to run against a real Postgres')
def test_export_against_real_postgres(tmp_path):
    """Integration test: runs the export in a read-only REPEATABLE READ transaction on Postgres."""
    from lakehouse.db import make_engine

    pg = make_engine(os.environ['EVIDENTIA_TEST_DATABASE_URL'])
    manifest = run_export(pg, LocalWriter(tmp_path), now=NOW)
    with pg.connect() as conn:
        assert manifest['tables']['claim_evidence']['rows'] == conn.execute(
            text('SELECT count(*) FROM claim_evidence')).scalar_one()
