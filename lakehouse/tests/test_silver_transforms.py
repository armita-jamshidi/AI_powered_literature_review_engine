"""Tests for the silver layer, run on a local SparkSession with Delta Lake.

Needs `pip install -r lakehouse/requirements-spark.txt` and Java 17+. The whole
module is skipped when pyspark isn't installed, so the bronze tests still run
without Spark.
"""
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip('pyspark')
pytest.importorskip('delta')

from pyspark.sql import Row, SparkSession  # noqa: E402
from sqlalchemy import text  # noqa: E402

from lakehouse.databricks import silver_transforms as st  # noqa: E402
from lakehouse.export.bronze_export import LocalWriter, run_export  # noqa: E402
from lakehouse.seed.seed_demo_data import generate_dataset, insert_dataset  # noqa: E402

T0 = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(scope='module')
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.master('local[2]').appName('evidentia-silver-tests')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.sql.warehouse.dir', str(tmp_path_factory.mktemp('warehouse')))
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.sql.shuffle.partitions', '2')
        .config('spark.ui.enabled', 'false')
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def schema(spark, request):
    name = 'silver_' + request.node.name.replace('[', '_').replace(']', '').replace('-', '_')
    spark.sql(f'DROP SCHEMA IF EXISTS {name} CASCADE')
    spark.sql(f'CREATE SCHEMA {name}')
    return name


def claim(id, paper_id=1, run='r1', exported=T0, **kw):
    base = dict(id=id, paper_id=paper_id, claim_key=f'cluster-0: statins -> ldl cholesterol', population='adults',
                intervention='statins', outcome='ldl cholesterol', result='negative', study_type='rct',
                confidence=0.9, _run_id=run, _exported_at=exported, _source_table='claim_evidence')
    base.update(kw)
    return Row(**base)


def paper(id, run='r1', exported=T0, filename=None, title=None):
    return Row(id=id, title=title or f'Paper {id}', source_filename=filename or f'paper_{id}.pdf',
               ingest_status='processed', ingest_error=None, created_at=T0, _run_id=run,
               _exported_at=exported, _source_table='papers')


def df(spark, rows, schema):
    return spark.createDataFrame(rows, schema)


def test_latest_versions_keeps_newest_row_and_flags_deletes(spark):
    t1 = T0 + timedelta(days=1)
    bronze = df(spark, [claim(1, run='r1'), claim(1, run='r2', exported=t1, result='positive'),
                        claim(2, run='r1')], st.BRONZE_CLAIMS_SCHEMA)
    out = {r.id: r for r in st.latest_versions(bronze, latest_run_id='r2').collect()}
    assert out[1].result == 'positive' and not out[1].is_deleted
    assert out[2].is_deleted  # id 2 is absent from the newest snapshot


def test_claims_are_normalized(spark):
    papers = st.build_silver_papers(df(spark, [paper(1)], st.BRONZE_PAPERS_SCHEMA), 'r1')
    bronze = df(spark, [
        claim(1, intervention='  Statins ', study_type='Randomized Controlled  Trial', confidence=85.0),
        claim(2, claim_key='cluster-7: statins -> ldl cholesterol', result='No Effect', study_type='cohort'),
        claim(3, claim_key='no cluster prefix', study_type='meta-analysis'),
    ], st.BRONZE_CLAIMS_SCHEMA)
    valid, rejects = st.build_silver_claims(bronze, papers, 'r1')
    rows = {r.claim_id: r for r in valid.collect()}

    assert rejects.count() == 0
    assert rows[1].intervention == 'statins'
    assert rows[1].study_type == 'rct' and rows[1].study_type_raw == 'randomized controlled trial'
    assert rows[1].confidence == pytest.approx(0.85) and rows[1].confidence_rescaled
    assert rows[2].result == 'null' and rows[2].result_raw == 'no effect'
    assert rows[2].study_type == 'observational' and rows[2].cluster_id == 7
    assert rows[3].study_type == 'other' and rows[3].cluster_id is None


@pytest.mark.parametrize('bad, reason', [
    (dict(result='strongly positive'), 'invalid_result'),
    (dict(confidence=250.0), 'invalid_confidence'),
    (dict(confidence=-0.1), 'invalid_confidence'),
    (dict(intervention='   '), 'missing_intervention_or_outcome'),
    (dict(paper_id=99), 'orphan_paper'),
])
def test_invalid_claims_are_rejected_with_reason(spark, bad, reason):
    papers = st.build_silver_papers(df(spark, [paper(1)], st.BRONZE_PAPERS_SCHEMA), 'r1')
    bronze = df(spark, [claim(1), claim(2, **bad)], st.BRONZE_CLAIMS_SCHEMA)
    valid, rejects = st.build_silver_claims(bronze, papers, 'r1')
    assert [r.claim_id for r in valid.collect()] == [1]
    assert [(r.claim_id, r.reject_reason) for r in rejects.collect()] == [(2, reason)]


def test_reuploaded_paper_claims_are_flagged_duplicate(spark):
    papers = st.build_silver_papers(df(spark, [
        paper(1, filename='a.pdf', title='Same'), paper(2, filename='a.pdf', title='Same'), paper(3),
    ], st.BRONZE_PAPERS_SCHEMA), 'r1')
    prows = {r.paper_id: r for r in papers.collect()}
    assert prows[2].canonical_paper_id == 1 and prows[2].is_duplicate_upload
    assert not prows[3].is_duplicate_upload

    bronze = df(spark, [claim(10, paper_id=1), claim(11, paper_id=2), claim(12, paper_id=3)],
                st.BRONZE_CLAIMS_SCHEMA)
    valid, _ = st.build_silver_claims(bronze, papers, 'r1')
    dupes = {r.claim_id: r.is_duplicate for r in valid.collect()}
    assert dupes == {10: False, 11: True, 12: False}


def test_end_to_end_from_bronze_export_is_idempotent(spark, schema, engine, tmp_path):
    """Seed -> bronze export (twice, as two runs) -> silver, then re-run silver."""
    insert_dataset(engine, generate_dataset(n_papers=20))
    run_export(engine, LocalWriter(tmp_path), now=T0)
    with engine.begin() as conn:  # a claim is deleted in Postgres between runs
        deleted_id = conn.execute(text('SELECT min(id) FROM claim_evidence')).scalar_one()
        conn.execute(text('DELETE FROM claim_evidence WHERE id = :id'), {'id': deleted_id})
        n_source = conn.execute(text('SELECT count(*) FROM claim_evidence')).scalar_one()
    run_export(engine, LocalWriter(tmp_path), now=T0 + timedelta(days=1))

    bronze_path = str(tmp_path / 'bronze')
    first = st.run_bronze_to_silver(spark, bronze_path, schema)
    assert first['runs_read'] == 2

    claims = spark.table(f'{schema}.claims')
    # One row per source id across both runs (the deleted one is kept, flagged).
    assert claims.count() == claims.select('claim_id').distinct().count()
    assert first['claims'] + first['claims_rejected'] == n_source + 1
    assert claims.where(f'claim_id = {deleted_id}').first().is_deleted
    # The seed's two "improved" results can't be mapped; "no effect" maps to null.
    assert spark.table(f'{schema}.claims_rejects').groupBy('reject_reason').count().collect() == [
        Row(reject_reason='invalid_result', count=first['claims_rejected'])
    ]
    assert claims.where("result_raw = 'no effect'").count() > 0
    version_after_first = spark.sql(f'DESCRIBE HISTORY {schema}.claims').count()

    second = st.run_bronze_to_silver(spark, bronze_path, schema)
    assert second == first
    # A MERGE ran, but no row changed: every row_hash matched.
    last_merge = spark.sql(f'DESCRIBE HISTORY {schema}.claims').orderBy('version', ascending=False).first()
    assert last_merge.operation == 'MERGE'
    assert int(last_merge.operationMetrics['numTargetRowsUpdated']) == 0
    assert int(last_merge.operationMetrics['numTargetRowsInserted']) == 0
    assert spark.sql(f'DESCRIBE HISTORY {schema}.claims').count() == version_after_first + 1
