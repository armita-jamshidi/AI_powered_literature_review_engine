"""Runs the dbt gold project end to end on local Spark + Delta.

Seed -> bronze export -> PySpark silver -> `dbt build` (models, seed and every
dbt test) on the `local_spark` target, then checks a few numbers against the
Python implementation the API uses. On Databricks the same project runs on the
`databricks` target; this test only proves the SQL and the tests are sound.
"""
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip('pyspark')
pytest.importorskip('delta')
pytest.importorskip('dbt.cli.main')

from lakehouse.databricks.silver_transforms import run_bronze_to_silver  # noqa: E402
from lakehouse.export.bronze_export import LocalWriter, run_export  # noqa: E402
from lakehouse.seed.seed_demo_data import generate_dataset, insert_dataset  # noqa: E402

DBT_DIR = Path(__file__).resolve().parents[1] / 'dbt'

# Copied from backend/app/pipeline.py so this test needs none of the app's dependencies.
STUDY_WEIGHTS = {
    'rct': 1.0, 'randomized controlled trial': 1.0, 'quasi-experimental': 0.8, 'observational': 0.6,
    'case-control': 0.5, 'cross-sectional': 0.45, 'case report': 0.3,
}


def weighted_majority_fraction(results, study_types):
    totals, total_weight = {}, 0.0
    for result, study_type in zip(results, study_types):
        weight = STUDY_WEIGHTS.get(study_type.lower(), 0.5)
        totals[result] = totals.get(result, 0.0) + weight
        total_weight += weight
    return max(totals.values()) / total_weight if total_weight else 0.0


@pytest.fixture(scope='module')
def gold(spark, tmp_path_factory):
    import sqlite3

    from sqlalchemy import create_engine, text

    from lakehouse.tests.conftest import DDL

    engine = create_engine('sqlite://', connect_args={'detect_types': sqlite3.PARSE_DECLTYPES})
    with engine.begin() as conn:
        for stmt in DDL:
            conn.execute(text(stmt))
    insert_dataset(engine, generate_dataset())
    lake = tmp_path_factory.mktemp('lake')
    run_export(engine, LocalWriter(lake), now=datetime(2026, 10, 1, tzinfo=timezone.utc))

    spark.sql('CREATE SCHEMA IF NOT EXISTS evidentia_silver')
    run_bronze_to_silver(spark, str(lake / 'bronze'), 'evidentia_silver')

    from dbt.cli.main import dbtRunner

    out = tmp_path_factory.mktemp('dbt')
    result = dbtRunner().invoke([
        'build', '--project-dir', str(DBT_DIR), '--profiles-dir', str(DBT_DIR), '--target', 'local_spark',
        '--target-path', str(out / 'target'), '--log-path', str(out / 'logs'),
    ])
    return spark, result


def test_dbt_build_passes_every_model_seed_and_test(gold):
    _, result = gold
    assert result.success, result.exception
    statuses = defaultdict(int)
    for r in result.result:
        statuses[(r.node.resource_type, str(r.status))] += 1
    assert statuses[('model', 'success')] == 7
    assert statuses[('seed', 'success')] == 1
    assert not [r.node.name for r in result.result if str(r.status) not in ('success', 'pass')]
    assert sum(n for (kind, _), n in statuses.items() if kind == 'test') >= 20


def test_weighted_disagreement_matches_api_formula(gold):
    spark, _ = gold
    claims = spark.table('evidentia_gold.fct_claims').select('intervention', 'outcome', 'result', 'study_type').collect()
    by_pair = defaultdict(lambda: ([], []))
    for c in claims:
        by_pair[(c.intervention, c.outcome)][0].append(c.result)
        by_pair[(c.intervention, c.outcome)][1].append(c.study_type)

    gold_rows = spark.table('evidentia_gold.gold_weighted_disagreement').collect()
    assert len(gold_rows) == len(by_pair)
    for row in gold_rows:
        results, types = by_pair[(row.intervention, row.outcome)]
        expected = round(1 - weighted_majority_fraction(results, types), 4)
        assert row.weighted_disagreement_score == pytest.approx(expected, abs=1e-4), row.claim_pair
