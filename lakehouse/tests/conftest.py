import importlib.util
import sqlite3

import pytest
from sqlalchemy import create_engine, text

# Same columns as backend/app/models.py. SQLite stands in for Postgres so the
# unit tests need no database server.
DDL = (
    """CREATE TABLE papers (
        id INTEGER PRIMARY KEY, title VARCHAR(512) NOT NULL, source_filename VARCHAR(256) NOT NULL,
        full_text TEXT NOT NULL, ingest_status VARCHAR(32) NOT NULL, ingest_error TEXT,
        created_at TIMESTAMP NOT NULL)""",
    """CREATE TABLE claim_evidence (
        id INTEGER PRIMARY KEY, paper_id INTEGER NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
        claim_key VARCHAR(512) NOT NULL, population VARCHAR(512) NOT NULL, intervention VARCHAR(512) NOT NULL,
        outcome VARCHAR(512) NOT NULL, result VARCHAR(16) NOT NULL, study_type VARCHAR(64) NOT NULL,
        confidence FLOAT NOT NULL)""",
)


@pytest.fixture
def engine():
    # PARSE_DECLTYPES makes sqlite3 return TIMESTAMP columns as datetime, like Postgres does.
    eng = create_engine('sqlite://', connect_args={'detect_types': sqlite3.PARSE_DECLTYPES})
    with eng.begin() as conn:
        conn.execute(text('PRAGMA foreign_keys = ON'))
        for stmt in DDL:
            conn.execute(text(stmt))
    yield eng
    eng.dispose()


@pytest.fixture(scope='session')
def spark(tmp_path_factory):
    """One local Spark session with Delta Lake, shared by the silver and dbt tests."""
    if not (importlib.util.find_spec('pyspark') and importlib.util.find_spec('delta')):
        pytest.skip('pyspark/delta-spark not installed (pip install -r lakehouse/requirements-spark.txt)')
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master('local[2]').appName('evidentia-lakehouse-tests')
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
