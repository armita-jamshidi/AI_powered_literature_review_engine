"""Bronze -> silver transformations, as plain PySpark DataFrame functions.

The Databricks notebook (01_bronze_to_silver.py) is a thin driver around these
functions. Keeping the logic here, with no notebook globals, means the same code
runs on Databricks serverless and in local pytest with a local SparkSession.

Only DataFrame and SQL APIs are used (no RDDs, no sparkContext, no caching),
because Databricks serverless runs on Spark Connect, which doesn't offer those.

What silver guarantees, compared with bronze:
  * an explicit, enforced schema
  * one row per source id (bronze has one row per id *per export run*)
  * rows deleted in Postgres are flagged `is_deleted` instead of lingering
  * normalized text, study types, results and confidences
  * invalid rows routed to a rejects table with a reason, never silently dropped
  * duplicate uploads of the same paper (and their claims) flagged
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# --- Schemas ----------------------------------------------------------------
# Mirrors lakehouse/export/schemas.py. Reading with an explicit schema instead of
# letting Spark infer it means a drifted file fails loudly here rather than
# quietly changing a column's type downstream.

_LINEAGE = [
    T.StructField('_run_id', T.StringType(), False),
    T.StructField('_exported_at', T.TimestampType(), False),
    T.StructField('_source_table', T.StringType(), False),
]

BRONZE_PAPERS_SCHEMA = T.StructType([
    T.StructField('id', T.LongType(), False),
    T.StructField('title', T.StringType()),
    T.StructField('source_filename', T.StringType()),
    T.StructField('ingest_status', T.StringType()),
    T.StructField('ingest_error', T.StringType()),
    T.StructField('created_at', T.TimestampType()),
    *_LINEAGE,
])

BRONZE_CLAIMS_SCHEMA = T.StructType([
    T.StructField('id', T.LongType(), False),
    T.StructField('paper_id', T.LongType()),
    T.StructField('claim_key', T.StringType()),
    T.StructField('population', T.StringType()),
    T.StructField('intervention', T.StringType()),
    T.StructField('outcome', T.StringType()),
    T.StructField('result', T.StringType()),
    T.StructField('study_type', T.StringType()),
    T.StructField('confidence', T.DoubleType()),
    *_LINEAGE,
])

# --- Vocabularies -----------------------------------------------------------

RESULT_VALUES = ('positive', 'negative', 'null', 'mixed')
# Phrases an LLM uses instead of the allowed labels, mapped to what they mean.
RESULT_SYNONYMS = {
    'no effect': 'null',
    'no significant effect': 'null',
    'not significant': 'null',
    'none': 'null',
    'inconclusive': 'mixed',
}

# Raw spellings -> canonical study design. Anything not listed becomes 'other'.
STUDY_TYPE_CANONICAL = {
    'rct': 'rct',
    'randomized controlled trial': 'rct',
    'randomised controlled trial': 'rct',
    'randomized trial': 'rct',
    'quasi-experimental': 'quasi-experimental',
    'observational': 'observational',
    'cohort': 'observational',
    'cohort study': 'observational',
    'prospective cohort': 'observational',
    'retrospective cohort': 'observational',
    'case-control': 'case-control',
    'case control': 'case-control',
    'cross-sectional': 'cross-sectional',
    'cross sectional': 'cross-sectional',
    'case report': 'case report',
}


# --- Reading bronze ---------------------------------------------------------

def complete_run_ids(spark: SparkSession, bronze_path: str) -> list[str]:
    """Run ids that have a manifest. The export writes the manifest last, so a
    run without one crashed partway and must not be read."""
    manifests = spark.read.option('multiLine', True).json(f'{bronze_path}/_manifests/')
    return sorted(r.run_id for r in manifests.select('run_id').distinct().collect())


def read_bronze(spark: SparkSession, bronze_path: str, table: str, schema: T.StructType,
                run_ids: list[str]) -> DataFrame:
    df = spark.read.schema(schema).parquet(f'{bronze_path}/{table}/')
    # Filter on the _run_id data column rather than the run_id= folder name, so
    # the result doesn't depend on partition discovery settings.
    return df.where(F.col('_run_id').isin(run_ids)).select(*schema.fieldNames())


# --- Generic helpers --------------------------------------------------------

def norm_text(col: str | Column) -> Column:
    """Lowercase, trim and collapse internal whitespace; empty strings become NULL."""
    cleaned = F.lower(F.trim(F.regexp_replace(F.col(col) if isinstance(col, str) else col, r'\s+', ' ')))
    return F.when(cleaned == '', None).otherwise(cleaned)


def _map_values(col: Column, mapping: dict[str, str], default: Column) -> Column:
    expr = None
    for raw, canonical in mapping.items():
        expr = (F.when(col == raw, canonical) if expr is None else expr.when(col == raw, canonical))
    return expr.otherwise(default)


def latest_versions(df: DataFrame, latest_run_id: str, key: str = 'id') -> DataFrame:
    """Keep each id's most recent bronze row and flag ids missing from the latest run.

    Every export is a full snapshot, so an id that isn't in the newest run was
    deleted in Postgres. We keep it, flagged, instead of erasing history.
    """
    w = Window.partitionBy(key).orderBy(F.col('_exported_at').desc(), F.col('_run_id').desc())
    return (
        df.withColumn('_rn', F.row_number().over(w))
        .where('_rn = 1')
        .drop('_rn')
        .withColumn('is_deleted', F.col('_run_id') != F.lit(latest_run_id))
    )


def _row_hash(*cols: str) -> Column:
    """Fingerprint of the business columns, used by MERGE to skip unchanged rows."""
    return F.sha2(F.concat_ws('||', *[F.coalesce(F.col(c).cast('string'), F.lit('\u0000')) for c in cols]), 256)


# --- Papers -----------------------------------------------------------------

def build_silver_papers(bronze_papers: DataFrame, latest_run_id: str) -> DataFrame:
    papers = latest_versions(bronze_papers, latest_run_id).select(
        F.col('id').alias('paper_id'),
        F.trim('title').alias('title'),
        F.trim('source_filename').alias('source_filename'),
        norm_text('ingest_status').alias('ingest_status'),
        'ingest_error',
        'created_at',
        F.col('source_filename').startswith('synthetic/').alias('is_synthetic'),
        'is_deleted',
        F.col('_run_id').alias('_last_run_id'),
        F.col('_exported_at').alias('_last_exported_at'),
    )
    # The same file uploaded twice becomes two paper rows. The earliest id is the
    # canonical copy; later ones are flagged so their claims aren't double-counted.
    same_upload = Window.partitionBy(F.lower('source_filename'), F.lower('title'))
    live_id = F.when(~F.col('is_deleted'), F.col('paper_id'))
    papers = papers.withColumn(
        'canonical_paper_id', F.coalesce(F.min(live_id).over(same_upload), F.col('paper_id'))
    ).withColumn('is_duplicate_upload', F.col('paper_id') != F.col('canonical_paper_id'))
    return papers.withColumn('row_hash', _row_hash(
        'title', 'source_filename', 'ingest_status', 'ingest_error', 'created_at',
        'is_deleted', 'canonical_paper_id',
    ))


# --- Claims -----------------------------------------------------------------

def _clean_claim_columns(claims: DataFrame) -> DataFrame:
    result_raw = norm_text('result')
    study_raw = norm_text('study_type')
    conf = F.col('confidence')
    return claims.select(
        F.col('id').alias('claim_id'),
        'paper_id',
        # claim_key looks like "cluster-3: aspirin -> mortality". The cluster id is
        # only meaningful within one paper (the app clusters per paper).
        F.regexp_extract('claim_key', r'^cluster-(\d+):', 1).alias('_cluster_str'),
        norm_text('population').alias('population'),
        norm_text('intervention').alias('intervention'),
        norm_text('outcome').alias('outcome'),
        result_raw.alias('result_raw'),
        F.when(result_raw.isin(*RESULT_VALUES), result_raw)
        .otherwise(_map_values(result_raw, RESULT_SYNONYMS, F.lit(None))).alias('result'),
        study_raw.alias('study_type_raw'),
        _map_values(study_raw, STUDY_TYPE_CANONICAL, F.lit('other')).alias('study_type'),
        conf.alias('confidence_raw'),
        # Some extractions report 85 meaning 85%. Rescale (0, 100] to (0, 1];
        # anything else outside [0, 1] can't be interpreted and is rejected.
        F.when(conf.between(0, 1), conf)
        .when((conf > 1) & (conf <= 100), conf / 100)
        .alias('confidence'),
        ((conf > 1) & (conf <= 100)).alias('confidence_rescaled'),
        'is_deleted',
        F.col('_run_id').alias('_last_run_id'),
        F.col('_exported_at').alias('_last_exported_at'),
    ).withColumn(
        'cluster_id', F.when(F.col('_cluster_str') != '', F.col('_cluster_str').cast('int'))
    ).drop('_cluster_str')


def build_silver_claims(bronze_claims: DataFrame, silver_papers: DataFrame,
                        latest_run_id: str) -> tuple[DataFrame, DataFrame]:
    """Return (valid claims, rejected claims)."""
    claims = _clean_claim_columns(latest_versions(bronze_claims, latest_run_id))
    papers = silver_papers.select('paper_id', 'canonical_paper_id', F.col('is_deleted').alias('_paper_deleted'))
    claims = claims.join(papers, on='paper_id', how='left')

    reject_reason = (
        F.when(F.col('paper_id').isNull() | F.col('canonical_paper_id').isNull(), 'orphan_paper')
        .when(F.col('result').isNull(), 'invalid_result')
        .when(F.col('confidence').isNull(), 'invalid_confidence')
        .when(F.col('intervention').isNull() | F.col('outcome').isNull(), 'missing_intervention_or_outcome')
    )
    claims = claims.withColumn('reject_reason', reject_reason)

    rejects = claims.where(F.col('reject_reason').isNotNull()).drop('_paper_deleted')
    valid = (
        claims.where(F.col('reject_reason').isNull())
        .drop('reject_reason')
        .withColumn('is_deleted', F.col('is_deleted') | F.col('_paper_deleted'))
        .drop('_paper_deleted')
    )

    # The same finding reported twice for one paper (e.g. from a re-upload) is
    # counted once: keep the lowest claim_id among live rows with the same content.
    same_finding = Window.partitionBy(
        'canonical_paper_id', 'population', 'intervention', 'outcome', 'result', 'study_type', 'is_deleted',
    ).orderBy('claim_id')
    valid = valid.withColumn('is_duplicate', F.row_number().over(same_finding) > 1)
    valid = valid.withColumn('row_hash', _row_hash(
        'paper_id', 'canonical_paper_id', 'cluster_id', 'population', 'intervention', 'outcome',
        'result', 'study_type', 'confidence', 'is_deleted', 'is_duplicate',
    ))
    return valid, rejects


# --- Writing Delta ----------------------------------------------------------

def merge_into_delta(spark: SparkSession, df: DataFrame, table: str, key: str) -> None:
    """Upsert df into a Delta table keyed on `key`.

    First run: create the table. Later runs: MERGE, updating only rows whose
    row_hash changed and inserting new keys. Re-running with the same bronze
    data changes nothing, so the job is idempotent.
    """
    df = df.withColumn('_silver_updated_at', F.current_timestamp())
    if not spark.catalog.tableExists(table):
        df.write.format('delta').saveAsTable(table)
        return
    df.createOrReplaceTempView('_silver_src')
    spark.sql(f"""
        MERGE INTO {table} AS t
        USING _silver_src AS s
        ON t.{key} = s.{key}
        WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def overwrite_delta(df: DataFrame, table: str) -> None:
    df.withColumn('_silver_updated_at', F.current_timestamp()).write.format('delta') \
        .mode('overwrite').option('overwriteSchema', 'true').saveAsTable(table)


def run_bronze_to_silver(spark: SparkSession, bronze_path: str, schema: str) -> dict:
    """Read every complete bronze run, build silver, write the Delta tables."""
    run_ids = complete_run_ids(spark, bronze_path)
    if not run_ids:
        raise ValueError(f'No complete bronze runs (no manifests) under {bronze_path}')
    latest = run_ids[-1]  # run ids are UTC timestamps, so they sort chronologically

    papers = build_silver_papers(read_bronze(spark, bronze_path, 'papers', BRONZE_PAPERS_SCHEMA, run_ids), latest)
    claims, rejects = build_silver_claims(
        read_bronze(spark, bronze_path, 'claim_evidence', BRONZE_CLAIMS_SCHEMA, run_ids), papers, latest,
    )

    merge_into_delta(spark, papers, f'{schema}.papers', 'paper_id')
    merge_into_delta(spark, claims, f'{schema}.claims', 'claim_id')
    # Rejects describe the current state of the source, so they're replaced each run.
    overwrite_delta(rejects, f'{schema}.claims_rejects')

    return {
        'runs_read': len(run_ids),
        'latest_run_id': latest,
        'papers': spark.table(f'{schema}.papers').count(),
        'claims': spark.table(f'{schema}.claims').count(),
        'claims_rejected': spark.table(f'{schema}.claims_rejects').count(),
    }
