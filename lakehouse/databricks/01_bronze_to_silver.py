# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze → Silver (PySpark → Delta Lake)
# MAGIC
# MAGIC Reads the bronze Parquet snapshots written by `lakehouse/export/bronze_export.py`, then cleans,
# MAGIC deduplicates and validates them, and writes three Delta tables:
# MAGIC
# MAGIC | Table | Grain | Notes |
# MAGIC |---|---|---|
# MAGIC | `<schema>.papers` | one row per paper id | `canonical_paper_id` / `is_duplicate_upload` flag re-uploads |
# MAGIC | `<schema>.claims` | one row per claim id | normalized; `is_duplicate`, `is_deleted` flags |
# MAGIC | `<schema>.claims_rejects` | one row per rejected claim | `reject_reason` says why |
# MAGIC
# MAGIC All transformation logic lives in `silver_transforms.py` next to this notebook, so it's
# MAGIC unit-tested locally (`lakehouse/tests/test_silver_transforms.py`).
# MAGIC
# MAGIC **Run it from a Git folder** (Workspace → Create → Git folder → this repo) so the import below works.

# COMMAND ----------

dbutils.widgets.text('bronze_path', '/Volumes/workspace/evidentia/landing/bronze', 'Bronze path')
dbutils.widgets.text('catalog', 'workspace', 'Catalog')
dbutils.widgets.text('silver_schema', 'evidentia_silver', 'Silver schema')

bronze_path = dbutils.widgets.get('bronze_path').rstrip('/')
catalog = dbutils.widgets.get('catalog')
silver_schema = f"{catalog}.{dbutils.widgets.get('silver_schema')}"

# The landing volume for bronze files, and the schema for silver tables.
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {catalog}.evidentia')
spark.sql(f'CREATE VOLUME IF NOT EXISTS {catalog}.evidentia.landing')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {silver_schema}')
print(f'Reading {bronze_path} -> writing {silver_schema}')

# COMMAND ----------

# In a Git folder, the notebook's own directory is on sys.path.
from silver_transforms import run_bronze_to_silver

summary = run_bronze_to_silver(spark, bronze_path, silver_schema)
summary

# COMMAND ----------

# MAGIC %md ## Data quality checks
# MAGIC These fail the run (and a scheduled job) if silver breaks its own guarantees.

# COMMAND ----------

claims = spark.table(f'{silver_schema}.claims')
papers = spark.table(f'{silver_schema}.papers')

assert claims.count() == claims.select('claim_id').distinct().count(), 'claim_id is not unique'
assert papers.count() == papers.select('paper_id').distinct().count(), 'paper_id is not unique'
assert claims.where("result NOT IN ('positive','negative','null','mixed')").count() == 0, 'bad result value'
assert claims.where('confidence < 0 OR confidence > 1').count() == 0, 'confidence out of [0, 1]'
orphans = claims.join(papers, 'paper_id', 'left_anti').count()
assert orphans == 0, f'{orphans} claims point at missing papers'
print('All silver checks passed.')

# COMMAND ----------

# MAGIC %md ## What got rejected, and why

# COMMAND ----------

display(spark.sql(f"""
    SELECT reject_reason, count(*) AS n, collect_set(result_raw) AS example_results
    FROM {silver_schema}.claims_rejects GROUP BY reject_reason
"""))

# COMMAND ----------

# MAGIC %md ## Delta Lake features worth showing
# MAGIC Every write is a versioned, ACID commit in the table's transaction log (`_delta_log/`).
# MAGIC `DESCRIBE HISTORY` lists them; `VERSION AS OF` reads the table as it was (time travel).

# COMMAND ----------

display(spark.sql(f'DESCRIBE HISTORY {silver_schema}.claims')
        .select('version', 'timestamp', 'operation', 'operationMetrics'))

# COMMAND ----------

display(spark.sql(f'SELECT count(*) AS claims_in_version_0 FROM {silver_schema}.claims VERSION AS OF 0'))
