# Databricks notebook source
# MAGIC %md
# MAGIC # Probe: can Databricks Free Edition reach the S3 bronze bucket?
# MAGIC
# MAGIC Run this once before building silver. It answers two separate questions:
# MAGIC
# MAGIC 1. **Network:** can this workspace's serverless compute open a connection to S3 at all?
# MAGIC    Free Edition limits outbound internet to a list of trusted domains.
# MAGIC 2. **Storage access:** can Spark read `s3://` paths? On a paid workspace you'd grant that
# MAGIC    through a Unity Catalog *storage credential* (an IAM role) plus an *external location*.
# MAGIC    Free Edition doesn't let you configure your own storage, so we expect this to fail.
# MAGIC
# MAGIC No AWS keys are used or needed here. If step 2 fails, the fallback is to land the same
# MAGIC Parquet files in a Unity Catalog volume (`--target s3,volume` in the export job), which step 4 creates.

# COMMAND ----------

dbutils.widgets.text('s3_bronze_path', 's3://YOUR-BUCKET/bronze', 'S3 bronze path')
s3_bronze_path = dbutils.widgets.get('s3_bronze_path').rstrip('/')
bucket = s3_bronze_path.removeprefix('s3://').split('/')[0]
print(f'Bucket: {bucket}\nPath:   {s3_bronze_path}')

# COMMAND ----------

# MAGIC %md ## 1. Network reachability
# MAGIC An HTTP 403 (AccessDenied) from S3 is a *good* result here: it means the request got through and
# MAGIC S3 refused it only because we sent no credentials. A timeout or connection error means
# MAGIC outbound traffic to S3 is blocked.

# COMMAND ----------

import urllib.error
import urllib.request

try:
    urllib.request.urlopen(f'https://{bucket}.s3.amazonaws.com/', timeout=10)
    network = 'reachable (public response)'
except urllib.error.HTTPError as e:
    network = f'reachable (S3 answered HTTP {e.code})'
except Exception as e:  # noqa: BLE001 - we want to report any failure
    network = f'BLOCKED: {type(e).__name__}: {e}'
print('Network to S3:', network)

# COMMAND ----------

# MAGIC %md ## 2. Spark reading `s3://` directly

# COMMAND ----------

try:
    spark.read.parquet(f'{s3_bronze_path}/claim_evidence/').limit(1).collect()
    spark_s3 = 'WORKS'
except Exception as e:  # noqa: BLE001
    spark_s3 = f'FAILS: {type(e).__name__}: {str(e)[:300]}'
print('Spark read from S3:', spark_s3)

# COMMAND ----------

# MAGIC %md ## 3. Can we create a storage credential? (needed for an external location)

# COMMAND ----------

try:
    rows = spark.sql('SHOW STORAGE CREDENTIALS').collect()
    credentials = f'{len(rows)} storage credential(s) visible'
except Exception as e:  # noqa: BLE001
    credentials = f'not available: {type(e).__name__}: {str(e)[:300]}'
print('Storage credentials:', credentials)

# COMMAND ----------

# MAGIC %md ## 4. Create the landing volume (the fallback target)
# MAGIC A Unity Catalog **volume** is governed file storage inside Databricks. Free Edition
# MAGIC stores it in Databricks-managed cloud storage, which serverless compute can always read.

# COMMAND ----------

spark.sql('CREATE SCHEMA IF NOT EXISTS workspace.evidentia')
spark.sql('CREATE VOLUME IF NOT EXISTS workspace.evidentia.landing')
print('Volume ready at /Volumes/workspace/evidentia/landing')

# COMMAND ----------

print(f"""
Summary
-------
Network to S3:        {network}
Spark read from S3:   {spark_s3}
Storage credentials:  {credentials}

If Spark can't read S3, use the volume fallback (the volume above now exists):
  Locally:  python -m lakehouse.export.bronze_export --target s3,volume
  Then run 01_bronze_to_silver with bronze_path = /Volumes/workspace/evidentia/landing/bronze
""")
