# Evidentia lakehouse

This folder holds the data engineering layer that sits beside the Evidentia app.
The app stays an OLTP system (Postgres, serving the API). The lakehouse copies
its data out and reshapes it for analytics, so heavy queries never hit the
app's database.

```mermaid
flowchart LR
    PG[(Postgres<br/>papers, claim_evidence)] -->|bronze_export.py<br/>pyarrow + boto3| B[BRONZE<br/>S3 Parquet<br/>ingest_date=YYYY-MM-DD]
    B -->|PySpark| S[SILVER<br/>Delta tables]
    S -->|dbt| G[GOLD<br/>Delta tables]
    G --> BI[Power BI]
```

Bronze and silver exist so far. Gold (dbt) and the dashboard arrive in later PRs.

## Layout

| Path | What it is |
|---|---|
| `db.py` | Builds a synchronous SQLAlchemy engine from the app's `DATABASE_URL`. |
| `seed/seed_demo_data.py` | Inserts clearly labeled synthetic papers and claims, so everything runs without an OpenAI key. |
| `export/bronze_export.py` | The bronze export job: Postgres → Parquet → local folder or S3. |
| `export/schemas.py` | The explicit Parquet schemas for bronze. |
| `databricks/00_probe_s3_access.py` | Databricks notebook: checks whether Free Edition can reach your S3 bucket, and creates the landing volume. |
| `databricks/01_bronze_to_silver.py` | Databricks notebook: runs the silver job and its data quality checks. |
| `databricks/silver_transforms.py` | The silver logic as plain PySpark functions (imported by the notebook, unit-tested locally). |
| `tests/` | pytest suite. SQLite stands in for Postgres, `moto` fakes S3, and a local Spark session runs the silver tests. |

## Bronze: what the export job does

1. Opens **one read-only, REPEATABLE READ transaction** on Postgres. Both tables are read from the same snapshot, so every exported claim's `paper_id` exists in the same run's papers, even while the app keeps ingesting.
2. Streams each table out in batches of 5,000 rows into a Parquet file with a fixed schema. `papers.full_text` is left out because it's large and no analytics need it.
3. Adds lineage columns to every row: `_run_id`, `_exported_at`, `_source_table`.
4. Writes `bronze/<table>/ingest_date=YYYY-MM-DD/run_id=<run_id>/part-00000.parquet`.
5. Writes `bronze/_manifests/run_id=<run_id>.json` **last**. A run without a manifest crashed partway, and silver will skip it.

Each run is a **full snapshot**: a claim exported on Monday and again on Tuesday appears in both partitions. That is normal for bronze, which keeps history raw. Silver deduplicates it.

## Silver: what the PySpark job does

`silver_transforms.run_bronze_to_silver` reads **every complete bronze run** (one with a manifest) and writes three Delta tables:

1. **Enforces a schema.** Parquet is read with an explicit `StructType`, so a drifted file fails loudly instead of changing a column type downstream.
2. **Dedupes snapshots.** Bronze holds each row once *per run*. A window (`row_number() over (partition by id order by _exported_at desc)`) keeps the newest version of each id. An id missing from the newest snapshot was deleted in Postgres, so it's kept and flagged `is_deleted` (a *soft delete*).
3. **Normalizes values.** Text is trimmed, lowercased and whitespace-collapsed. `study_type` spellings map to one label (`randomized controlled trial` → `rct`, `cohort` → `observational`). Results like `no effect` map to `null`. Confidences on a 0–100 scale are rescaled to 0–1 and flagged `confidence_rescaled`. The cluster id is parsed out of `claim_key`.
4. **Routes bad rows to `claims_rejects`** with a `reject_reason` (`invalid_result`, `invalid_confidence`, `orphan_paper`, `missing_intervention_or_outcome`) rather than dropping them silently.
5. **Flags semantic duplicates.** Re-uploads of the same file point to the earliest paper (`canonical_paper_id`). The same finding repeated for one canonical paper is flagged `is_duplicate`, so gold counts it once.
6. **Writes with Delta `MERGE`.** New ids are inserted. Existing ids are updated only when their `row_hash` (a SHA-256 of the business columns) changed. Re-running on the same bronze data changes nothing, which makes the job **idempotent**.

On the seeded data (42 papers, 196 claims): 2 claims are rejected (`invalid_result`), 4 confidences are rescaled, 3 `no effect` results map to `null`, 2 papers are flagged as re-uploads, and 33 claims are flagged as duplicates. Most of those duplicates are the seed reporting the same finding twice within one paper.

## Running it locally (Windows PowerShell)

```powershell
# 1. Start Postgres + the app once, so the app creates its tables
docker compose up -d --build

# 2. Create a virtual environment for the lakehouse jobs (from the repo root)
python -m venv .venv-lakehouse
.\.venv-lakehouse\Scripts\Activate.ps1
pip install -r lakehouse\requirements-dev.txt

# 3. Config: copy the example and edit it (lakehouse\.env is gitignored)
Copy-Item lakehouse\.env.example lakehouse\.env

# 4. Seed synthetic data, export to a local folder, run the tests
python -m lakehouse.seed.seed_demo_data --reset
python -m lakehouse.export.bronze_export --target local
pytest
```

The local export lands in `.\lake\bronze\...` (gitignored).

The silver tests need Spark (`pip install -r lakehouse\requirements-spark.txt` and Java 17+). Without Spark installed they're skipped. Running Spark on Windows also needs Hadoop's `winutils.exe`, so it's easier to run the silver job in Databricks, which is what it's for.

To also run the Postgres integration test, set `$env:EVIDENTIA_TEST_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/evidentia"` before `pytest`.

## Setting up AWS (you do this; nothing here creates cloud resources for you)

**Cost:** the bronze files are a few hundred KB. S3 storage and request charges at that size come to fractions of a cent per month. Step 1 adds an alert so any spend at all emails you.

1. **Turn on a spending alert.** Sign in to the AWS console as the root user (turn on MFA for root if you haven't). Go to **Billing and Cost Management → Budgets → Create budget**, pick the **Zero spend budget** template, enter your email, and create it.
2. **Create the bucket.** Go to **S3 → Create bucket**.
   - Name: something globally unique, e.g. `evidentia-lake-<yourname>-2026`.
   - Region: `us-east-1`, or any region; put the same one in `AWS_DEFAULT_REGION`.
   - Object Ownership: *ACLs disabled*. **Block all public access: on.** Leave versioning off and default encryption (SSE-S3) on.
3. **Create a least-privilege policy.** Go to **IAM → Policies → Create policy → JSON**, paste the following with your bucket name, and name it `EvidentiaLakeAccess`:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {"Effect": "Allow", "Action": "s3:ListBucket",
        "Resource": "arn:aws:s3:::YOUR-BUCKET"},
       {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"],
        "Resource": "arn:aws:s3:::YOUR-BUCKET/*"}
     ]
   }
   ```
   It allows reading and writing objects in this one bucket and nothing else. It can't delete objects, so a bug in the job can't wipe the lake.
4. **Create the IAM user.** Go to **IAM → Users → Create user**, name it `evidentia-lake-exporter`, and leave console access *off*. Choose **Attach policies directly**, pick `EvidentiaLakeAccess`, and create the user.
5. **Create an access key.** Open the user, then **Security credentials → Create access key**. Choose the use case **Local code** and copy both values into `lakehouse\.env` (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`), plus `S3_BUCKET`. AWS shows the secret only once. Never paste it into code, a commit, or a chat.
6. **Run it:** `python -m lakehouse.export.bronze_export --target s3`, then look in the bucket in the S3 console.

### Teardown

1. **S3** → select the bucket → **Empty** → **Delete**.
2. **IAM → Users** → delete `evidentia-lake-exporter`. Its access keys are deleted with it.
3. **IAM → Policies** → delete `EvidentiaLakeAccess`.
4. Optionally delete the zero-spend budget. It's free to keep.

## Setting up Databricks Free Edition

Free Edition is free and has no credit card. If you go over its daily compute quota, compute pauses until the next day; it never bills you.

1. **Add the repo as a Git folder.** In the workspace, go to **Workspace → Create → Git folder** and paste `https://github.com/armita-jamshidi/AI_powered_literature_review_engine`. The repo is public, so no GitHub credentials are needed to read it.
2. **Run the probe.** Open `lakehouse/databricks/00_probe_s3_access`, set the `s3_bronze_path` widget to `s3://YOUR-BUCKET/bronze`, and **Run all**. It reports whether Spark can read your bucket and creates the landing volume `/Volumes/workspace/evidentia/landing`.
3. **Create a personal access token.** Click your avatar, then **Settings → Developer → Access tokens → Generate new token**. Name it `evidentia-local` and give it a 90-day lifetime. Put it in `lakehouse\.env` as `DATABRICKS_TOKEN`, and put your workspace URL (e.g. `https://dbc-xxxx.cloud.databricks.com`) in `DATABRICKS_HOST`.
4. **Land bronze in the volume:** `python -m lakehouse.export.bronze_export --target s3,volume`
5. **Build silver.** Open `lakehouse/databricks/01_bronze_to_silver` and **Run all**. Its default `bronze_path` is the volume.

**Databricks teardown:** delete the token under **Settings → Developer → Access tokens**. Then run `DROP SCHEMA workspace.evidentia CASCADE` and `DROP SCHEMA workspace.evidentia_silver CASCADE` in the SQL editor.
