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

Every layer is built. The Power BI steps are in [`bi/README.md`](bi/README.md).

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
| `dbt/` | The dbt project for the gold layer: staging views, a fact and a dimension table, three gold marts, a seed, and tests. |
| `bi/` | Power BI Desktop steps, and where to save the dashboard (`evidentia_gold.pbix`). |
| `dbt/` | The dbt project for the gold layer: staging views, a fact and a dimension table, three gold marts, five pipeline metrics tables, a seed, and tests. |
| `tests/` | pytest suite. SQLite stands in for Postgres, `moto` fakes S3, and a local Spark session runs the silver tests. |

## Bronze: what the export job does

1. Opens **one read-only, REPEATABLE READ transaction** on Postgres. Both tables are read from the same snapshot, so every exported claim's `paper_id` exists in the same run's papers, even while the app keeps ingesting.
2. Streams each table out in batches of 5,000 rows into a Parquet file with a fixed schema. `papers.full_text` is left out because it's large and no analytics need it.
3. Adds lineage columns to every row: `_run_id`, `_exported_at`, `_source_table`.
4. Writes `bronze/<table>/ingest_date=YYYY-MM-DD/run_id=<run_id>/part-00000.parquet`.
5. Writes `bronze/_manifests/run_id=<run_id>.json` **last**. A run without a manifest crashed partway, and silver will skip it.

Each run is a **full snapshot**: a claim exported on Monday and again on Tuesday appears in both partitions. That is normal for bronze, which keeps history raw. Silver deduplicates it.

## Silver: what the PySpark job does

`silver_transforms.run_bronze_to_silver` reads **every complete bronze run** (one with a manifest) and writes four Delta tables: `papers`, `claims`, `claims_rejects`, and `bronze_runs` (one row per export run and table, with its row count from the manifest, used by the gold metrics). For `papers` and `claims` it:

1. **Enforces a schema.** Parquet is read with an explicit `StructType`, so a drifted file fails loudly instead of changing a column type downstream.
2. **Dedupes snapshots.** Bronze holds each row once *per run*. A window (`row_number() over (partition by id order by _exported_at desc)`) keeps the newest version of each id. An id missing from the newest snapshot was deleted in Postgres, so it's kept and flagged `is_deleted` (a *soft delete*).
3. **Normalizes values.** Text is trimmed, lowercased and whitespace-collapsed. `study_type` spellings map to one label (`randomized controlled trial` → `rct`, `cohort` → `observational`). Results like `no effect` map to `null`. Confidences on a 0–100 scale are rescaled to 0–1 and flagged `confidence_rescaled`. The cluster id is parsed out of `claim_key`.
4. **Routes bad rows to `claims_rejects`** with a `reject_reason` (`invalid_result`, `invalid_confidence`, `orphan_paper`, `missing_intervention_or_outcome`) rather than dropping them silently.
5. **Flags semantic duplicates.** Re-uploads of the same file point to the earliest paper (`canonical_paper_id`). The same finding repeated for one canonical paper is flagged `is_duplicate`, so gold counts it once.
6. **Writes with Delta `MERGE`.** New ids are inserted. Existing ids are updated only when their `row_hash` (a SHA-256 of the business columns) changed. Re-running on the same bronze data changes nothing, which makes the job **idempotent**.

On the seeded data (42 papers, 196 claims): 2 claims are rejected (`invalid_result`), 4 confidences are rescaled, 3 `no effect` results map to `null`, 2 papers are flagged as re-uploads, and 33 claims are flagged as duplicates. Most of those duplicates are the seed reporting the same finding twice within one paper.

## Gold: the dbt project (ELT)

Silver gets data *loaded* into Databricks. Gold then *transforms it inside the warehouse* with SQL, which is the "LT" of **ELT**. dbt compiles each model into a `CREATE TABLE/VIEW ... AS SELECT` on the Databricks SQL warehouse, works out the build order from `ref()`, and runs tests after each model.

| Model | Type | What it is |
|---|---|---|
| `stg_papers`, `stg_claims` | view | Silver minus deleted rows and duplicates; claims are attributed to the canonical paper. |
| `study_type_weights` | seed (CSV) | RCT 1.0 … case report 0.3, the same weights as `backend/app/pipeline.py`. |
| `fct_claims` | table | One row per live claim, with its study weight and confidence bucket. |
| `dim_papers` | table | One row per live paper, with its claim count. |
| `gold_claims_by_intervention_outcome` | table | Claims per intervention × outcome, split by result. |
| `gold_weighted_disagreement` | table | `1 − (weight of the winning result / total weight)` per pair, plus the unweighted score for comparison. |
| `gold_confidence_distribution` | table | Confidence histogram by study type and result. |

**Tests (63 in all, including the metrics tests below):** `unique`/`not_null` on every key; `accepted_values` on `result` and `ingest_status`; `relationships` from claims to papers and to the weights seed; a custom generic test `value_between` (confidence in [0, 1], disagreement in [0, 0.75]); and two singular tests. One checks that every claim is counted exactly once in each mart, the other that a pair whose claims all agree scores 0.

`lakehouse/tests/test_dbt_gold.py` runs the whole chain (seed → bronze → silver → `dbt build`) on a local Spark session. It also checks that every gold disagreement score equals what a copy of the API's `weighted_majority_fraction()` (kept in the test, so it needs none of the app's dependencies) computes from the same claims. On the seeded data, `mindfulness meditation → anxiety symptoms` disagrees most (0.66), and `statins → ldl cholesterol` disagrees least (0.0).

### Pipeline metrics (`models/metrics/`)

These tables describe the pipeline itself rather than the research findings. They are ordinary dbt models in `evidentia_gold`, so they're built and tested by the same `dbt build`.

| Model | Grain | What it tells you |
|---|---|---|
| `metrics_layer_row_counts` | one row per layer table | Rows in bronze (latest export), silver (passed and quarantined) and gold, in pipeline order. |
| `metrics_quarantine` | one row per check outcome | How many live claims passed silver's checks, and how many were quarantined for each reason, with shares. |
| `metrics_evidence_per_paper` | one row per live paper | Claims, distinct intervention → outcome pairs, summed study-design weight and average confidence. |
| `metrics_freshness_by_ingest_date` | one row per export day | Runs that day, when the last one finished, rows exported, change since the previous export day, and days ago. |
| `metrics_pipeline_summary` | one row | Headline numbers: last export time and hours since, quarantine rate, failed-ingest rate, papers and claims per layer. |

A singular test, `assert_no_rows_lost_between_layers`, fails the build if any paper or claim in the latest bronze export is missing from silver (every exported claim must either pass or be quarantined). On the seeded data: 42 papers and 196 claims in bronze, 194 claims passed and 2 quarantined (a 1.02% quarantine rate), and 40 papers in gold once the 2 re-uploads merge into their originals. After appending 200 papers: 252 papers and 1,084 claims in bronze, 15 quarantined (1.38%).

## Adding more papers

Papers reach Postgres in two ways: uploading a PDF or text file to the app's `POST /ingest`, or the synthetic seed. Without an OpenAI key the app's extractor only produces placeholder claims, so the seed is the way to grow the demo data. It can add a batch on top of what's there:

```powershell
python -m lakehouse.seed.seed_demo_data --append --papers 200   # add 200 more papers
python -m lakehouse.seed.seed_demo_data --reset --papers 500    # or start over with 500
```

An appended batch continues the numbering (`paper_041.txt`, ...), has its own random seed, and is dated over the last 30 days. Every 40 papers bring the same mix of mess (4 rescaled confidences, 3 `no effect`, 2 `improved`, 2 re-uploads, 2 failed ingests), so silver keeps having work to do and the quarantine rate stays meaningful. Running the seed a second time without `--append` or `--reset` stops with a message instead of inserting copies.

Then send the new data through the pipeline: run the bronze export, the silver notebook, and `dbt build`. The step-by-step version is in "Running it after adding papers" below.

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

### Running dbt against Databricks

1. **Find the warehouse.** Go to **SQL Warehouses**, open the Serverless Starter Warehouse (Free Edition's one warehouse), then **Connection details**. Copy the **HTTP path** into `lakehouse\.env` as `DATABRICKS_HTTP_PATH`.
2. **Install dbt** in its own virtual environment, so its dependencies stay separate from the export job's:
   ```powershell
   python -m venv .venv-dbt
   .\.venv-dbt\Scripts\Activate.ps1
   pip install -r lakehouse\requirements-dbt.txt
   ```
3. **Load `.env` into the shell.** dbt reads environment variables, not `.env` files. `profiles.yml` contains no secrets, only `env_var(...)` lookups.
   ```powershell
   Get-Content lakehouse\.env | Where-Object { $_ -match '^[A-Z_]+=' } | ForEach-Object {
       $name, $value = $_ -split '=', 2; Set-Item "env:$name" $value }
   ```
4. **Build and test:**
   ```powershell
   cd lakehouse\dbt
   dbt debug --profiles-dir .          # checks the connection
   dbt build --profiles-dir .          # seed + models + tests, in dependency order
   dbt docs generate --profiles-dir .
   dbt docs serve --profiles-dir .     # opens the docs site with the lineage graph
   ```
   `dbt docs generate` needs Databricks; it doesn't work on the local Spark test target.

**Databricks teardown:** delete the token under **Settings → Developer → Access tokens**. Then run `DROP SCHEMA workspace.evidentia CASCADE`, `DROP SCHEMA workspace.evidentia_silver CASCADE` and `DROP SCHEMA workspace.evidentia_gold CASCADE` in the SQL editor.

## Running it after adding papers (Windows PowerShell + Databricks)

1. **Postgres must be running.** Open Docker Desktop, then in PowerShell, from the repo root: `docker compose up -d`.
2. **Add papers.** In the lakehouse environment:
   ```powershell
   .\.venv-lakehouse\Scripts\Activate.ps1
   python -m lakehouse.seed.seed_demo_data --append --papers 200
   ```
3. **Export to bronze** (S3 and the Databricks volume): `python -m lakehouse.export.bronze_export --target s3,volume`
4. **Update the Git folder in Databricks.** Open **Workspace → Home →** the repo's Git folder, click the branch name at the top, then **Pull**.
5. **Rebuild silver.** Open `lakehouse/databricks/01_bronze_to_silver` and click **Run all**.
6. **Rebuild gold and the metrics.** In a new PowerShell window, from the repo root:
   ```powershell
   .\.venv-dbt\Scripts\Activate.ps1
   Get-Content lakehouse\.env | Where-Object { $_ -match '^[A-Z_]+=' } | ForEach-Object {
       $name, $value = $_ -split '=', 2; Set-Item "env:$name" $value }
   cd lakehouse\dbt
   dbt build --profiles-dir .
   ```
7. **Look at the metrics.** In Databricks, open **Catalog → workspace → evidentia_gold**, click `metrics_pipeline_summary`, then the **Sample data** tab. Or open **SQL Editor** and run `SELECT * FROM workspace.evidentia_gold.metrics_layer_row_counts ORDER BY step`.
