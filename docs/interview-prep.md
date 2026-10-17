# Interview prep: the Evidentia lakehouse

This file explains each design decision in the lakehouse layer, and gives questions you're likely to be asked with answers you can defend from the code. File references point at the real implementation, so you can show it.

Each section follows the same shape: what we built, why, the trade-off, then likely questions.

---

## 1. The 30-second version

> Evidentia is a FastAPI app that extracts structured claims from research papers into Postgres. I added a medallion lakehouse next to it:
> - A Python job snapshots Postgres into S3 as partitioned Parquet (bronze).
> - A PySpark job on Databricks cleans, deduplicates and validates that into Delta tables (silver).
> - dbt builds tested analytics models on top (gold).
> - Power BI reads the gold tables.
>
> The gold disagreement score is the same study-type-weighted metric the app's API computes, and a test checks the two agree exactly.

---

## 2. Bronze: the export job (`lakehouse/export/bronze_export.py`)

**What:** It reads `papers` and `claim_evidence` from Postgres and writes one Parquet file per table per run. The path is `bronze/<table>/ingest_date=YYYY-MM-DD/run_id=<UTC timestamp>/part-00000.parquet`. A manifest JSON is written last.

**Decisions and why:**
- **Read-only, REPEATABLE READ transaction.** Both tables come from one consistent snapshot. Every exported claim's `paper_id` therefore exists in the same run's papers, even while the app keeps inserting. Read-only means the job can't modify production data.
- **Full snapshot per run, not incremental.** The tables are small, and a snapshot is simple and self-healing. It also lets silver detect deletes: an id missing from the newest snapshot was deleted. The trade-off is repeated data in bronze, which silver deduplicates. At scale I'd switch to incremental extraction using an `updated_at` watermark or change data capture (CDC), e.g. Debezium.
- **Explicit Parquet schema** (`export/schemas.py`). Types stay stable even when a column is empty or all NULL in one run.
- **Hive-style partitions** (`ingest_date=`). Engines turn folder names into columns and skip partitions that don't match a filter (partition pruning).
- **Manifest written last.** It acts as a commit marker. A run that crashed halfway has no manifest, and silver ignores it. (This is the same idea as Spark's `_SUCCESS` file.)
- **Streaming in batches** (`fetchmany(5000)` into `ParquetWriter`). Memory use stays flat as tables grow.
- **Left out `papers.full_text`.** It's the largest column and no analytics use it.
- **Credentials only through the standard AWS chain** (env vars or `~/.aws/credentials`). Nothing is hardcoded, and `.env` is gitignored. The IAM policy allows `ListBucket` / `GetObject` / `PutObject` on one bucket and **no delete**, which is least privilege.

**Likely questions**
- *Why Parquet and not CSV/JSON?* Columnar layout means queries read only the needed columns. It compresses well (Snappy), stores types and schema in the file, and every lake engine reads it.
- *How would you make it incremental?* Add an `updated_at` column and keep a high-water mark per table, or use CDC from Postgres's write-ahead log. Then MERGE in silver, which already exists.
- *How do you know the job didn't change production data?* It only runs SELECTs, inside a read-only transaction. A test (`test_export_does_not_modify_source`) checks that the source tables are identical before and after.
- *How is it tested without AWS?* `moto` fakes S3 in-process, and SQLite stands in for Postgres. There's also an opt-in integration test against real Postgres.

---

## 3. Silver: PySpark → Delta (`lakehouse/databricks/silver_transforms.py`)

**What:** It reads every complete bronze run with an explicit schema and writes `papers`, `claims` and `claims_rejects` as Delta tables.

**Decisions and why:**
- **Logic in a plain module, notebook as a thin driver.** The same functions run on Databricks and in local pytest. Notebooks are hard to test; modules aren't.
- **Spark Connect–safe APIs only** (no RDDs, `sparkContext` or `.cache()`), because Databricks serverless runs Spark Connect.
- **Dedup with a window:** `row_number() over (partition by id order by _exported_at desc)` keeps the latest version of each id across snapshots.
- **Soft deletes:** ids absent from the latest run get `is_deleted = true` instead of being removed, so history isn't lost.
- **Normalization:**
  - text is trimmed, lowercased and whitespace-collapsed
  - `study_type` variants map to a canonical value (`randomized controlled trial` → `rct`, `cohort` → `observational`, unknown → `other`)
  - result synonyms map to the allowed set (`no effect` → `null`)
  - confidences on a 0–100 scale are rescaled and flagged
  - the cluster id is parsed out of `claim_key`
- **Quarantine, don't drop:** invalid rows go to `claims_rejects` with a `reject_reason`. Silently dropping data hides upstream bugs.
- **Semantic duplicates:** a re-uploaded file creates a second paper row. Silver points it at the earliest paper id (`canonical_paper_id`) and flags repeated findings `is_duplicate`.
- **Idempotent MERGE:** each row gets a SHA-256 `row_hash` of its business columns. MERGE updates only rows whose hash changed and inserts new ids. Re-running on the same bronze changes 0 rows, and a test asserts this from Delta's operation metrics.

**Likely questions**
- *What is Delta Lake, concretely?* Parquet data files plus a JSON transaction log (`_delta_log/`). Every write is an atomic commit. Readers see a consistent version, you get MERGE/UPDATE/DELETE on files, schema enforcement, and time travel (`VERSION AS OF n`, `DESCRIBE HISTORY`).
- *Why MERGE instead of overwrite?* Overwrite rewrites everything and loses the "what changed" signal. MERGE keyed on the primary key is incremental and idempotent, and the hash avoids rewriting unchanged rows. (`claims_rejects` *is* overwritten, because it describes current state.)
- *How do you handle schema drift?* Bronze is read with an explicit `StructType`, so a drifted file fails loudly at silver. Adding a column means updating the schema in one place on each side.
- *Why soft deletes?* Auditability, and gold can still choose to filter them out (`stg_*` models do).
- *Where are data quality checks?* There are three levels:
  - rejects with reasons
  - asserts in the notebook (unique keys, valid results, confidence in [0, 1], no orphans)
  - dbt tests in gold

---

## 4. The Databricks Free Edition constraint

**What happened:** Free Edition runs only serverless compute, limits outbound internet to trusted domains, and doesn't let you configure your own storage (no storage credentials or external locations for your bucket). `00_probe_s3_access.py` tests this directly. When I ran it on my workspace, the network check reached S3 (S3 answered with HTTP 403, so the host is reachable), but the Spark read failed with `UNAUTHORIZED_ACCESS`, and the workspace had 0 storage credentials. That confirmed the limitation rather than assuming it.

**Decision:** Keep S3 as the data lake of record, and have the export job *also* upload the same files into a Unity Catalog **volume** (`--target s3,volume`, through the Databricks SDK Files API). Databricks reads the volume.

**Likely questions**
- *How would this work in a real company?* An AWS IAM role trusted by Databricks, a Unity Catalog storage credential on that role, an external location on `s3://bucket/bronze`, and then Spark reads `s3://` directly with no copy. Or Auto Loader, which ingests new files incrementally.
- *Isn't copying data twice wasteful?* At this size it's negligible, and it's the honest workaround for the free tier. The README says so rather than hiding it.

---

## 5. Gold: dbt (`lakehouse/dbt/`)

**What:**
- staging views (`stg_papers`, `stg_claims`)
- a fact table (`fct_claims`) and a dimension (`dim_papers`)
- a seed (`study_type_weights.csv`)
- three marts:
  - `gold_claims_by_intervention_outcome`
  - `gold_weighted_disagreement`
  - `gold_confidence_distribution`

**Decisions and why:**
- **ELT with dbt.** The data is already loaded, so transformations are SQL that runs inside the warehouse. dbt gives dependency ordering via `ref()`, materializations (views vs tables), tests, docs, and a lineage graph, all version-controlled.
- **Staging → marts layering.** Staging only renames and filters: it drops deleted rows and duplicates and maps claims to the canonical paper. Business logic lives in marts. `fct_claims` / `dim_papers` form a small star schema that Power BI can relate.
- **Weights as a seed.** A CSV in git is reviewable, testable (`unique`, `relationships`), and identical to `STUDY_WEIGHTS` in the app.
- **The disagreement metric:** each claim votes for its result with its study design's weight. The score is `1 − (weight of the winning result / total weight)`, which is 0 when every study agrees and at most 0.75 with four results. The unweighted score sits next to it to show what weighting changes.
- **Tests (43 for the marts; `dbt build` passed 51/51 nodes on Databricks. The pipeline-metrics models later brought the total to 63):**
  - `unique` and `not_null` on keys
  - `accepted_values` on `result`
  - `relationships` from claims to papers and to the weights seed
  - a custom generic test `value_between`
  - two singular tests: every claim counted exactly once in each mart, and pairs with only one result score exactly 0
- **Credentials:** `profiles.yml` is committed but contains only `env_var()` calls.
- **Tested without Databricks:** a second profile target runs the same project on local Spark + Delta. `test_dbt_gold.py` runs seed → bronze → silver → `dbt build`, then checks that every gold score equals the app's Python `weighted_majority_fraction()`.

**Likely questions**
- *Difference between a generic and a singular dbt test?* A generic test is parameterized and attached to columns in YAML (`unique`, or our `value_between`). A singular test is a one-off SQL file in `tests/`. Both fail when they return rows.
- *View vs table materialization?* Views are cheap and always fresh but recompute on every query, so we use them for staging. Tables are precomputed and fast for BI, so marts are tables. Incremental models are the next step at scale.
- *Why not do all of this in PySpark?* We could. dbt makes the business layer SQL that analysts can read and change, and gives tests, docs and lineage for free. PySpark is the better tool for the messy, row-level cleaning in silver.
- *How do you know the numbers are right?* The reconciliation tests, plus the cross-check against the API's own Python implementation.

---

## 6. BI: Power BI

**What:** a one-page Power BI Desktop report (`lakehouse/bi/evidentia_gold.pbix`, screenshot in `lakehouse/bi/dashboard.png`) on the gold tables:
- KPI cards: 161 claims, 38 papers, 18 intervention → outcome pairs
- an intervention slicer that filters every visual
- a bar chart of weighted vs. unweighted disagreement per pair
- a 100% stacked bar of what the studies found (positive / negative / null / mixed) per pair
- a stacked column chart of claim confidence, bucketed and colored by study type

**Decisions and why:**
- **Azure Databricks connector, Import mode.** The connector takes the warehouse's server hostname, HTTP path and a personal access token. Import copies the (tiny) gold tables into the report, so visuals are instant and the serverless warehouse doesn't have to stay awake. DirectQuery would send every click to the warehouse, which only makes sense for large or near-real-time data.
- **Star schema in the model view:**
  - `fct_claims[paper_id]` → `dim_papers` (many-to-one);
  - `fct_claims[claim_pair]` → `gold_weighted_disagreement` (many-to-one);
  - `gold_claims_by_intervention_outcome` ↔ `gold_weighted_disagreement` on `claim_pair` (one-to-one, filtering both ways).
  The fact table sits in the middle, and filters flow from the "one" side to the "many" side.
- **Explicit DAX measures** instead of dragging raw columns: `Papers = DISTINCTCOUNT(fct_claims[paper_id])` and `Pairs = COUNTROWS(gold_weighted_disagreement)`. `paper_id` is text, so Power BI would otherwise treat it as a category, not a number to count.
- **Summarization fix.** The disagreement columns defaulted to "Don't summarize", so the bar chart wouldn't aggregate them. I set their default to Average in Column tools.
- **Why 161 and 38, not 194 and 42?** Gold counts each finding once. Silver flags 33 semantic duplicates, and 2 re-uploaded papers merge into their originals. The cards count papers that have at least one live claim.

**Likely questions**
- *Import vs DirectQuery?* Import is a cached copy: fast, and works offline, but only as fresh as the last refresh. DirectQuery runs every visual as a live SQL query: always fresh, but slower, and it costs warehouse compute. A middle option is a composite model, where large facts use DirectQuery and small dimensions use Import.
- *What is cross-filter direction?* It's which way a filter travels across a relationship. Single direction (from dimension to fact) is the default and is predictable. "Both" is useful for a one-to-one link like the two pair-level marts, but on many-to-many links it can create ambiguous paths.
- *Measure vs calculated column?* A calculated column is computed per row at refresh and stored. A measure is computed at query time in the current filter context, which is why `Papers` responds to the slicer.
- *Why do the aggregation in dbt instead of DAX?* Logic in dbt is versioned, tested and reusable by any tool. DAX should mostly be thin measures over clean, tested tables.

---

## 7. General questions

- **What is the medallion architecture?** Bronze is raw and append-only, silver is cleaned and conformed, and gold is business-level aggregates. Each layer has one job, and you can rebuild downstream layers from upstream ones.
- **What is a lakehouse?** Cheap, open-format storage (Parquet in object storage) plus a transactional table layer (Delta) and a SQL engine. You get warehouse features without copying data into a proprietary warehouse.
- **OLTP vs OLAP?** Postgres serves the app: many small row-level reads and writes, normalized. The lakehouse serves analytics: big scans and aggregations, columnar. Running heavy analytics on the OLTP database would slow the app down.
- **What would you do next?**
  - an orchestrator (Databricks Workflows or Airflow) to chain export → silver → dbt on a schedule
  - incremental extraction or CDC
  - Auto Loader with an external location on a paid tier
  - incremental dbt models
  - CI that runs the pytest suite on every PR
- **What was hard?**
  - Spark treats paths that start with `_` as hidden, so the `_manifests/` folder was being ignored. I switched to an explicit glob.
  - Postgres's `VARCHAR(16)` on `result` rejected a test value, which a real-database integration test caught. I added a test that checks seed values against column lengths.
  - Free Edition can't read your own S3 bucket, which led to the volume fallback.
