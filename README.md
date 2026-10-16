# Evidentia (AI-Powered Literature Review Engine)

Evidentia reads research papers, pulls out what each one claims, and shows where the research agrees and where it disagrees. Next to the app sits a full lakehouse data pipeline (AWS S3, PySpark, Delta Lake, Databricks, dbt) that feeds a Power BI dashboard.

![Power BI dashboard built on the gold layer](lakehouse/bi/dashboard.png)

*The Power BI dashboard, reading the dbt gold tables in Databricks. It shows claims, papers and intervention-outcome pairs, how much the studies on each pair disagree (weighted by study design vs. unweighted), what the studies found, and how confident the extracted claims are, by study type. The intervention slicer filters every visual.*

## How it works, in plain English

1. **You upload papers.** Each paper is a PDF or text file about a health question, for example "does aspirin lower cardiovascular mortality?".
2. **The app reads each paper and writes down its claims** in a fixed format: who was studied, what was tried (the *intervention*), what was measured (the *outcome*), what was found (positive, negative, no effect, or mixed), and what kind of study it was. A randomized controlled trial counts for more than a single case report.
3. **Claims about the same question are grouped.** Every paper's "aspirin → cardiovascular mortality" claim sits in one group, whichever paper it came from.
4. **Each group gets a disagreement score.** If every study found the same thing, the score is 0. If the studies are split, it's high. Stronger study designs get a bigger vote.
5. **A separate data pipeline copies the data out of the app** and cleans it in stages: raw copy (bronze), cleaned (silver), and analysis-ready (gold). It then serves it to a dashboard, so analysis never slows the app down.
6. **The dashboard answers questions at a glance:** which research questions are settled, which are contested, and how strong the evidence behind each one is.

## What I built

**AI / NLP pipeline**
- Built an **LLM structured-extraction** step: each paper's text (truncated to a 12,000-character context budget) goes to **GPT-4o-mini** through the async **OpenAI Responses API**. A system prompt asks for up to 8 claims as a JSON array with a fixed schema: population, intervention, outcome, result (`positive | negative | null | mixed`), study type, and confidence.
- Parsed the model output into typed `ExtractedClaim` dataclasses, with per-field defaults and label normalization (lower-cased results and study types, float-coerced confidence). Any API or JSON-parse failure is logged and falls back to a **rule-based extractor**: regex sentence splitting plus keyword polarity rules (`increase`/`improve` → positive, `decrease`/`reduction` → negative, `no significant` → null). Ingestion never stalls on a bad model response.
- Built the **document processing** step:
  - parsed PDFs with **PyMuPDF** (`fitz`) page by page;
  - split the text into **overlapping chunks** (1,500-character windows, 200-character overlap) so sentences on a boundary keep their context;
  - embedded the chunks in batches with **`text-embedding-3-small`**;
  - indexed them in a persistent **Chroma** vector collection, with `paper_id` and `chunk_index` metadata and deterministic `paper_id:chunk` ids.
- **Clustered claims semantically**:
  - embedded each claim's "intervention + outcome" string;
  - grouped near-duplicate claims within a paper with **scikit-learn `AgglomerativeClustering`** (average linkage, **cosine distance**, `distance_threshold=0.25`), so the number of clusters is learned rather than fixed in advance;
  - stored the cluster id in the claim key; across papers, claims are grouped by their normalized intervention → outcome pair.
- Added a seeded, deterministic fallback embedding (NumPy `default_rng(42)`), so the whole pipeline runs reproducibly offline without an API key.
- Designed an **evidence-aggregation metric**: a **study-design-weighted majority vote**. Each claim votes for its result with a weight from an evidence hierarchy (RCT 1.0, quasi-experimental 0.8, observational 0.6, case-control 0.5, cross-sectional 0.45, case report 0.3). Disagreement is `1 − (winning weight / total weight)`: 0 when every study agrees, at most 0.75 with four outcomes. The unweighted version is reported alongside it, to show what design-weighting changes.
- **Post-processed LLM output in the data pipeline**: PySpark maps free-text labels from the model onto a fixed vocabulary (`randomized controlled trial` → `rct`, `cohort` → `observational`, `no effect` → `null`). It detects confidences emitted on a 0–100 scale and rescales them to 0–1, and quarantines claims with invalid labels or missing fields, with a reason code, instead of letting them skew the scores.
- Verified the metric end to end: a test runs the same claims through the Python implementation and the dbt SQL implementation and asserts the scores are identical.

**Application backend**
- Built a **FastAPI** backend whose `POST /ingest` returns immediately and runs parsing, embedding, extraction and clustering as a background **`asyncio`** task. A failing paper is marked `failed` with its error and never blocks the others.
- Stored papers, chunks and claims in **PostgreSQL** through async **SQLAlchemy 2.0** + **asyncpg**, with **Pydantic** schemas and **pydantic-settings** config. Added a TTL cache for `/claims` and `/heatmap` that is invalidated on every ingest.
- Built a dashboard UI with a **Plotly** intervention × outcome disagreement heatmap, and containerized the stack with **Docker Compose** (Postgres 16 + API).

**Bronze layer: ETL from Postgres to the data lake (AWS S3)**
- Wrote a Python **ETL** job that exports Postgres to **Apache Parquet** on **AWS S3**, using **pyarrow** and **boto3**, with explicit Parquet schemas, Snappy compression and **Hive-style partitioning** (`ingest_date=YYYY-MM-DD/run_id=...`).
- Read both tables inside **one read-only `REPEATABLE READ` transaction**, so every export is a consistent snapshot even while the app keeps writing. Fetched rows from Postgres in 5,000-row batches.
- Added **lineage columns** (`_run_id`, `_exported_at`, `_source_table`) and a JSON **manifest written last as a commit marker**, so downstream jobs skip any run that crashed partway.
- Provisioned AWS by hand with **least privilege**:
  - a private S3 bucket with all public access blocked and SSE-S3 encryption;
  - an **IAM** user with a customer-managed policy allowing only `ListBucket`/`GetObject`/`PutObject` on that one bucket, with no delete rights;
  - a zero-spend **AWS Budgets** alert.
- Kept every credential in a gitignored `.env`, with none in code or commits.
- Tested the export with **pytest**, using **moto** to fake S3 and SQLite to stand in for Postgres. Ran it for real: 42 papers and 196 claims landed in S3.

**Silver layer: PySpark → Delta Lake on Databricks**
- Wrote the bronze-to-silver job in **PySpark** as plain, unit-tested functions, run from a **Databricks** notebook committed to the repo and synced through a Databricks **Git folder**.
- Enforced schemas with an explicit `StructType`, and **deduplicated snapshots** with a window function (`row_number() over (partition by id order by _exported_at desc)`). Handled **soft deletes** (`is_deleted`) for ids missing from the newest snapshot.
- Normalized messy values:
  - text trimmed and lowercased;
  - study-type synonyms mapped to one label (`randomized controlled trial` → `rct`);
  - "no effect" mapped to `null`;
  - 0–100 confidences rescaled to 0–1.
- Routed invalid rows to a **quarantine table** (`claims_rejects`) with a `reject_reason`, instead of dropping them silently.
- Wrote to **Delta Lake** with `MERGE` upserts keyed on a **SHA-256 `row_hash`**, so re-running the job changes nothing (**idempotent**).
- Diagnosed that **Databricks Free Edition cannot attach an external S3 bucket**. A probe notebook showed the network could reach S3, but Spark was denied because no storage credentials are allowed. I landed the same Parquet files in a **Unity Catalog volume** instead and kept S3 as the lake of record.
- Ran it on Databricks: 194 claims passed, and 2 were quarantined with `invalid_result`.

**Gold layer: ELT with dbt on Databricks**
- Built a **dbt** project (`dbt-core` + `dbt-databricks`) that transforms silver inside the **Databricks SQL warehouse**. It has staging views, a **star schema** (`fct_claims` fact, `dim_papers` dimension), three gold marts and five pipeline-metrics models.
- Loaded the study-design weights as a **dbt seed** (CSV) that mirrors the API's weights, with a test checking that gold scores match the API's for the same claims.
- Wrote **63 data tests**:
  - `unique`, `not_null`, `accepted_values` and `relationships`;
  - a custom generic test (`value_between`);
  - singular tests that reconcile gold counts, check that unanimous pairs score 0, and fail the build if any row is lost between layers.
- Added **pipeline observability** tables: row counts per layer, quarantine rate, export freshness by day, evidence per paper, and a one-row summary.
- Verified end to end on local Spark: gold disagreement scores match the API's own `weighted_majority_fraction()` for the same claims. The seed can append batches of hundreds of papers (tested at 252 papers and 1,084 claims).
- Documented every model and column in dbt YAML, browsable as **dbt docs** with a lineage graph. The `profiles.yml` holds no secrets, only `env_var()` lookups.

**BI: Power BI**
- Connected **Power BI Desktop** to the gold tables through the **Azure Databricks connector** (SQL warehouse, **Import mode**, personal access token).
- Modeled a **star schema** in Power BI: `fct_claims` → `dim_papers` (many-to-one), `fct_claims` → `gold_weighted_disagreement` by `claim_pair`, and a one-to-one link between the two pair-level marts, with cross-filter direction set so the slicer filters every visual.
- Wrote **DAX** measures (`DISTINCTCOUNT`, `COUNTROWS`) and fixed column default summarization.
- Built the report:
  - KPI cards: 161 claims, 38 papers, 18 pairs;
  - an intervention slicer;
  - a weighted vs. unweighted disagreement bar chart;
  - a 100% stacked bar of findings per pair;
  - a stacked column chart of confidence by study type.

**Engineering practice**
- Shipped each layer as its own pull request on GitHub, with code review, and kept the app and its tests untouched by the data platform.
- Documented setup, cost and teardown for every cloud resource, and kept the whole stack on **free tiers**.

## Tech stack

| Area | Tools |
|---|---|
| Languages | Python, SQL (Spark SQL / Databricks SQL, Jinja-templated dbt SQL), DAX, HTML/JavaScript |
| Application | FastAPI, Uvicorn, Pydantic, SQLAlchemy 2.0 (async), asyncpg, PostgreSQL 16 |
| AI / ML | OpenAI API (Responses API, GPT-4o-mini, text-embedding-3-small), LLM structured extraction, prompt design, Chroma vector database, embeddings, scikit-learn (agglomerative clustering, cosine distance), NumPy, PyMuPDF, document chunking |
| Data lake | AWS S3, AWS IAM (least-privilege policy), AWS Budgets, boto3, Apache Parquet, pyarrow, Hive partitioning |
| Lakehouse | Databricks (Free Edition, serverless compute, SQL warehouse, Unity Catalog, volumes, Git folders, notebooks), Apache Spark / PySpark, Delta Lake (`MERGE` upserts, ACID transaction log) |
| Transformation | dbt Core, dbt-databricks, dbt seeds, generic and singular tests, dbt docs |
| BI | Power BI Desktop, Azure Databricks connector, DAX, star-schema modeling |
| Testing | pytest, moto (mock AWS), SQLite test doubles, local PySpark + delta-spark |
| DevOps | Docker, Docker Compose, Git, GitHub pull requests, `.env` secrets management, PowerShell |
| Concepts | Medallion architecture (bronze/silver/gold), lakehouse, ETL vs. ELT, OLTP vs. OLAP, idempotent pipelines, schema enforcement, data quality quarantine, soft deletes, snapshot isolation, data lineage, dimensional modeling |

## App features
- **FastAPI backend** with asynchronous paper ingestion.
- **Structured claim extraction** for:
  - population
  - intervention
  - outcome
  - result (`positive | negative | null | mixed`)
  - study type
- **PostgreSQL storage** for papers, chunks, and extracted claims.
- **Vector storage (Chroma)** for paper chunk embeddings.
- **Claim clustering** using embedding similarity.
- **Disagreement scoring** per claim: `1 - majority_class_fraction`.
- **Disagreement heatmap** (`intervention x outcome`).
- **Dashboard UI** for upload, claim browsing, and heatmap visualization.
- **Resilience** with partial-failure handling and extraction error logging.
- **Caching** for repeated `/claims` and `/heatmap` queries.
- **Bonus logic**:
  - study-type weighting (RCT > observational)
  - confidence scoring (from LLM or heuristic fallback)
  - filters (`population`, `study_type`)

## API Endpoints
- `POST /ingest` — upload a paper (`.pdf` or text file).
- `GET /claims` — list structured claims (`topic`, `population`, `study_type` filters).
- `GET /heatmap` — disagreement heatmap cells.
- `GET /papers` — list all processed papers and statuses.

## Run with Docker
```bash
docker compose up --build
```

Then open:
- API docs: http://localhost:8000/docs
- Dashboard: http://localhost:8000/

## Local Development
```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

### Environment variables
- `DATABASE_URL` (default: `postgresql+asyncpg://postgres:postgres@localhost:5432/evidentia`)
- `CHROMA_PATH` (default: `./.chroma`)
- `OPENAI_API_KEY` (optional, enables LLM extraction + real embeddings)
- `EMBEDDING_MODEL` (default: `text-embedding-3-small`)
- `EXTRACTION_MODEL` (default: `gpt-4o-mini`)

If `OPENAI_API_KEY` is not provided, Evidentia uses deterministic fallback heuristics and synthetic embeddings.

## Lakehouse (analytics layer)

Next to the app sits a medallion-style lakehouse that copies Evidentia's data out of Postgres and turns it into analytics tables for a BI dashboard. It only reads from the app's database and never writes to it. Setup steps, commands and teardown are in [`lakehouse/README.md`](lakehouse/README.md).

```mermaid
flowchart LR
    subgraph App["Evidentia app (OLTP)"]
        API[FastAPI ingest] --> PG[(PostgreSQL<br/>papers, claim_evidence)]
    end
    subgraph Bronze["BRONZE: raw, in the data lake"]
        S3[(AWS S3<br/>Parquet, partitioned by<br/>ingest_date / run_id)]
        VOL[(Unity Catalog volume<br/>same Parquet files)]
    end
    subgraph Silver["SILVER: clean, in Databricks"]
        SP[PySpark job] --> SD[(Delta tables<br/>papers, claims,<br/>claims_rejects)]
    end
    subgraph Gold["GOLD: analytics, in Databricks"]
        DBT[dbt models + tests] --> GD[(Delta tables<br/>fct_claims, dim_papers,<br/>3 gold marts)]
    end
    PG -->|"bronze_export.py<br/>(Python ETL: extract + load)"| S3
    PG -.->|same run| VOL
    VOL --> SP
    SD --> DBT
    GD -->|SQL warehouse| PBI[Power BI Desktop]
```

### Why each layer exists

| Layer | Where | Job | Why not skip it |
|---|---|---|---|
| **Bronze** | S3 (+ a Databricks volume copy) | An untouched, append-only history of every export, as Parquet. | If a later step has a bug, you can rebuild everything from bronze without touching production Postgres again. |
| **Silver** | Databricks, PySpark → Delta | One trustworthy row per entity: schema enforced, deduplicated, normalized, invalid rows quarantined with a reason. | Every downstream model would otherwise re-implement the same cleaning, slightly differently. |
| **Gold** | Databricks, dbt → Delta | Business-level tables shaped for questions: claims per intervention × outcome, weighted disagreement, confidence distributions. | The dashboard should read small, pre-aggregated, tested tables, not raw claims. |

**ETL vs ELT.** The bronze export is classic **ETL**: a Python job extracts from Postgres, transforms lightly (fixed schema, lineage columns) and loads files into the lake. Gold is **ELT**: the data is already loaded into the warehouse, and dbt transforms it there with SQL. That uses the warehouse's compute and keeps every transformation versioned, tested and documented in one place.

**Why Parquet.** It's a columnar, compressed, self-describing format. An analytics query reads only the columns it needs. Snappy compression makes files several times smaller than CSV. The schema travels inside the file, and Spark, Databricks, DuckDB and pandas all read it natively.

**Why Delta Lake.** Plain Parquet files can't be updated in place and give readers no consistency guarantees. Delta adds a transaction log (`_delta_log/`) on top of Parquet, which gives you:
- ACID commits
- `MERGE` (upserts), which silver uses to stay idempotent
- schema enforcement
- time travel (`VERSION AS OF`) and an audit trail (`DESCRIBE HISTORY`)

**What a lakehouse is.** It combines the cheap, open file storage of a data lake (Parquet files in object storage) with the reliability and SQL features of a warehouse (Delta transactions, tables, a SQL engine). One copy of the data then serves data engineering, analytics and BI.

**Honest limitations.**
- The demo data is synthetic and clearly labeled (`lakehouse/seed/`), because without an OpenAI key the app's extractor only produces placeholder claims.
- Databricks Free Edition can't attach your own S3 bucket. So the export also lands the same files in a Unity Catalog volume, which is what Databricks reads. S3 stays the data lake of record.

## Notes on pipeline behavior
- Ingestion runs asynchronously so one bad paper does not block others.
- Failed ingestions are marked with `ingest_status=failed` and `ingest_error` populated.
- The heatmap disagreement uses weighted voting by study type before computing majority fraction.
