# Evidentia (AI-Powered Literature Review Engine)

Evidentia is a full-stack system for structured literature reviews over research papers.

## Features
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

Next to the app sits a medallion-style lakehouse that copies Evidentia's data out of Postgres and turns it into analytics tables for a BI dashboard. It only reads from the app's database and never writes to it. Setup steps, commands and teardown are in [`lakehouse/README.md`](lakehouse/README.md). Design decisions and likely interview questions are in [`docs/interview-prep.md`](docs/interview-prep.md).

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
