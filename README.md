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

## Notes on pipeline behavior
- Ingestion runs asynchronously so one bad paper does not block others.
- Failed ingestions are marked with `ingest_status=failed` and `ingest_error` populated.
- The heatmap disagreement uses weighted voting by study type before computing majority fraction.
