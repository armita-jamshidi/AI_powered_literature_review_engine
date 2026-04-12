from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from pathlib import Path

from fastapi import Depends, FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .cache import TTLCache
from .config import settings
from .db import Base, SessionLocal, engine, get_db
from .models import ClaimEvidence, Paper, PaperChunk
from .pipeline import LiteraturePipeline, weighted_majority_fraction
from .schemas import ClaimGroupOut, ClaimOut, HeatmapCell, IngestResponse, PaperOut, Study, StudyCreate

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title='Evidentia API')
pipeline = LiteraturePipeline()
query_cache = TTLCache(settings.cache_ttl_seconds)
study_store: list[Study] = []
study_id_counter = 1

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)


@app.on_event('startup')
async def startup_event() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


@app.get('/')
async def root() -> FileResponse:
    dashboard = Path(__file__).resolve().parents[2] / 'frontend' / 'src' / 'index.html'
    return FileResponse(dashboard)


@app.post('/ingest', response_model=IngestResponse | Study)
async def ingest_paper(
    request: Request,
    file: UploadFile | None = File(default=None),
    db: AsyncSession = Depends(get_db),
):
    global study_id_counter
    if request.headers.get('content-type', '').startswith('application/json'):
        body = await request.json()
        study = Study(id=study_id_counter, **StudyCreate(**body).model_dump())
        study_store.append(study)
        study_id_counter += 1
        query_cache.clear()
        return study

    if file is None:
        raise HTTPException(status_code=400, detail='Provide either JSON study payload or a file upload')

    raw_data = await file.read()
    if not raw_data:
        raise HTTPException(status_code=400, detail='Empty upload')

    paper = Paper(title=file.filename or 'Uploaded paper', source_filename=file.filename or 'unknown', full_text='')
    db.add(paper)
    await db.commit()
    await db.refresh(paper)

    asyncio.create_task(process_ingestion(paper.id, file.filename or 'uploaded', raw_data))
    return IngestResponse(paper_id=paper.id, status='queued')


async def process_ingestion(paper_id: int, filename: str, raw_data: bytes) -> None:
    async with SessionLocal() as db:
        paper = await db.get(Paper, paper_id)
        if not paper:
            return
        try:
            paper.ingest_status = 'processing'
            await db.commit()

            if filename.lower().endswith('.pdf'):
                text = pipeline.parse_pdf_bytes(raw_data)
            else:
                text = pipeline.parse_text_bytes(raw_data)
            paper.full_text = text

            chunks = pipeline.chunk_text(text)
            embeddings = await pipeline.embed_texts(chunks) if chunks else []
            for idx, chunk in enumerate(chunks):
                db.add(PaperChunk(paper_id=paper.id, chunk_index=idx, section='body', content=chunk))

            if chunks and embeddings:
                pipeline.collection.add(
                    ids=[f'{paper.id}:{i}' for i in range(len(chunks))],
                    documents=chunks,
                    embeddings=embeddings,
                    metadatas=[{'paper_id': paper.id, 'chunk_index': i} for i in range(len(chunks))],
                )

            extracted_claims = await pipeline.extract_structured_claims(text)
            claim_strings = [f"{c.intervention} {c.outcome}" for c in extracted_claims]
            claim_embeddings = await pipeline.embed_texts(claim_strings) if claim_strings else []
            cluster_ids = pipeline.cluster_claims(claim_embeddings) if claim_embeddings else [0] * len(extracted_claims)

            for claim, cluster_id in zip(extracted_claims, cluster_ids):
                intervention = claim.intervention.strip().lower()
                outcome = claim.outcome.strip().lower()
                claim_key = f'cluster-{cluster_id}: {pipeline.claim_key(intervention, outcome)}'
                db.add(
                    ClaimEvidence(
                        paper_id=paper.id,
                        claim_key=claim_key,
                        population=claim.population,
                        intervention=intervention,
                        outcome=outcome,
                        result=claim.result,
                        study_type=claim.study_type,
                        confidence=claim.confidence,
                    )
                )

            paper.ingest_status = 'processed'
            paper.ingest_error = None
            await db.commit()
            query_cache.clear()
        except Exception as exc:
            logger.exception('Ingestion failure for paper %s: %s', paper_id, exc)
            paper.ingest_status = 'failed'
            paper.ingest_error = str(exc)
            await db.commit()


@app.get('/papers', response_model=list[PaperOut])
async def list_papers(db: AsyncSession = Depends(get_db)):
    rows = (await db.execute(select(Paper).order_by(Paper.created_at.desc()))).scalars().all()
    return [
        PaperOut(
            id=row.id,
            title=row.title,
            source_filename=row.source_filename,
            ingest_status=row.ingest_status,
            ingest_error=row.ingest_error,
        )
        for row in rows
    ]


@app.get('/claims', response_model=list[ClaimGroupOut] | list[ClaimOut])
async def claims(
    topic: str | None = Query(default=None),
    population: str | None = Query(default=None),
    study_type: str | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
):
    cache_key = f'claims:{topic}:{population}:{study_type}'
    cached = query_cache.get(cache_key)
    if cached is not None:
        return cached

    if study_store:
        grouped: dict[tuple[str, str], list[Study]] = defaultdict(list)
        for study in study_store:
            if topic and topic.lower() not in f'{study.intervention} {study.outcome}'.lower():
                continue
            if population and population.lower() not in study.population.lower():
                continue
            if study_type and study_type.lower() not in study.study_type.lower():
                continue
            grouped[(study.intervention, study.outcome)].append(study)
        payload: list[ClaimGroupOut] = []
        for (intervention, outcome), studies in grouped.items():
            counts: dict[str, int] = defaultdict(int)
            for study in studies:
                counts[study.result] += 1
            payload.append(
                ClaimGroupOut(
                    claim_key=pipeline.claim_key(intervention, outcome),
                    intervention=intervention,
                    outcome=outcome,
                    result_counts=dict(counts),
                    total_studies=len(studies),
                )
            )
        query_cache.set(cache_key, payload)
        return payload

    stmt = select(ClaimEvidence)
    if topic:
        like = f'%{topic.lower()}%'
        stmt = stmt.where(ClaimEvidence.claim_key.ilike(like))
    if population:
        stmt = stmt.where(ClaimEvidence.population.ilike(f'%{population}%'))
    if study_type:
        stmt = stmt.where(ClaimEvidence.study_type.ilike(f'%{study_type}%'))

    rows = (await db.execute(stmt)).scalars().all()
    payload = [
        ClaimOut(
            claim_key=r.claim_key,
            population=r.population,
            intervention=r.intervention,
            outcome=r.outcome,
            result=r.result,
            study_type=r.study_type,
            confidence=r.confidence,
            paper_id=r.paper_id,
        )
        for r in rows
    ]
    query_cache.set(cache_key, payload)
    return payload


@app.get('/heatmap', response_model=list[HeatmapCell])
async def heatmap(db: AsyncSession = Depends(get_db)):
    cache_key = 'heatmap'
    cached = query_cache.get(cache_key)
    if cached is not None:
        return cached

    matrix: dict[tuple[str, str], list[str]] = defaultdict(list)
    if study_store:
        for study in study_store:
            matrix[(study.intervention, study.outcome)].append(study.result)
    else:
        rows = (await db.execute(select(ClaimEvidence))).scalars().all()
        for row in rows:
            matrix[(row.intervention, row.outcome)].append(row.result)

    cells: list[HeatmapCell] = []
    for (intervention, outcome), results in matrix.items():
        if study_store:
            total = len(results)
            counts: dict[str, int] = defaultdict(int)
            for result in results:
                counts[result] += 1
            majority_count = max(counts.values()) if counts else 0
            disagreement = round(1 - (majority_count / total if total else 0.0), 4)
        else:
            rows = (await db.execute(
                select(ClaimEvidence).where(
                    ClaimEvidence.intervention == intervention,
                    ClaimEvidence.outcome == outcome,
                )
            )).scalars().all()
            majority_fraction = weighted_majority_fraction(
                [s.result for s in rows],
                [s.study_type for s in rows],
            )
            disagreement = round(1 - majority_fraction, 4)
        cells.append(
            HeatmapCell(
                intervention=intervention,
                outcome=outcome,
                disagreement_score=disagreement,
                total_studies=len(results),
            )
        )

    query_cache.set(cache_key, cells)
    return cells
