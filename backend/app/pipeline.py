from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any

import fitz
import numpy as np
from chromadb import PersistentClient
from openai import AsyncOpenAI
from sklearn.cluster import AgglomerativeClustering
from sklearn.metrics.pairwise import cosine_similarity

from .config import settings

logger = logging.getLogger(__name__)

PLACEHOLDER_PHRASES = {
    'intervention from paper',
    'primary outcome',
    'population from study',
    'result from paper',
}


@dataclass
class ExtractedClaim:
    population: str
    intervention: str
    outcome: str
    result: str
    study_type: str
    confidence: float


STUDY_WEIGHTS = {
    'rct': 1.0,
    'randomized controlled trial': 1.0,
    'quasi-experimental': 0.8,
    'observational': 0.6,
    'case-control': 0.5,
    'cross-sectional': 0.45,
    'case report': 0.3,
}


class LiteraturePipeline:
    def __init__(self):
        self.client = PersistentClient(path=settings.chroma_path)
        self.collection = self.client.get_or_create_collection('paper_chunks')
        self.openai = AsyncOpenAI(api_key=settings.openai_api_key) if settings.openai_api_key else None

    @staticmethod
    def parse_pdf_bytes(data: bytes) -> str:
        with fitz.open(stream=data, filetype='pdf') as doc:
            return '\n'.join(page.get_text('text') for page in doc)

    @staticmethod
    def parse_text_bytes(data: bytes) -> str:
        return data.decode('utf-8', errors='ignore')

    @staticmethod
    def chunk_text(text: str, chunk_size: int = 1500, overlap: int = 200) -> list[str]:
        if not text.strip():
            return []
        chunks: list[str] = []
        start = 0
        while start < len(text):
            end = min(len(text), start + chunk_size)
            chunks.append(text[start:end].strip())
            start += chunk_size - overlap
        return [c for c in chunks if c]

    async def embed_texts(self, texts: list[str]) -> list[list[float]]:
        if self.openai:
            response = await self.openai.embeddings.create(model=settings.embedding_model, input=texts)
            return [item.embedding for item in response.data]
        rng = np.random.default_rng(42)
        return [rng.random(256).tolist() for _ in texts]

    async def extract_structured_claims(self, full_text: str) -> list[ExtractedClaim]:
        if self.openai:
            prompt = (
                'Extract up to 8 structured study claims as JSON array with fields: '
                'population, intervention, outcome, result (positive|negative|null|mixed), study_type, confidence. '
                'Use exact or near-exact terms from the paper text. Do not invent template phrases. '
                'If a field cannot be grounded in the text, output "unknown". For comparative trials, name the '
                'main tested intervention explicitly (e.g., "high-dose atorvastatin" not "intervention").'
            )
            try:
                response = await self.openai.responses.create(
                    model=settings.extraction_model,
                    input=[
                        {'role': 'system', 'content': prompt},
                        {'role': 'user', 'content': full_text[:12000]},
                    ],
                )
                content = response.output_text
                raw_claims = json.loads(content)
                claims = [
                    ExtractedClaim(
                        population=c.get('population', 'unknown'),
                        intervention=c.get('intervention', 'unknown'),
                        outcome=c.get('outcome', 'unknown'),
                        result=(c.get('result', 'null') or 'null').lower(),
                        study_type=(c.get('study_type', 'observational') or 'observational').lower(),
                        confidence=float(c.get('confidence', 0.5)),
                    )
                    for c in raw_claims
                ]
                return [self._sanitize_claim(claim) for claim in claims]
            except Exception as exc:
                logger.exception('LLM extraction failed; falling back to heuristic parser: %s', exc)

        sentences = re.split(r'(?<=[.!?])\s+', full_text[:12000])
        claims: list[ExtractedClaim] = []
        for sentence in sentences[:10]:
            sent = sentence.lower()
            if any(k in sent for k in ('increase', 'improve', 'reduction', 'decrease', 'no significant')):
                if 'no significant' in sent:
                    result = 'null'
                elif 'mixed' in sent:
                    result = 'mixed'
                elif any(k in sent for k in ('decrease', 'reduction', 'lower')):
                    result = 'negative'
                else:
                    result = 'positive'
                intervention = self._extract_intervention(sentence) or 'unknown'
                outcome = self._extract_outcome(sentence) or 'unknown'
                population = self._extract_population(full_text) or 'unknown'
                claims.append(
                    ExtractedClaim(
                        population=population,
                        intervention=intervention,
                        outcome=outcome,
                        result=result,
                        study_type='observational',
                        confidence=0.35,
                    )
                )
        if not claims:
            claims = [ExtractedClaim(
                population='unknown',
                intervention='unknown',
                outcome='unknown',
                result='null',
                study_type='observational',
                confidence=0.2,
            )]
        return [self._sanitize_claim(claim) for claim in claims]

    @staticmethod
    def claim_key(intervention: str, outcome: str) -> str:
        return f'{intervention.strip().lower()} -> {outcome.strip().lower()}'

    @staticmethod
    def _extract_population(text: str) -> str | None:
        patterns = [
            r'in ([a-z0-9\\-\\s]+ patients)',
            r'among ([a-z0-9\\-\\s]+ adults)',
            r'in ([a-z0-9\\-\\s]+ children)',
        ]
        lowered = text.lower()
        for pattern in patterns:
            match = re.search(pattern, lowered)
            if match:
                return match.group(1).strip()
        return None

    @staticmethod
    def _extract_intervention(sentence: str) -> str | None:
        lowered = sentence.lower()
        match = re.search(r'([a-z0-9\\-\\s]{3,80})\\s(?:vs\\.?|versus|compared with)\\s', lowered)
        if match:
            return match.group(1).strip()
        match = re.search(r'(treatment with|use of)\\s([a-z0-9\\-\\s]{3,80})', lowered)
        if match:
            return match.group(2).strip()
        return None

    @staticmethod
    def _extract_outcome(sentence: str) -> str | None:
        lowered = sentence.lower()
        match = re.search(r'(?:improved|reduced|decreased|increased|no significant difference in)\\s([a-z0-9\\-\\s]{3,120})', lowered)
        if match:
            return match.group(1).strip(' .,;:')
        match = re.search(r'(?:for|on)\\s([a-z0-9\\-\\s]{3,120})', lowered)
        if match:
            return match.group(1).strip(' .,;:')
        return None

    @staticmethod
    def _sanitize_field(field_name: str, value: str) -> str:
        cleaned = (value or 'unknown').strip().lower()
        if cleaned in PLACEHOLDER_PHRASES or 'placeholder' in cleaned:
            logger.warning('Placeholder-like %s detected ("%s"); replacing with "unknown".', field_name, value)
            return 'unknown'
        return cleaned or 'unknown'

    def _sanitize_claim(self, claim: ExtractedClaim) -> ExtractedClaim:
        return ExtractedClaim(
            population=self._sanitize_field('population', claim.population),
            intervention=self._sanitize_field('intervention', claim.intervention),
            outcome=self._sanitize_field('outcome', claim.outcome),
            result=claim.result if claim.result in {'positive', 'negative', 'null', 'mixed'} else 'null',
            study_type=(claim.study_type or 'observational').lower(),
            confidence=claim.confidence,
        )

    @staticmethod
    def cluster_claims(vectors: list[list[float]], threshold: float = 0.25) -> list[int]:
        if len(vectors) <= 1:
            return [0] * len(vectors)
        model = AgglomerativeClustering(n_clusters=None, distance_threshold=threshold, metric='cosine', linkage='average')
        return model.fit_predict(np.array(vectors)).tolist()

    @staticmethod
    def assign_claim_similarity(claim_vectors: list[list[float]], cluster_centers: list[list[float]]) -> list[int]:
        if not claim_vectors or not cluster_centers:
            return []
        sim = cosine_similarity(np.array(claim_vectors), np.array(cluster_centers))
        return sim.argmax(axis=1).tolist()


def weighted_majority_fraction(results: list[str], study_types: list[str]) -> float:
    totals: dict[str, float] = {}
    total_weight = 0.0
    for result, study_type in zip(results, study_types):
        weight = STUDY_WEIGHTS.get(study_type.lower(), 0.5)
        totals[result] = totals.get(result, 0.0) + weight
        total_weight += weight
    if total_weight == 0:
        return 0.0
    majority = max(totals.values()) if totals else 0.0
    return majority / total_weight
