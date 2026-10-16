"""Seed Postgres with clearly labeled SYNTHETIC papers and claims.

Without an OpenAI key, Evidentia's extractor emits placeholder claims, which
makes for an empty dashboard. This script writes realistic-looking rows straight
into the app's `papers` and `claim_evidence` tables so the lakehouse can be
built and demoed for free.

Every synthetic paper has a title starting with "[SYNTHETIC]" and a
source_filename under "synthetic/", so it can never be mistaken for real
evidence and can be removed with `--reset`.

The data is deliberately a little messy, the way LLM extraction output is in
practice, so the silver layer has real cleaning work to do:
  * study_type spelled several ways ("rct", "randomized controlled trial", "cohort")
  * a few confidences on a 0-100 scale instead of 0-1
  * a few results outside the allowed set ("no effect", which silver can map
    to "null", and "improved", which it must reject)
  * two papers uploaded twice, producing duplicate claims

Usage (from the repo root, with the app's tables already created by starting the API once):
    python -m lakehouse.seed.seed_demo_data                       # first time: insert 40 papers
    python -m lakehouse.seed.seed_demo_data --reset               # delete synthetic rows, then insert 40
    python -m lakehouse.seed.seed_demo_data --reset --papers 500  # start over with 500 papers
    python -m lakehouse.seed.seed_demo_data --append --papers 200 # add 200 more, keep what's there

The amount of mess scales with the batch: every 40 papers bring 4 rescaled
confidences, 3 "no effect" results, 2 "improved" results and 2 re-uploads.
"""
from __future__ import annotations

import argparse
import random
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from lakehouse.db import make_engine

SYNTHETIC_PREFIX = 'synthetic/'

# (intervention, outcome) -> probabilities of (positive, negative, null, mixed).
# "negative" means the outcome went down (e.g. statins lower LDL), matching how
# the app's heuristic extractor labels a "decrease".
EFFECTS: dict[tuple[str, str], tuple[float, float, float, float]] = {
    ('statins', 'ldl cholesterol'): (0.03, 0.90, 0.05, 0.02),
    ('statins', 'all-cause mortality'): (0.05, 0.60, 0.25, 0.10),
    ('metformin', 'hba1c'): (0.05, 0.85, 0.07, 0.03),
    ('metformin', 'body weight'): (0.10, 0.45, 0.35, 0.10),
    ('aspirin', 'cardiovascular mortality'): (0.10, 0.40, 0.35, 0.15),
    ('aspirin', 'all-cause mortality'): (0.15, 0.30, 0.40, 0.15),
    ('vitamin d supplementation', 'bone fracture risk'): (0.10, 0.30, 0.45, 0.15),
    ('vitamin d supplementation', 'depressive symptoms'): (0.15, 0.25, 0.45, 0.15),
    ('mediterranean diet', 'cardiovascular mortality'): (0.05, 0.70, 0.15, 0.10),
    ('mediterranean diet', 'body weight'): (0.10, 0.50, 0.30, 0.10),
    ('aerobic exercise', 'depressive symptoms'): (0.05, 0.70, 0.15, 0.10),
    ('aerobic exercise', 'blood pressure'): (0.05, 0.75, 0.15, 0.05),
    ('cognitive behavioral therapy', 'anxiety symptoms'): (0.05, 0.80, 0.10, 0.05),
    ('cognitive behavioral therapy', 'sleep quality'): (0.70, 0.05, 0.15, 0.10),
    ('mindfulness meditation', 'anxiety symptoms'): (0.10, 0.50, 0.30, 0.10),
    ('intermittent fasting', 'body weight'): (0.05, 0.55, 0.30, 0.10),
    ('intermittent fasting', 'hba1c'): (0.10, 0.35, 0.40, 0.15),
    ('omega-3 supplementation', 'cardiovascular mortality'): (0.10, 0.25, 0.50, 0.15),
}
RESULTS = ('positive', 'negative', 'null', 'mixed')

POPULATIONS = (
    'adults', 'older adults', 'adults with type 2 diabetes',
    'postmenopausal women', 'adolescents', 'adults with hypertension',
)

# Clean label -> spellings an LLM might emit for it. The app lowercases study_type
# but does nothing else, so all of these can land in Postgres.
STUDY_TYPE_SPELLINGS: dict[str, tuple[str, ...]] = {
    'rct': ('rct', 'rct', 'randomized controlled trial'),
    'quasi-experimental': ('quasi-experimental',),
    'observational': ('observational', 'observational', 'cohort'),
    'case-control': ('case-control',),
    'cross-sectional': ('cross-sectional',),
    'case report': ('case report',),
}
STUDY_TYPE_MIX = (('rct', 0.30), ('quasi-experimental', 0.10), ('observational', 0.35),
                  ('case-control', 0.10), ('cross-sectional', 0.10), ('case report', 0.05))
# Higher-quality designs get higher extractor confidence on average.
CONFIDENCE_CENTER = {'rct': 0.85, 'quasi-experimental': 0.75, 'observational': 0.65,
                     'case-control': 0.6, 'cross-sectional': 0.55, 'case report': 0.45}


@dataclass
class SyntheticPaper:
    key: int
    title: str
    source_filename: str
    full_text: str
    ingest_status: str
    ingest_error: str | None
    created_at: datetime
    claims: list[dict] = field(default_factory=list)


def _weighted_choice(rng: random.Random, options, weights):
    return rng.choices(options, weights=weights, k=1)[0]


def generate_dataset(n_papers: int = 40, seed: int = 42, now: datetime | None = None,
                     first_number: int = 1) -> list[SyntheticPaper]:
    """Build the synthetic papers and claims in memory. Pure and deterministic for a given seed.

    Papers are numbered from `first_number`, so a batch appended later gets new
    titles and filenames instead of looking like re-uploads of the first batch.
    """
    rng = random.Random(seed)
    now = now or datetime(2026, 10, 1, 12, 0, 0)
    pairs = list(EFFECTS)
    papers: list[SyntheticPaper] = []

    for i in range(first_number, first_number + n_papers):
        created_at = now - timedelta(days=rng.randint(0, 29), minutes=rng.randint(0, 1439))
        failed = i % 40 in (13, 29)  # two failed ingests in every 40 papers
        study_label = _weighted_choice(rng, [s for s, _ in STUDY_TYPE_MIX], [w for _, w in STUDY_TYPE_MIX])
        focus = rng.sample(pairs, k=rng.randint(2, 4))
        paper = SyntheticPaper(
            key=i,
            title=f'[SYNTHETIC] {focus[0][0].title()} and {focus[0][1]}: study {i:03d}',
            source_filename=f'{SYNTHETIC_PREFIX}paper_{i:03d}.txt',
            full_text=f'Synthetic abstract {i:03d}. Generated by lakehouse/seed for demo purposes only.',
            ingest_status='failed' if failed else 'processed',
            ingest_error='Synthetic failure: PDF could not be parsed' if failed else None,
            created_at=created_at,
        )
        if not failed:
            population = rng.choice(POPULATIONS)
            for cluster_id, (intervention, outcome) in enumerate(focus):
                for _ in range(rng.randint(1, 2)):
                    result = _weighted_choice(rng, RESULTS, EFFECTS[(intervention, outcome)])
                    confidence = round(min(0.99, max(0.05, rng.gauss(CONFIDENCE_CENTER[study_label], 0.1))), 3)
                    paper.claims.append({
                        'claim_key': f'cluster-{cluster_id}: {intervention} -> {outcome}',
                        'population': population,
                        'intervention': intervention,
                        'outcome': outcome,
                        'result': result,
                        'study_type': rng.choice(STUDY_TYPE_SPELLINGS[study_label]),
                        'confidence': confidence,
                    })
        papers.append(paper)

    _inject_messiness(rng, papers, scale=max(1, n_papers // 40))
    return papers


def _inject_messiness(rng: random.Random, papers: list[SyntheticPaper], scale: int = 1) -> None:
    claims = [c for p in papers for c in p.claims]
    messy = rng.sample(claims, k=min(9 * scale, len(claims)))
    for claim in messy[:4 * scale]:
        claim['confidence'] = round(claim['confidence'] * 100, 1)  # 0-100 scale slip
    for claim in messy[4 * scale:7 * scale]:
        claim['result'] = 'no effect'  # outside the vocabulary, but means "null"
    for claim in messy[7 * scale:]:
        claim['result'] = 'improved'  # can't be mapped to a direction

    # Re-uploads: the same file ingested twice yields a second paper row with identical claims.
    processed = [p for p in papers if p.claims]
    for original in rng.sample(processed, k=min(2 * scale, len(processed))):
        papers.append(SyntheticPaper(
            key=len(papers) + 1,
            title=original.title,
            source_filename=original.source_filename,
            full_text=original.full_text,
            ingest_status=original.ingest_status,
            ingest_error=original.ingest_error,
            created_at=original.created_at + timedelta(hours=3),
            claims=[dict(c) for c in original.claims],
        ))


def count_synthetic(engine: Engine) -> int:
    with engine.connect() as conn:
        return conn.execute(
            text('SELECT count(*) FROM papers WHERE source_filename LIKE :p'), {'p': SYNTHETIC_PREFIX + '%'}
        ).scalar_one()


def next_paper_number(engine: Engine) -> int:
    """One past the highest synthetic paper number already in the database (1 if there are none)."""
    with engine.connect() as conn:
        names = conn.execute(
            text('SELECT source_filename FROM papers WHERE source_filename LIKE :p'), {'p': SYNTHETIC_PREFIX + '%'}
        ).scalars().all()
    numbers = [int(m.group(1)) for n in names if (m := re.search(r'paper_(\d+)\.txt$', n))]
    return max(numbers, default=0) + 1


def reset_synthetic(engine: Engine) -> int:
    with engine.begin() as conn:
        # claim_evidence and paper_chunks cascade from papers via ON DELETE CASCADE.
        result = conn.execute(text('DELETE FROM papers WHERE source_filename LIKE :p'), {'p': SYNTHETIC_PREFIX + '%'})
        return result.rowcount


def insert_dataset(engine: Engine, papers: list[SyntheticPaper]) -> tuple[int, int]:
    n_claims = 0
    with engine.begin() as conn:
        for paper in papers:
            paper_id = conn.execute(
                text(
                    'INSERT INTO papers (title, source_filename, full_text, ingest_status, ingest_error, created_at) '
                    'VALUES (:title, :source_filename, :full_text, :ingest_status, :ingest_error, :created_at) '
                    'RETURNING id'
                ),
                {
                    'title': paper.title, 'source_filename': paper.source_filename, 'full_text': paper.full_text,
                    'ingest_status': paper.ingest_status, 'ingest_error': paper.ingest_error,
                    'created_at': paper.created_at,
                },
            ).scalar_one()
            for claim in paper.claims:
                conn.execute(
                    text(
                        'INSERT INTO claim_evidence (paper_id, claim_key, population, intervention, outcome, '
                        'result, study_type, confidence) VALUES (:paper_id, :claim_key, :population, '
                        ':intervention, :outcome, :result, :study_type, :confidence)'
                    ),
                    {'paper_id': paper_id, **claim},
                )
                n_claims += 1
    return len(papers), n_claims


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--reset', action='store_true', help='delete previously seeded synthetic rows first')
    mode.add_argument('--append', action='store_true', help='add a new batch after the synthetic papers already there')
    parser.add_argument('--papers', type=int, default=40, help='how many papers to generate (default 40)')
    parser.add_argument('--seed', type=int, default=None,
                        help='random seed (default 42, or a new one per appended batch)')
    args = parser.parse_args(argv)
    if args.papers < 1:
        parser.error('--papers must be at least 1')

    engine = make_engine()
    missing = {'papers', 'claim_evidence'} - set(inspect(engine).get_table_names())
    if missing:
        raise SystemExit(
            f'Missing tables {sorted(missing)}. Start the API once (docker compose up) so it creates them.'
        )
    existing = count_synthetic(engine)
    if existing and not (args.reset or args.append):
        raise SystemExit(
            f'There are already {existing} synthetic papers. Running again would insert copies of them.\n'
            '  Add a new batch on top:  --append --papers 200\n'
            '  Start over:              --reset --papers 200'
        )
    if args.reset:
        print(f'Deleted {reset_synthetic(engine)} synthetic papers (and their claims).')

    first_number = next_paper_number(engine) if args.append else 1
    if args.append:
        # A batch added now looks like papers that arrived over the last few weeks,
        # and its own seed keeps it from repeating the first batch's claims.
        dataset = generate_dataset(args.papers, args.seed if args.seed is not None else first_number,
                                   now=datetime.now(timezone.utc).replace(tzinfo=None), first_number=first_number)
    else:
        dataset = generate_dataset(args.papers, args.seed if args.seed is not None else 42)
    n_papers, n_claims = insert_dataset(engine, dataset)
    print(f'Inserted {n_papers} synthetic papers and {n_claims} claims.')
    print(f'Synthetic papers in the database now: {count_synthetic(engine)}.')


if __name__ == '__main__':
    main()
