from collections import Counter

from sqlalchemy import text

from lakehouse.seed.seed_demo_data import (
    SYNTHETIC_PREFIX,
    generate_dataset,
    insert_dataset,
    reset_synthetic,
)


def test_dataset_is_deterministic():
    assert [p.claims for p in generate_dataset(seed=7)] == [p.claims for p in generate_dataset(seed=7)]


def test_every_paper_is_labeled_synthetic():
    for paper in generate_dataset():
        assert paper.title.startswith('[SYNTHETIC]')
        assert paper.source_filename.startswith(SYNTHETIC_PREFIX)


def test_dataset_contains_the_mess_silver_must_clean():
    papers = generate_dataset()
    claims = [c for p in papers for c in p.claims]
    assert sum(c['confidence'] > 1 for c in claims) == 4
    assert sum(c['result'] == 'no effect' for c in claims) == 3
    assert sum(c['result'] == 'improved' for c in claims) == 2
    assert {'rct', 'randomized controlled trial'} <= {c['study_type'] for c in claims}
    reuploaded = [f for f, n in Counter(p.source_filename for p in papers).items() if n > 1]
    assert len(reuploaded) == 2


def test_failed_papers_have_errors_and_no_claims():
    failed = [p for p in generate_dataset() if p.ingest_status == 'failed']
    assert failed
    assert all(p.ingest_error and not p.claims for p in failed)


def test_reset_removes_only_synthetic_rows(engine):
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO papers (title, source_filename, full_text, ingest_status, created_at) "
            "VALUES ('real paper', 'real.pdf', 'text', 'processed', '2026-10-01 00:00:00')"
        ))
    insert_dataset(engine, generate_dataset(n_papers=5))
    reset_synthetic(engine)
    with engine.connect() as conn:
        assert conn.execute(text('SELECT source_filename FROM papers')).scalars().all() == ['real.pdf']
        assert conn.execute(text('SELECT count(*) FROM claim_evidence')).scalar_one() == 0


def test_values_fit_postgres_column_lengths():
    # claim_evidence.result is VARCHAR(16) and study_type VARCHAR(64) in backend/app/models.py.
    for paper in generate_dataset():
        assert len(paper.source_filename) <= 256 and len(paper.title) <= 512
        for claim in paper.claims:
            assert len(claim['result']) <= 16
            assert len(claim['study_type']) <= 64
