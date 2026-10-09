import sqlite3

import pytest
from sqlalchemy import create_engine, text

# Same columns as backend/app/models.py. SQLite stands in for Postgres so the
# unit tests need no database server.
DDL = (
    """CREATE TABLE papers (
        id INTEGER PRIMARY KEY, title VARCHAR(512) NOT NULL, source_filename VARCHAR(256) NOT NULL,
        full_text TEXT NOT NULL, ingest_status VARCHAR(32) NOT NULL, ingest_error TEXT,
        created_at TIMESTAMP NOT NULL)""",
    """CREATE TABLE claim_evidence (
        id INTEGER PRIMARY KEY, paper_id INTEGER NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
        claim_key VARCHAR(512) NOT NULL, population VARCHAR(512) NOT NULL, intervention VARCHAR(512) NOT NULL,
        outcome VARCHAR(512) NOT NULL, result VARCHAR(16) NOT NULL, study_type VARCHAR(64) NOT NULL,
        confidence FLOAT NOT NULL)""",
)


@pytest.fixture
def engine():
    # PARSE_DECLTYPES makes sqlite3 return TIMESTAMP columns as datetime, like Postgres does.
    eng = create_engine('sqlite://', connect_args={'detect_types': sqlite3.PARSE_DECLTYPES})
    with eng.begin() as conn:
        conn.execute(text('PRAGMA foreign_keys = ON'))
        for stmt in DDL:
            conn.execute(text(stmt))
    yield eng
    eng.dispose()
