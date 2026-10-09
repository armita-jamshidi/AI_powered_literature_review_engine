"""Database helpers shared by the lakehouse jobs.

The FastAPI app talks to Postgres through the async `asyncpg` driver. Batch jobs
like the bronze export don't need async, so they use the synchronous `psycopg`
driver against the same database and the same DATABASE_URL.
"""
from __future__ import annotations

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

DEFAULT_DATABASE_URL = 'postgresql://postgres:postgres@localhost:5432/evidentia'


def normalize_database_url(url: str) -> str:
    """Rewrite the app's async URL into one the sync psycopg driver accepts."""
    for prefix in ('postgresql+asyncpg://', 'postgresql+psycopg2://', 'postgresql://', 'postgres://'):
        if url.startswith(prefix):
            return 'postgresql+psycopg://' + url[len(prefix):]
    return url


def make_engine(url: str | None = None) -> Engine:
    load_dotenv()
    return create_engine(normalize_database_url(url or os.getenv('DATABASE_URL', DEFAULT_DATABASE_URL)))
