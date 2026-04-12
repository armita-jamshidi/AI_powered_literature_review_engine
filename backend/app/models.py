from __future__ import annotations

from datetime import datetime
from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .db import Base


class Paper(Base):
    __tablename__ = 'papers'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    title: Mapped[str] = mapped_column(String(512), default='Untitled')
    source_filename: Mapped[str] = mapped_column(String(256))
    full_text: Mapped[str] = mapped_column(Text)
    ingest_status: Mapped[str] = mapped_column(String(32), default='pending')
    ingest_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    chunks: Mapped[list[PaperChunk]] = relationship(back_populates='paper', cascade='all, delete-orphan')
    claims: Mapped[list[ClaimEvidence]] = relationship(back_populates='paper', cascade='all, delete-orphan')


class PaperChunk(Base):
    __tablename__ = 'paper_chunks'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    paper_id: Mapped[int] = mapped_column(ForeignKey('papers.id', ondelete='CASCADE'))
    chunk_index: Mapped[int] = mapped_column(Integer)
    section: Mapped[str] = mapped_column(String(64), default='body')
    content: Mapped[str] = mapped_column(Text)

    paper: Mapped[Paper] = relationship(back_populates='chunks')


class ClaimEvidence(Base):
    __tablename__ = 'claim_evidence'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    paper_id: Mapped[int] = mapped_column(ForeignKey('papers.id', ondelete='CASCADE'))
    claim_key: Mapped[str] = mapped_column(String(512), index=True)
    population: Mapped[str] = mapped_column(String(512), default='unknown')
    intervention: Mapped[str] = mapped_column(String(512), default='unknown')
    outcome: Mapped[str] = mapped_column(String(512), default='unknown')
    result: Mapped[str] = mapped_column(String(16), default='null')
    study_type: Mapped[str] = mapped_column(String(64), default='observational')
    confidence: Mapped[float] = mapped_column(Float, default=0.5)

    paper: Mapped[Paper] = relationship(back_populates='claims')
