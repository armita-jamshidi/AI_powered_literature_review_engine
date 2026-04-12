from pydantic import BaseModel


class PaperOut(BaseModel):
    id: int
    title: str
    source_filename: str
    ingest_status: str
    ingest_error: str | None


class ClaimOut(BaseModel):
    claim_key: str
    population: str
    intervention: str
    outcome: str
    result: str
    study_type: str
    confidence: float
    paper_id: int


class HeatmapCell(BaseModel):
    intervention: str
    outcome: str
    disagreement_score: float
    total_studies: int


class IngestResponse(BaseModel):
    paper_id: int
    status: str
