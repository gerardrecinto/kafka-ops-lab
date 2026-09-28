"""The event schema this pipeline actually produces and consumes."""

from typing import Literal

from pydantic import BaseModel


class DeployEvent(BaseModel):
    schema_version: str = "1.1"
    event_id: str
    service: str
    environment: Literal["dev", "staging", "prod"]
    status: Literal["started", "succeeded", "failed", "rolled_back"]
    released_by: str
    occurred_at: str  # ISO 8601
    duration_seconds: float | None = None  # added in 1.1; optional, so 1.0 producers stay valid


# Kept for the compatibility check below and for tests/test_schema_compatibility.py
# — this is what the pipeline accepted before duration_seconds was added.
class DeployEventV1_0(BaseModel):
    schema_version: str = "1.0"
    event_id: str
    service: str
    environment: Literal["dev", "staging", "prod"]
    status: Literal["started", "succeeded", "failed", "rolled_back"]
    released_by: str
    occurred_at: str
