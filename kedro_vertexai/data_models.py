from dataclasses import dataclass
from typing import Optional


# Source: https://cloud.google.com/vertex-ai/docs/reference/rest/v1/PipelineState
class PipelineStatus:
    PIPELINE_STATE_UNSPECIFIED = "PIPELINE_STATE_UNSPECIFIED"
    PIPELINE_STATE_QUEUED = "PIPELINE_STATE_QUEUED"
    PIPELINE_STATE_PENDING = "PIPELINE_STATE_PENDING"
    PIPELINE_STATE_RUNNING = "PIPELINE_STATE_RUNNING"
    PIPELINE_STATE_SUCCEEDED = "PIPELINE_STATE_SUCCEEDED"
    PIPELINE_STATE_FAILED = "PIPELINE_STATE_FAILED"
    PIPELINE_STATE_CANCELLING = "PIPELINE_STATE_CANCELLING"
    PIPELINE_STATE_CANCELLED = "PIPELINE_STATE_CANCELLED"
    PIPELINE_STATE_PAUSED = "PIPELINE_STATE_PAUSED"


@dataclass(frozen=True)
class PipelineResult:
    is_success: bool
    state: str
    job_data: Optional[dict] = None
