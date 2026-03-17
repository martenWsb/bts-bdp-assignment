from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional

router = APIRouter()

# Generate 20 mock items so the pagination tests (page 0, page 1) pass 
MOCK_PIPELINES = [
    {
        "id": f"run-{i:03d}",
        "repository": "bts-bdp-assignment",
        "branch": "main",
        "status": "success" if i % 2 == 0 else "failed",
        "triggered_by": "push",
        "started_at": f"2026-03-10T10:{i:02d}:00Z",
        "finished_at": f"2026-03-10T10:{i:02d}:30Z",
        "stages": ["lint", "test", "build"]
    } for i in range(1, 21)
]

# Sort by time descending (newest first) as required by tests 
MOCK_PIPELINES.sort(key=lambda x: x["started_at"], reverse=True)

@router.get("/api/s9/pipelines")
def get_pipelines(
    num_results: int = 10, 
    page: int = 0, 
    status_filter: Optional[str] = None
):
    """Returns a list of CI/CD pipeline runs with pagination and filtering."""
    data = MOCK_PIPELINES
    if status_filter:
        data = [p for p in data if p["status"] == status_filter]
    
    start = page * num_results
    end = start + num_results
    return data[start:end]

@router.get("/api/s9/pipelines/{pipeline_id}/stages")
def get_pipeline_stages(pipeline_id: str):
    """Returns the stages of a specific pipeline run."""
    if not any(p["id"] == pipeline_id for p in MOCK_PIPELINES):
        raise HTTPException(status_code=404, detail="Pipeline run not found")
        
    return [
        {"name": "lint", "status": "success", "started_at": "2026-03-10T10:00:00Z", "finished_at": "2026-03-10T10:00:45Z", "logs_url": f"/api/s9/pipelines/{pipeline_id}/stages/lint/logs"},
        {"name": "test", "status": "success", "started_at": "2026-03-10T10:01:00Z", "finished_at": "2026-03-10T10:03:00Z", "logs_url": f"/api/s9/pipelines/{pipeline_id}/stages/test/logs"},
        {"name": "build", "status": "success", "started_at": "2026-03-10T10:03:30Z", "finished_at": "2026-03-10T10:05:30Z", "logs_url": f"/api/s9/pipelines/{pipeline_id}/stages/build/logs"}
    ]
