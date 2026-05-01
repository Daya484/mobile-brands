"""
Cloud Run — FastAPI Extraction Service (Event-Driven)
=======================================================
Two modes of operation:
  1. SCHEDULED:  Airflow calls POST /extract on a schedule
  2. EVENT-DRIVEN: GCS file upload → Pub/Sub → POST /gcs-event → DAG triggered immediately

This makes the pipeline truly event-driven when new Excel files arrive.

Endpoints:
    POST /extract   — Run the full extraction pipeline
    GET  /health    — Health check
    GET  /status    — Last run status

Deploy:
    gcloud run deploy mb-extractor \
        --image gcr.io/YOUR_PROJECT_ID/mb-extractor:latest \
        --region us-central1 \
        --service-account mb-pipeline-sa@YOUR_PROJECT_ID.iam.gserviceaccount.com \
        --no-allow-unauthenticated
"""

import logging
import os
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

# Import extraction logic
from extract import run_extraction

# Import GCS event-driven trigger router
from gcs_trigger import gcs_trigger_router

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG (from env vars injected by Cloud Run)
# ─────────────────────────────────────────────────────────────────────────────
SOURCE_BUCKET = os.environ.get("SOURCE_BUCKET", "mobile-brands")
DEST_BUCKET   = os.environ.get("DEST_BUCKET",   "mb-pipeline-bucket")
LOG_LEVEL     = os.environ.get("LOG_LEVEL",     "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger("mb-extractor")

# ─────────────────────────────────────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Mobile Brands Extractor",
    description=(
        "Event-driven GCS extraction service for mobile brand sales data. "
        "Supports both scheduled (POST /extract) and event-driven (POST /gcs-event) modes."
    ),
    version="2.0.0",
)

# Mount the GCS event-trigger router (POST /gcs-event)
app.include_router(gcs_trigger_router)

_last_run: dict = {}


# ─────────────────────────────────────────────────────────────────────────────
# REQUEST / RESPONSE MODELS
# ─────────────────────────────────────────────────────────────────────────────
class ExtractionRequest(BaseModel):
    source_bucket: Optional[str] = None
    dest_bucket:   Optional[str] = None
    dry_run:       bool          = False


class ExtractionResponse(BaseModel):
    status:    str
    folders:   int
    files:     int
    uploaded:  int
    errors:    int
    timestamp: str
    duration_seconds: float


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINTS
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health_check():
    """Health check endpoint — used by Cloud Run readiness probe."""
    return {"status": "healthy", "service": "mb-extractor", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/status")
async def last_status():
    """Returns the result of the last extraction run."""
    if not _last_run:
        return {"status": "no_runs_yet"}
    return _last_run


@app.post("/extract", response_model=ExtractionResponse)
async def trigger_extraction(request: ExtractionRequest):
    """
    Triggers the mobile brands extraction pipeline.
    Reads Excel files from source GCS bucket, splits brand sheets,
    and writes them to the raw/ landing zone in the destination bucket.
    """
    global _last_run

    src  = request.source_bucket or SOURCE_BUCKET
    dest = request.dest_bucket   or DEST_BUCKET

    log.info("Extraction triggered: source=%s dest=%s dry_run=%s", src, dest, request.dry_run)

    if request.dry_run:
        return ExtractionResponse(
            status="dry_run",
            folders=0, files=0, uploaded=0, errors=0,
            timestamp=datetime.now(timezone.utc).isoformat(),
            duration_seconds=0.0,
        )

    start = datetime.now(timezone.utc)
    try:
        result = run_extraction(source_bucket=src, dest_bucket=dest)
    except Exception as exc:
        log.exception("Extraction failed with exception: %s", exc)
        raise HTTPException(status_code=500, detail=f"Extraction failed: {exc}")
    end = datetime.now(timezone.utc)

    duration = (end - start).total_seconds()
    result["duration_seconds"] = round(duration, 2)

    _last_run = result
    log.info("Extraction completed in %.2fs: %s", duration, result)

    if result.get("status") == "error":
        raise HTTPException(
            status_code=500,
            detail=f"Extraction completed with errors: {result['errors']} file(s) failed.",
        )

    return ExtractionResponse(**result)


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
