"""
GCS Event Trigger — Mobile Brands Pipeline
============================================
Replaces the scheduled Airflow DAG trigger with an event-driven trigger.

Flow:
  1. New Excel file lands in GCS source bucket
  2. GCS sends a notification to Pub/Sub topic
  3. Cloud Run (this service) receives the Pub/Sub push message
  4. Cloud Run triggers the Airflow DAG via Composer REST API
  5. Airflow DAG runs immediately (not on a schedule)

This makes the pipeline TRULY event-driven:
  - File arrives → pipeline starts within seconds
  - No polling, no fixed schedule
  - Multiple files can trigger parallel DAG runs

Deploy alongside main.py in the same Cloud Run service.
Add route to main.py: app.include_router(gcs_trigger_router)

GCS Notification setup (run once):
    gsutil notification create \
        -f json \
        -t projects/PROJECT/topics/mb-gcs-events \
        -e OBJECT_FINALIZE \
        -p INDIA/ \
        gs://mobile-brands

Pub/Sub Push Subscription (run once):
    gcloud pubsub subscriptions create mb-gcs-push-sub \
        --topic=mb-gcs-events \
        --push-endpoint=https://CLOUD_RUN_URL/gcs-event \
        --ack-deadline=60
"""

import base64
import json
import logging
import os
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

log = logging.getLogger("gcs-trigger")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
ENV              = os.environ.get("ENV",              "dv")
PROJECT_ID       = os.environ.get("PROJECT_ID",       f"{ENV}-env")
REGION           = os.environ.get("REGION",           "us-central1")
COMPOSER_ENV     = os.environ.get("COMPOSER_ENV",     f"{ENV}-mb-composer")
DAG_ID           = f"{ENV}_mobile_brands_pipeline"

# File extensions that trigger the pipeline
TRIGGER_EXTENSIONS = (".xlsx", ".xls", ".xlsm")

gcs_trigger_router = APIRouter()


# ─────────────────────────────────────────────────────────────────────────────
# MODELS
# ─────────────────────────────────────────────────────────────────────────────

class PubSubMessage(BaseModel):
    """Pub/Sub push message envelope."""
    message: dict
    subscription: str


# ─────────────────────────────────────────────────────────────────────────────
# COMPOSER TRIGGER
# ─────────────────────────────────────────────────────────────────────────────

async def trigger_airflow_dag(
    dag_id: str,
    conf: dict,
    token: str,
) -> dict:
    """
    Triggers an Airflow DAG via Cloud Composer REST API.
    Uses the Composer 2 Airflow API endpoint.
    """
    # Get Composer Airflow web server URL
    async with httpx.AsyncClient(timeout=30) as client:
        # Get Composer environment details to find the Airflow URL
        meta_url = (
            f"https://composer.googleapis.com/v1/projects/{PROJECT_ID}"
            f"/locations/{REGION}/environments/{COMPOSER_ENV}"
        )
        meta_resp = await client.get(
            meta_url,
            headers={"Authorization": f"Bearer {token}"},
        )
        meta_resp.raise_for_status()
        airflow_url = meta_resp.json()["config"]["airflowUri"]

        # Trigger the DAG
        trigger_url = f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns"
        payload = {
            "conf": conf,
            "note": f"Triggered by GCS event: {conf.get('source_file', 'unknown')}",
        }
        trigger_resp = await client.post(
            trigger_url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        trigger_resp.raise_for_status()
        return trigger_resp.json()


async def get_access_token() -> str:
    """Gets GCP access token from the Cloud Run metadata server."""
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
            headers={"Metadata-Flavor": "Google"},
        )
        resp.raise_for_status()
        return resp.json()["access_token"]


# ─────────────────────────────────────────────────────────────────────────────
# ENDPOINT: /gcs-event
# Receives Pub/Sub push notifications from GCS object notifications
# ─────────────────────────────────────────────────────────────────────────────

@gcs_trigger_router.post("/gcs-event")
async def handle_gcs_event(request: Request):
    """
    Receives GCS object finalize events via Pub/Sub push subscription.
    Parses the event and triggers the Airflow DAG if the file is an Excel file.

    Pub/Sub push message format:
    {
      "message": {
        "data": "<base64-encoded JSON>",
        "messageId": "...",
        "publishTime": "..."
      },
      "subscription": "..."
    }
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    # Decode the Pub/Sub message data
    message_data = body.get("message", {}).get("data", "")
    if not message_data:
        log.warning("Empty Pub/Sub message received — ignoring.")
        return {"status": "ignored", "reason": "empty_message"}

    try:
        gcs_event = json.loads(base64.b64decode(message_data).decode("utf-8"))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to decode message: {exc}")

    # Extract GCS object info
    object_name  = gcs_event.get("name", "")
    bucket_name  = gcs_event.get("bucket", "")
    event_type   = gcs_event.get("kind", "")
    content_type = gcs_event.get("contentType", "")

    log.info(
        "GCS event received: bucket=%s object=%s contentType=%s",
        bucket_name, object_name, content_type,
    )

    # Only trigger pipeline for Excel files
    if not object_name.lower().endswith(TRIGGER_EXTENSIONS):
        log.info("Not an Excel file (%s) — ignoring event.", object_name)
        return {"status": "ignored", "reason": "not_excel", "object": object_name}

    # Extract region from folder structure: INDIA/file.xlsx → INDIA
    parts = object_name.split("/")
    region = parts[0] if len(parts) > 1 else "UNKNOWN"
    file_name = parts[-1]

    log.info(
        "Excel file detected: region=%s file=%s — triggering DAG %s",
        region, file_name, DAG_ID,
    )

    # Get access token and trigger Airflow DAG
    try:
        token = await get_access_token()
        dag_conf = {
            "source_file":  f"gs://{bucket_name}/{object_name}",
            "region":       region,
            "file_name":    file_name,
            "trigger_type": "gcs_event",
            "bucket":       bucket_name,
        }
        result = await trigger_airflow_dag(DAG_ID, dag_conf, token)
        log.info("DAG triggered successfully: %s", result.get("dag_run_id", "unknown"))

        return {
            "status":      "triggered",
            "dag_id":      DAG_ID,
            "dag_run_id":  result.get("dag_run_id"),
            "source_file": f"gs://{bucket_name}/{object_name}",
            "region":      region,
        }

    except httpx.HTTPStatusError as exc:
        log.error("Failed to trigger DAG: %s %s", exc.response.status_code, exc.response.text)
        raise HTTPException(
            status_code=500,
            detail=f"DAG trigger failed: {exc.response.status_code} — {exc.response.text}",
        )
    except Exception as exc:
        log.exception("Unexpected error triggering DAG: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
