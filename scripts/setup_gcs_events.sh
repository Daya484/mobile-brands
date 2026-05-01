#!/usr/bin/env bash
# ==============================================================================
# setup_gcs_events.sh — Wire GCS bucket notifications to Cloud Run via Pub/Sub
# ==============================================================================
#
# This replaces the scheduled Airflow trigger with event-driven triggering.
# When a new Excel file is uploaded to the source bucket, the pipeline
# starts AUTOMATICALLY within seconds — no waiting for the 6 AM schedule.
#
# What this creates (per environment):
#   1. Pub/Sub topic to receive GCS notifications
#   2. GCS bucket notification (OBJECT_FINALIZE) → Pub/Sub topic
#   3. Pub/Sub push subscription → Cloud Run /gcs-event endpoint
#   4. IAM: allows Pub/Sub to invoke Cloud Run
#
# Usage:
#   ENV=dv bash scripts/setup_gcs_events.sh
#   ENV=pd bash scripts/setup_gcs_events.sh
# ==============================================================================

set -euo pipefail

ENV="${ENV:-dv}"
REGION="${REGION:-us-central1}"

# ── PICK ENV ──────────────────────────────────────────────────────────────────
case "${ENV}" in
  dv)
    PROJECT_ID="dv-env"
    SOURCE_BUCKET="dv-mobile-brands"
    CLOUD_RUN_SERVICE="dv-mb-extractor"
    SA_EMAIL="mb-pipeline-sa@dv-env.iam.gserviceaccount.com"
    ;;
  pd)
    PROJECT_ID="prod-env"
    SOURCE_BUCKET="mobile-brands"
    CLOUD_RUN_SERVICE="pd-mb-extractor"
    SA_EMAIL="mb-pipeline-sa@prod-env.iam.gserviceaccount.com"
    ;;
  *)
    echo "ERROR: ENV must be 'dv' or 'pd'"; exit 1 ;;
esac

TOPIC_NAME="mb-gcs-events-${ENV}"
SUB_NAME="mb-gcs-push-sub-${ENV}"

echo "======================================================================"
echo "  Setting up GCS Event Trigger for ENV=${ENV}"
echo "  Project:  ${PROJECT_ID}"
echo "  Bucket:   gs://${SOURCE_BUCKET}"
echo "  Topic:    ${TOPIC_NAME}"
echo "======================================================================"

# ── Get Cloud Run URL ─────────────────────────────────────────────────────────
echo ""
echo "▶ Getting Cloud Run service URL..."
CLOUD_RUN_URL=$(gcloud run services describe "${CLOUD_RUN_SERVICE}" \
  --region="${REGION}" \
  --project="${PROJECT_ID}" \
  --format="value(status.url)")
echo "  Cloud Run URL: ${CLOUD_RUN_URL}"

# ── 1. Create Pub/Sub Topic ───────────────────────────────────────────────────
echo ""
echo "▶ Creating Pub/Sub topic: ${TOPIC_NAME}..."
gcloud pubsub topics create "${TOPIC_NAME}" \
  --project="${PROJECT_ID}" \
  2>/dev/null || echo "  (topic already exists)"
echo "  ✅ Topic ready."

# ── 2. Grant GCS service account permission to publish to topic ──────────────
echo ""
echo "▶ Granting GCS → Pub/Sub publish permission..."
GCS_SA=$(gcloud storage buckets describe "gs://${SOURCE_BUCKET}" \
  --project="${PROJECT_ID}" \
  --format="value(storageClass)" 2>/dev/null || echo "")

# GCS uses a project-level service account to publish notifications
PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format="value(projectNumber)")
GCS_PUBSUB_SA="service-${PROJECT_NUMBER}@gs-project-accounts.iam.gserviceaccount.com"

gcloud pubsub topics add-iam-policy-binding "${TOPIC_NAME}" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${GCS_PUBSUB_SA}" \
  --role="roles/pubsub.publisher" \
  2>/dev/null || echo "  (binding may already exist)"
echo "  ✅ Publish permission granted."

# ── 3. Create GCS Bucket Notification ────────────────────────────────────────
echo ""
echo "▶ Creating GCS bucket notification → Pub/Sub..."
echo "  Watching: gs://${SOURCE_BUCKET}  on OBJECT_FINALIZE"

# Remove existing notifications first (avoid duplicates)
EXISTING=$(gsutil notification list "gs://${SOURCE_BUCKET}" 2>/dev/null | grep "projects/" | awk '{print $1}' || echo "")
if [ -n "${EXISTING}" ]; then
  echo "  Removing existing notifications..."
  gsutil notification delete "${EXISTING}" 2>/dev/null || true
fi

gsutil notification create \
  -f json \
  -t "projects/${PROJECT_ID}/topics/${TOPIC_NAME}" \
  -e OBJECT_FINALIZE \
  "gs://${SOURCE_BUCKET}"
echo "  ✅ GCS notification created."

# ── 4. Create Pub/Sub Push Subscription → Cloud Run ──────────────────────────
echo ""
echo "▶ Creating Pub/Sub push subscription → Cloud Run /gcs-event..."

gcloud pubsub subscriptions create "${SUB_NAME}" \
  --topic="${TOPIC_NAME}" \
  --project="${PROJECT_ID}" \
  --push-endpoint="${CLOUD_RUN_URL}/gcs-event" \
  --push-auth-service-account="${SA_EMAIL}" \
  --ack-deadline=60 \
  --message-retention-duration=1d \
  2>/dev/null || {
    # Update if already exists
    gcloud pubsub subscriptions modify-push-config "${SUB_NAME}" \
      --project="${PROJECT_ID}" \
      --push-endpoint="${CLOUD_RUN_URL}/gcs-event" \
      --push-auth-service-account="${SA_EMAIL}"
    echo "  (subscription updated)"
  }
echo "  ✅ Push subscription ready."

# ── 5. Allow Pub/Sub SA to invoke Cloud Run ───────────────────────────────────
echo ""
echo "▶ Granting Pub/Sub SA permission to invoke Cloud Run..."
PUBSUB_SA="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

gcloud run services add-iam-policy-binding "${CLOUD_RUN_SERVICE}" \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --member="serviceAccount:${PUBSUB_SA}" \
  --role="roles/run.invoker" \
  2>/dev/null || echo "  (binding may already exist)"
echo "  ✅ Cloud Run invoke permission granted."

# ── SUMMARY ───────────────────────────────────────────────────────────────────
echo ""
echo "======================================================================"
echo "  ✅ GCS Event Trigger Setup Complete [${ENV}]!"
echo ""
echo "  Flow:"
echo "    Excel uploaded to gs://${SOURCE_BUCKET}/INDIA/sales.xlsx"
echo "       │"
echo "       ▼ (GCS OBJECT_FINALIZE notification)"
echo "    Pub/Sub topic: ${TOPIC_NAME}"
echo "       │"
echo "       ▼ (push subscription)"
echo "    Cloud Run POST /gcs-event: ${CLOUD_RUN_URL}/gcs-event"
echo "       │"
echo "       ▼ (Composer REST API)"
echo "    Airflow DAG: ${ENV}_mobile_brands_pipeline triggered!"
echo ""
echo "  Test it:"
echo "    gsutil cp sample.xlsx gs://${SOURCE_BUCKET}/INDIA/test_upload.xlsx"
echo "    # Watch: Cloud Run logs + Composer DAG runs"
echo "======================================================================"
