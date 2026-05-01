#!/usr/bin/env bash
# ==============================================================================
# deploy.sh — Bootstrap script for Mobile Brands GCP Pipeline
# ==============================================================================
# Provisions resources for BOTH dv and pd environments.
# Run once per GCP project to set up the complete infrastructure.
#
# Usage:
#   # Deploy DV only:
#   ENV=dv bash scripts/deploy.sh
#
#   # Deploy PD only:
#   ENV=pd bash scripts/deploy.sh
#
#   # Deploy both (default):
#   bash scripts/deploy.sh
# ==============================================================================

set -euo pipefail

TARGET_ENV="${ENV:-both}"   # "dv", "pd", or "both"
REGION="${REGION:-us-central1}"

echo "======================================================================"
echo "  Mobile Brands Pipeline — Bootstrap"
echo "  Target env: ${TARGET_ENV}  |  Region: ${REGION}"
echo "======================================================================"

# ── ENV DEFINITIONS ───────────────────────────────────────────────────────────
declare -A PROJECT_IDS=(["dv"]="dv-env"    ["pd"]="prod-env")
declare -A BUCKETS=(    ["dv"]="dv-mb-pipeline-bucket"  ["pd"]="pd-mb-pipeline-bucket")
declare -A SRC_BUCKETS=(["dv"]="dv-mobile-brands"       ["pd"]="mobile-brands")
declare -A SERVICES=(   ["dv"]="dv-mb-extractor"         ["pd"]="pd-mb-extractor")
declare -A COMPOSERS=(  ["dv"]="dv-mb-composer"          ["pd"]="pd-mb-composer")
declare -A AR_REPOS=(   ["dv"]="mb-repo"                 ["pd"]="mb-repo")

SA_NAME="mb-pipeline-sa"   # same SA name in each project
BQ_DATASET="mobile_brands"

# ─────────────────────────────────────────────────────────────────────────────
bootstrap_env() {
  local env=$1
  local project="${PROJECT_IDS[$env]}"
  local bucket="${BUCKETS[$env]}"
  local src_bucket="${SRC_BUCKETS[$env]}"
  local service="${SERVICES[$env]}"
  local composer="${COMPOSERS[$env]}"
  local ar_repo="${AR_REPOS[$env]}"
  local sa_email="${SA_NAME}@${project}.iam.gserviceaccount.com"
  local ar_path="${REGION}-docker.pkg.dev/${project}/${ar_repo}"

  echo ""
  echo "══════════════════════════════════════════════════════════════"
  echo "  Bootstrapping ENV=${env}  PROJECT=${project}"
  echo "══════════════════════════════════════════════════════════════"

  # ── 1. Enable APIs ──────────────────────────────────────────────────────────
  echo ""
  echo "[${env}] ▶ Enabling GCP APIs..."
  gcloud services enable \
    run.googleapis.com \
    dataproc.googleapis.com \
    composer.googleapis.com \
    bigquery.googleapis.com \
    cloudbuild.googleapis.com \
    artifactregistry.googleapis.com \
    storage.googleapis.com \
    iam.googleapis.com \
    --project="${project}"
  echo "[${env}] ✅ APIs enabled."

  # ── 2. Service Account ──────────────────────────────────────────────────────
  echo ""
  echo "[${env}] ▶ Creating service account: ${sa_email}..."
  gcloud iam service-accounts create "${SA_NAME}" \
    --display-name="Mobile Brands Pipeline SA [${env}]" \
    --project="${project}" \
    2>/dev/null || echo "  (already exists)"

  for role in \
    "roles/storage.admin" \
    "roles/bigquery.admin" \
    "roles/dataproc.editor" \
    "roles/run.invoker" \
    "roles/composer.worker" \
    "roles/artifactregistry.writer" \
    "roles/iam.serviceAccountUser"; do
    gcloud projects add-iam-policy-binding "${project}" \
      --member="serviceAccount:${sa_email}" \
      --role="${role}" \
      --quiet 2>/dev/null || true
  done
  echo "[${env}] ✅ Service account ready."

  # ── 3. GCS Buckets ─────────────────────────────────────────────────────────
  echo ""
  echo "[${env}] ▶ Creating GCS buckets..."

  gcloud storage buckets create "gs://${bucket}" \
    --project="${project}" --location="${REGION}" \
    --uniform-bucket-level-access \
    2>/dev/null || echo "  gs://${bucket} already exists."

  gcloud storage buckets create "gs://${src_bucket}" \
    --project="${project}" --location="${REGION}" \
    --uniform-bucket-level-access \
    2>/dev/null || echo "  gs://${src_bucket} already exists."

  for prefix in raw/ bronze/ silver/ gold/ scripts/; do
    echo "" | gsutil cp - "gs://${bucket}/${prefix}.keep" 2>/dev/null || true
  done
  echo "[${env}] ✅ GCS buckets ready."

  # ── 4. Artifact Registry ───────────────────────────────────────────────────
  echo ""
  echo "[${env}] ▶ Creating Artifact Registry repo: ${ar_repo}..."
  gcloud artifacts repositories create "${ar_repo}" \
    --repository-format=docker \
    --location="${REGION}" \
    --project="${project}" \
    --description="Mobile Brands Docker images [${env}]" \
    2>/dev/null || echo "  (already exists)"
  gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet
  echo "[${env}] ✅ Artifact Registry ready."

  # ── 5. BigQuery ────────────────────────────────────────────────────────────
  echo ""
  echo "[${env}] ▶ Setting up BigQuery..."
  python3 scripts/setup_bigquery.py \
    --project_id="${project}" \
    --dataset="${BQ_DATASET}"
  echo "[${env}] ✅ BigQuery ready."

  # ── 6. Upload PySpark Scripts ──────────────────────────────────────────────
  echo ""
  echo "[${env}] ▶ Uploading PySpark scripts..."
  gsutil -m cp \
    dataproc/bronze_job.py \
    dataproc/silver_job.py \
    dataproc/gold_job.py \
    config/env_config.py \
    transformed.py \
    "gs://${bucket}/scripts/"
  echo "[${env}] ✅ Scripts uploaded."

  # ── 7. Build & Deploy Cloud Run ───────────────────────────────────────────
  echo ""
  echo "[${env}] ▶ Building Docker image and deploying Cloud Run..."

  gcloud builds submit cloud-run/ \
    --tag="${ar_path}/mb-extractor:${env}-latest" \
    --project="${project}"

  gcloud run deploy "${service}" \
    --image="${ar_path}/mb-extractor:${env}-latest" \
    --region="${REGION}" \
    --platform=managed \
    --service-account="${sa_email}" \
    --no-allow-unauthenticated \
    --memory=2Gi \
    --cpu=2 \
    --timeout=900 \
    --set-env-vars="ENV=${env},PROJECT_ID=${project},SOURCE_BUCKET=${src_bucket},DEST_BUCKET=${bucket}" \
    --project="${project}"

  local cloud_run_url
  cloud_run_url=$(gcloud run services describe "${service}" \
    --region="${REGION}" --project="${project}" \
    --format="value(status.url)")
  echo "[${env}] ✅ Cloud Run deployed: ${cloud_run_url}"

  # ── 8. Cloud Composer ─────────────────────────────────────────────────────
  echo ""
  echo "[${env}] ▶ Creating Cloud Composer environment (this takes ~20-30 min)..."
  gcloud composer environments create "${composer}" \
    --location="${REGION}" \
    --image-version="composer-2-airflow-2" \
    --service-account="${sa_email}" \
    --project="${project}" \
    2>/dev/null || echo "  (already exists)"

  local composer_bucket
  composer_bucket=$(gcloud composer environments describe "${composer}" \
    --location="${REGION}" --project="${project}" \
    --format="value(config.dagGcsPrefix)" | sed 's|/dags||')

  echo "[${env}] Uploading DAG..."
  gsutil cp dags/mobile_brands_dag.py "${composer_bucket}/dags/"
  gsutil -m rsync -r dataproc/ "${composer_bucket}/dags/dataproc/"
  gsutil -m rsync -r config/   "${composer_bucket}/dags/config/"

  echo "[${env}] Setting Airflow Variables..."
  gcloud composer environments run "${composer}" \
    --location="${REGION}" --project="${project}" \
    variables -- set MB_ENV "${env}"

  gcloud composer environments run "${composer}" \
    --location="${REGION}" --project="${project}" \
    variables -- set MB_CLOUD_RUN_URL "${cloud_run_url}"

  # ── 9. Set up Airflow Connection for Cloud Run ────────────────────────────
  echo "[${env}] Setting Airflow Connection: mb_${env}_cloud_run_conn..."
  gcloud composer environments run "${composer}" \
    --location="${REGION}" --project="${project}" \
    connections -- add "mb_${env}_cloud_run_conn" \
      --conn-type=http \
      --conn-host="${cloud_run_url}"

  echo ""
  echo "[${env}] ══════════════════════════════════════════════════════════"
  echo "[${env}] ENV ${env} bootstrap complete!"
  echo "[${env}]   Cloud Run:   ${cloud_run_url}"
  echo "[${env}]   GCS Bucket:  gs://${bucket}"
  echo "[${env}]   BQ Dataset:  ${project}.${BQ_DATASET}"
  echo "[${env}]   Composer:    ${composer} (${REGION})"
  echo "[${env}]   AR:          ${ar_path}"
  echo "[${env}]   Composer GCS:${composer_bucket}"
  echo "[${env}] ══════════════════════════════════════════════════════════"

  # Export composer bucket so setup_triggers.sh can use it
  echo "${composer_bucket}" > "/tmp/mb_${env}_composer_bucket.txt"
}

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

cd "$(dirname "$0")/.."   # always run from mobile-brands/

case "${TARGET_ENV}" in
  "dv")   bootstrap_env "dv" ;;
  "pd")   bootstrap_env "pd" ;;
  "both") bootstrap_env "dv"; bootstrap_env "pd" ;;
  *)      echo "ERROR: ENV must be 'dv', 'pd', or 'both'"; exit 1 ;;
esac

echo ""
echo "======================================================================"
echo "  ✅ Bootstrap complete for: ${TARGET_ENV}"
echo ""
echo "  NEXT STEPS:"
echo "  1. Connect GitHub repo to Cloud Build in GCP Console"
echo "  2. Run: bash scripts/setup_triggers.sh"
echo "  3. Push a feature branch to test DV trigger:"
echo "       git checkout -b feature/test-pipeline"
echo "       git push origin feature/test-pipeline"
echo "  4. Merge to main to test PD trigger:"
echo "       git checkout main && git merge feature/test-pipeline"
echo "       git push origin main"
echo "======================================================================"
