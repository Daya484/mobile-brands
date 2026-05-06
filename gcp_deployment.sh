#!/usr/bin/env bash
# ==============================================================================
# gcp_deployment.sh — Full End-to-End Deployment: Mobile Brands GCP Pipeline
# ==============================================================================
# Usage (in Cloud Shell):
#   git clone https://github.com/Daya484/mobile-brands.git
#   cd mobile-brands
#
#   # Deploy Development:
#   ENV=dv bash gcp_deployment.sh
#
#   # Deploy Production:
#   ENV=pd bash gcp_deployment.sh
#
#   # Deploy Both:
#   bash gcp_deployment.sh
# ==============================================================================

set -euo pipefail

# ── COLOUR HELPERS ─────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()    { echo -e "${YELLOW}▶ $*${NC}"; }
success() { echo -e "${GREEN}✅ $*${NC}"; }
error()   { echo -e "${RED}❌ $*${NC}"; exit 1; }
banner()  { echo -e "\n${GREEN}══════════════════════════════════════════════════${NC}";
            echo -e "${GREEN}  $*${NC}";
            echo -e "${GREEN}══════════════════════════════════════════════════${NC}\n"; }

# ── GLOBAL CONFIG ──────────────────────────────────────────────────────────────
TARGET_ENV="${ENV:-both}"   # "dv", "pd", or "both"
REGION="${REGION:-us-central1}"
SA_NAME="mb-pipeline-sa"
BQ_DATASET="mobile_brands"
GITHUB_OWNER="Daya484"
GITHUB_REPO="mobile-brands"

# ── ENV DEFINITIONS ────────────────────────────────────────────────────────────
declare -A PROJECT_IDS=( ["dv"]="dv-env"              ["pd"]="prod-env" )
declare -A PIPELINE_BUCKETS=( ["dv"]="dv-mb-pipeline-bucket" ["pd"]="pd-mb-pipeline-bucket" )
declare -A SOURCE_BUCKETS=(   ["dv"]="dv-mobile-brands"       ["pd"]="mobile-brands" )
declare -A SERVICES=(         ["dv"]="dv-mb-extractor"         ["pd"]="pd-mb-extractor" )
declare -A COMPOSERS=(        ["dv"]="dv-mb-composer"          ["pd"]="pd-mb-composer" )

# ==============================================================================
# MAIN DEPLOY FUNCTION — runs all phases for one environment
# ==============================================================================
deploy_env() {
  local env=$1
  local project="${PROJECT_IDS[$env]}"
  local pipeline_bucket="${PIPELINE_BUCKETS[$env]}"
  local source_bucket="${SOURCE_BUCKETS[$env]}"
  local service="${SERVICES[$env]}"
  local composer="${COMPOSERS[$env]}"
  local sa_email="${SA_NAME}@${project}.iam.gserviceaccount.com"
  local repo_name="mb-repo"
  local image_url="${REGION}-docker.pkg.dev/${project}/${repo_name}/mb-extractor:latest"

  banner "Deploying ENV=${env}  |  PROJECT=${project}  |  REGION=${REGION}"

  # ── PHASE 1: Set active project ──────────────────────────────────────────────
  info "[${env}] Setting active GCP project..."
  gcloud config set project "${project}"
  gcloud config set compute/region "${REGION}"
  success "[${env}] Active project: ${project}"

  # ── PHASE 2: Enable APIs ─────────────────────────────────────────────────────
  info "[${env}] Enabling GCP APIs..."
  gcloud services enable \
    run.googleapis.com \
    dataproc.googleapis.com \
    composer.googleapis.com \
    bigquery.googleapis.com \
    bigquerystorage.googleapis.com \
    cloudbuild.googleapis.com \
    artifactregistry.googleapis.com \
    storage.googleapis.com \
    iam.googleapis.com \
    cloudresourcemanager.googleapis.com \
    secretmanager.googleapis.com \
    --project="${project}"
  success "[${env}] APIs enabled"

  # ── PHASE 3: Service Account & IAM ──────────────────────────────────────────
  info "[${env}] Creating service account: ${sa_email}..."
  gcloud iam service-accounts create "${SA_NAME}" \
    --display-name="Mobile Brands Pipeline SA [${env}]" \
    --project="${project}" 2>/dev/null || echo "  (already exists — skipping)"

  info "[${env}] Granting IAM roles..."
  for ROLE in \
    "roles/storage.admin" \
    "roles/bigquery.admin" \
    "roles/dataproc.editor" \
    "roles/run.invoker" \
    "roles/run.developer" \
    "roles/composer.worker" \
    "roles/artifactregistry.writer" \
    "roles/iam.serviceAccountUser" \
    "roles/logging.logWriter" \
    "roles/monitoring.metricWriter"; do
      gcloud projects add-iam-policy-binding "${project}" \
        --member="serviceAccount:${sa_email}" \
        --role="${ROLE}" --quiet 2>/dev/null || true
      echo "    ✅ ${ROLE}"
  done
  success "[${env}] Service account & IAM ready"

  # ── PHASE 4: GCS Buckets ─────────────────────────────────────────────────────
  info "[${env}] Creating GCS buckets..."
  gcloud storage buckets create "gs://${source_bucket}" \
    --location="${REGION}" --uniform-bucket-level-access \
    --project="${project}" 2>/dev/null || echo "  gs://${source_bucket} already exists"

  gcloud storage buckets create "gs://${pipeline_bucket}" \
    --location="${REGION}" --uniform-bucket-level-access \
    --project="${project}" 2>/dev/null || echo "  gs://${pipeline_bucket} already exists"

  for PREFIX in raw bronze silver gold scripts; do
    echo "" | gcloud storage cp - "gs://${pipeline_bucket}/${PREFIX}/.keep" \
      --project="${project}" 2>/dev/null || true
    echo "    ✅ ${PREFIX}/"
  done
  success "[${env}] GCS buckets ready"

  # ── PHASE 5: Artifact Registry ───────────────────────────────────────────────
  info "[${env}] Creating Artifact Registry repo: ${repo_name}..."
  gcloud artifacts repositories create "${repo_name}" \
    --repository-format=docker \
    --location="${REGION}" \
    --description="Mobile Brands Docker images [${env}]" \
    --project="${project}" 2>/dev/null || echo "  (already exists)"
  success "[${env}] Artifact Registry ready"

  # ── PHASE 6: Build & Deploy Cloud Run ────────────────────────────────────────
  info "[${env}] Granting Cloud Build service accounts storage access..."
  local project_number
  project_number=$(gcloud projects describe "${project}" --format="value(projectNumber)")

  # Compute Engine default SA (used by Cloud Build in newer projects)
  gcloud projects add-iam-policy-binding "${project}" \
    --member="serviceAccount:${project_number}-compute@developer.gserviceaccount.com" \
    --role="roles/storage.admin" --quiet 2>/dev/null || true

  # Legacy Cloud Build SA
  gcloud projects add-iam-policy-binding "${project}" \
    --member="serviceAccount:${project_number}@cloudbuild.gserviceaccount.com" \
    --role="roles/storage.admin" --quiet 2>/dev/null || true

  gcloud projects add-iam-policy-binding "${project}" \
    --member="serviceAccount:${project_number}@cloudbuild.gserviceaccount.com" \
    --role="roles/artifactregistry.writer" --quiet 2>/dev/null || true

  success "[${env}] Cloud Build permissions ready"

  info "[${env}] Building Docker image via Cloud Build (no local Docker needed)..."
  gcloud builds submit cloud-run/ \
    --tag="${image_url}" \
    --project="${project}"
  success "[${env}] Docker image built & pushed: ${image_url}"

  info "[${env}] Deploying Cloud Run service: ${service}..."
  gcloud run deploy "${service}" \
    --image="${image_url}" \
    --region="${REGION}" \
    --platform=managed \
    --service-account="${sa_email}" \
    --no-allow-unauthenticated \
    --memory=2Gi \
    --cpu=2 \
    --timeout=900 \
    --max-instances=5 \
    --set-env-vars="ENV=${env},PROJECT_ID=${project},SOURCE_BUCKET=${source_bucket},DEST_BUCKET=${pipeline_bucket},LOG_LEVEL=INFO" \
    --project="${project}"

  local cloud_run_url
  cloud_run_url=$(gcloud run services describe "${service}" \
    --region="${REGION}" --project="${project}" \
    --format="value(status.url)")
  success "[${env}] Cloud Run deployed: ${cloud_run_url}"

  # Health check
  info "[${env}] Running health check..."
  local token
  token=$(gcloud auth print-identity-token)
  curl -s -H "Authorization: Bearer ${token}" "${cloud_run_url}/health" && echo ""
  success "[${env}] Health check passed"

  # ── PHASE 7: BigQuery ────────────────────────────────────────────────────────
  info "[${env}] Setting up BigQuery dataset..."
  bq --project_id="${project}" mk \
    --dataset \
    --location="${REGION}" \
    --description="Mobile Brands sales data — medallion pipeline" \
    "${project}:${BQ_DATASET}" 2>/dev/null || echo "  (dataset already exists)"

  info "[${env}] Creating BigQuery tables..."
  pip install google-cloud-bigquery --quiet
  python3 scripts/setup_bigquery.py \
    --project_id="${project}" \
    --dataset="${BQ_DATASET}"
  success "[${env}] BigQuery ready — dataset: ${project}.${BQ_DATASET}"

  # ── PHASE 8: PySpark Scripts (pipeline bucket) ────────────────────────────────
  info "[${env}] Uploading PySpark scripts to pipeline bucket..."
  gsutil -m cp \
    dataproc/bronze_job.py \
    dataproc/silver_job.py \
    dataproc/gold_job.py \
    config/env_config.py \
    transformed.py \
    "gs://${pipeline_bucket}/scripts/"
  success "[${env}] PySpark scripts uploaded to gs://${pipeline_bucket}/scripts/"

  # ── PHASE 9: Cloud Composer ───────────────────────────────────────────────────
  info "[${env}] Creating Cloud Composer environment: ${composer} (this takes 20–30 min)..."
  gcloud composer environments create "${composer}" \
    --location="${REGION}" \
    --image-version="composer-2.6.6-airflow-2.7.3" \
    --service-account="${sa_email}" \
    --environment-size=small \
    --project="${project}" 2>/dev/null || echo "  (already exists)"

  local composer_bucket
  composer_bucket=$(gcloud composer environments describe "${composer}" \
    --location="${REGION}" --project="${project}" \
    --format="value(config.dagGcsPrefix)" | sed 's|gs://||' | cut -d'/' -f1)
  success "[${env}] Composer ready — GCS: gs://${composer_bucket}"

  # ── PHASE 10: Upload DAG & Scripts to Composer ───────────────────────────────
  info "[${env}] Uploading DAG to Composer..."
  gcloud storage cp dags/mobile_brands_dag.py \
    "gs://${composer_bucket}/dags/mobile_brands_dag.py"

  info "[${env}] Uploading PySpark scripts to Composer..."
  gcloud storage cp dataproc/bronze_job.py "gs://${composer_bucket}/dags/dataproc/"
  gcloud storage cp dataproc/silver_job.py "gs://${composer_bucket}/dags/dataproc/"
  gcloud storage cp dataproc/gold_job.py   "gs://${composer_bucket}/dags/dataproc/"
  gcloud storage cp config/env_config.py   "gs://${composer_bucket}/dags/config/"
  success "[${env}] DAG & scripts uploaded to Composer"

  # ── PHASE 11: Airflow Variables & Connection ──────────────────────────────────
  info "[${env}] Setting Airflow variable: MB_ENV=${env}..."
  gcloud composer environments run "${composer}" \
    --location="${REGION}" --project="${project}" \
    variables -- set MB_ENV "${env}"

  info "[${env}] Setting Airflow variable: MB_CLOUD_RUN_URL..."
  gcloud composer environments run "${composer}" \
    --location="${REGION}" --project="${project}" \
    variables -- set MB_CLOUD_RUN_URL "${cloud_run_url}"

  info "[${env}] Creating Airflow connection: mb_${env}_cloud_run_conn..."
  local cloud_run_host
  cloud_run_host=$(echo "${cloud_run_url}" | sed 's|https://||')
  gcloud composer environments run "${composer}" \
    --location="${REGION}" --project="${project}" \
    connections -- add "mb_${env}_cloud_run_conn" \
      --conn-type=http \
      --conn-host="${cloud_run_host}" \
      --conn-schema=https \
      --conn-port=443
  success "[${env}] Airflow variables & connection set"

  # ── PHASE 12: Cloud Build IAM Permissions ────────────────────────────────────
  info "[${env}] Granting Cloud Build service account permissions..."
  local cb_sa
  cb_sa="$(gcloud projects describe "${project}" \
    --format='value(projectNumber)')@cloudbuild.gserviceaccount.com"

  for ROLE in "roles/run.admin" "roles/storage.admin" \
              "roles/iam.serviceAccountUser" "roles/artifactregistry.writer"; do
    gcloud projects add-iam-policy-binding "${project}" \
      --member="serviceAccount:${cb_sa}" \
      --role="${ROLE}" --quiet 2>/dev/null || true
    echo "    ✅ CB granted: ${ROLE}"
  done
  success "[${env}] Cloud Build permissions granted"

  # ── SUMMARY ──────────────────────────────────────────────────────────────────
  echo ""
  echo -e "${GREEN}══════════════════════════════════════════════════════════════${NC}"
  echo -e "${GREEN}  ✅  ENV [${env}] DEPLOYMENT COMPLETE${NC}"
  echo -e "${GREEN}══════════════════════════════════════════════════════════════${NC}"
  echo "  Project:        ${project}"
  echo "  Cloud Run:      ${cloud_run_url}"
  echo "  Source Bucket:  gs://${source_bucket}"
  echo "  Pipeline Bucket:gs://${pipeline_bucket}"
  echo "  BQ Dataset:     ${project}.${BQ_DATASET}"
  echo "  Composer:       ${composer} (${REGION})"
  echo "  Composer GCS:   gs://${composer_bucket}"
  echo ""
  echo "  NEXT STEPS:"
  echo "  1. Open Airflow UI → toggle DAG ON: ${env}_mobile_brands_pipeline"
  echo "  2. Connect GitHub repo to Cloud Build (manual, one-time):"
  echo "     https://console.cloud.google.com/cloud-build/triggers"
  echo "  3. Run: bash scripts/setup_triggers.sh"
  echo -e "${GREEN}══════════════════════════════════════════════════════════════${NC}"
}

# ==============================================================================
# ENTRY POINT
# ==============================================================================
cd "$(dirname "$0")"   # Always run from mobile-brands/ root

banner "Mobile Brands GCP Pipeline — Full Deployment"
echo "  Target ENV : ${TARGET_ENV}"
echo "  Region     : ${REGION}"
echo ""

case "${TARGET_ENV}" in
  "dv")   deploy_env "dv" ;;
  "pd")   deploy_env "pd" ;;
  "both") deploy_env "dv"; deploy_env "pd" ;;
  *)      error "ENV must be 'dv', 'pd', or 'both'. Got: '${TARGET_ENV}'" ;;
esac

banner "🎉 All Done! Deployment complete for: ${TARGET_ENV}"
