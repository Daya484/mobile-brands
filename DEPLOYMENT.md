# 🚀 Deployment Guide — Mobile Brands GCP Pipeline

> Step-by-step instructions to deploy the entire pipeline from scratch on GCP.
> Estimated total setup time: **~60–90 minutes**

---

## 📋 Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [GCP Project Setup](#2-gcp-project-setup)
3. [Enable GCP APIs](#3-enable-gcp-apis)
4. [Create Service Account & IAM Roles](#4-create-service-account--iam-roles)
5. [Create GCS Buckets](#5-create-gcs-buckets)
6. [Setup Artifact Registry](#6-setup-artifact-registry)
7. [Build & Deploy Cloud Run](#7-build--deploy-cloud-run)
8. [Setup BigQuery](#8-setup-bigquery)
9. [Deploy Cloud Composer](#9-deploy-cloud-composer)
10. [Upload DAG & PySpark Scripts](#10-upload-dag--pyspark-scripts)
11. [Configure Airflow Variables & Connections](#11-configure-airflow-variables--connections)
12. [Setup Cloud Build CI/CD](#12-setup-cloud-build-cicd)
13. [Test End-to-End](#13-test-end-to-end)
14. [Monitoring & Troubleshooting](#14-monitoring--troubleshooting)

---

## 1. Prerequisites

### Local Machine Requirements

```bash
# Check all tools are installed
gcloud --version        # Google Cloud CLI (>= 450.0.0)
docker --version        # Docker Desktop
python --version        # Python 3.11+
git --version           # Git
```

### Install gcloud CLI (if not installed)
Download from: https://cloud.google.com/sdk/docs/install

```bash
# After install, initialize
gcloud init
gcloud auth login
gcloud auth application-default login
```

### GCP Requirements
- ✅ A Google Cloud account with billing enabled
- ✅ Two GCP projects created:
  - **Development:** Project ID = `dv-env`
  - **Production:** Project ID = `prod-env`
- ✅ Owner or Editor role on both projects

> **Tip:** Create projects at https://console.cloud.google.com/projectcreate

---

## 2. GCP Project Setup

Run all commands below **twice** — once for DV, once for PD.

```bash
# ── SET ENVIRONMENT ──────────────────────────────────────────
# For Development:
export ENV=dv
export PROJECT_ID=dv-env
export REGION=us-central1

# For Production (run again with these values):
# export ENV=pd
# export PROJECT_ID=prod-env
# export REGION=us-central1

# ── SET ACTIVE PROJECT ────────────────────────────────────────
gcloud config set project $PROJECT_ID
gcloud config set compute/region $REGION

# Verify
gcloud config list
```

---

## 3. Enable GCP APIs

```bash
# Enable all required APIs in one command
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
  --project=$PROJECT_ID

echo "✅ APIs enabled for $PROJECT_ID"
```

> ⏱️ This takes ~2 minutes. Wait for the command to finish before proceeding.

---

## 4. Create Service Account & IAM Roles

```bash
# ── CREATE SERVICE ACCOUNT ────────────────────────────────────
export SA_NAME=mb-pipeline-sa
export SA_EMAIL=${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com

gcloud iam service-accounts create $SA_NAME \
  --display-name="Mobile Brands Pipeline Service Account" \
  --project=$PROJECT_ID

echo "✅ Service account created: $SA_EMAIL"

# ── GRANT IAM ROLES ──────────────────────────────────────────
ROLES=(
  "roles/storage.admin"
  "roles/bigquery.admin"
  "roles/dataproc.editor"
  "roles/run.invoker"
  "roles/run.developer"
  "roles/composer.worker"
  "roles/artifactregistry.writer"
  "roles/iam.serviceAccountUser"
  "roles/logging.logWriter"
  "roles/monitoring.metricWriter"
)

for ROLE in "${ROLES[@]}"; do
  gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="$ROLE" \
    --quiet
  echo "  ✅ Granted: $ROLE"
done

echo "✅ All IAM roles granted to $SA_EMAIL"
```

---

## 5. Create GCS Buckets

```bash
# ── BUCKET NAMES ─────────────────────────────────────────────
# DV:  dv-mobile-brands (source) + dv-mb-pipeline-bucket (pipeline)
# PD:  mobile-brands (source)    + pd-mb-pipeline-bucket (pipeline)

if [ "$ENV" = "dv" ]; then
  SOURCE_BUCKET=dv-mobile-brands
  PIPELINE_BUCKET=dv-mb-pipeline-bucket
else
  SOURCE_BUCKET=mobile-brands
  PIPELINE_BUCKET=pd-mb-pipeline-bucket
fi

# ── CREATE SOURCE BUCKET (where distributors upload Excel files) ──
gcloud storage buckets create gs://$SOURCE_BUCKET \
  --location=$REGION \
  --uniform-bucket-level-access \
  --project=$PROJECT_ID

echo "✅ Source bucket: gs://$SOURCE_BUCKET"

# ── CREATE PIPELINE BUCKET (raw/bronze/silver/gold/scripts) ──
gcloud storage buckets create gs://$PIPELINE_BUCKET \
  --location=$REGION \
  --uniform-bucket-level-access \
  --project=$PROJECT_ID

echo "✅ Pipeline bucket: gs://$PIPELINE_BUCKET"

# ── CREATE FOLDER PREFIXES ────────────────────────────────────
# GCS doesn't have real folders but we create placeholder objects
for PREFIX in raw bronze silver gold scripts; do
  echo "" | gcloud storage cp - gs://$PIPELINE_BUCKET/$PREFIX/.keep \
    --project=$PROJECT_ID
  echo "  ✅ Created folder: $PREFIX/"
done

echo "✅ All GCS buckets and folders created"
```

---

## 6. Setup Artifact Registry

```bash
# ── CREATE DOCKER REPOSITORY ──────────────────────────────────
export REPO_NAME=mb-repo

gcloud artifacts repositories create $REPO_NAME \
  --repository-format=docker \
  --location=$REGION \
  --description="Mobile Brands Docker images" \
  --project=$PROJECT_ID

echo "✅ Artifact Registry repository created: $REGION-docker.pkg.dev/$PROJECT_ID/$REPO_NAME"

# ── CONFIGURE DOCKER TO USE ARTIFACT REGISTRY ─────────────────
gcloud auth configure-docker ${REGION}-docker.pkg.dev --quiet

echo "✅ Docker configured for Artifact Registry"
```

---

## 7. Build & Deploy Cloud Run

```bash
# ── VARIABLES ─────────────────────────────────────────────────
export SERVICE_NAME=${ENV}-mb-extractor
export IMAGE_URL=${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/mb-extractor:latest

# ── BUILD DOCKER IMAGE ────────────────────────────────────────
echo "Building Docker image..."
cd cloud-run/

docker build -t $IMAGE_URL .

# ── PUSH TO ARTIFACT REGISTRY ─────────────────────────────────
echo "Pushing image to Artifact Registry..."
docker push $IMAGE_URL

echo "✅ Image pushed: $IMAGE_URL"

# ── DEPLOY TO CLOUD RUN ───────────────────────────────────────
echo "Deploying Cloud Run service..."
gcloud run deploy $SERVICE_NAME \
  --image=$IMAGE_URL \
  --region=$REGION \
  --platform=managed \
  --service-account=$SA_EMAIL \
  --set-env-vars="SOURCE_BUCKET=${SOURCE_BUCKET},DEST_BUCKET=${PIPELINE_BUCKET},LOG_LEVEL=INFO" \
  --memory=2Gi \
  --cpu=2 \
  --timeout=900 \
  --max-instances=5 \
  --no-allow-unauthenticated \
  --project=$PROJECT_ID

# ── GET SERVICE URL ───────────────────────────────────────────
export CLOUD_RUN_URL=$(gcloud run services describe $SERVICE_NAME \
  --region=$REGION \
  --project=$PROJECT_ID \
  --format="value(status.url)")

echo "✅ Cloud Run deployed: $CLOUD_RUN_URL"

# ── TEST HEALTH CHECK ─────────────────────────────────────────
# Get an auth token and test
TOKEN=$(gcloud auth print-identity-token)
curl -H "Authorization: Bearer $TOKEN" "${CLOUD_RUN_URL}/health"

cd ..
```

> **Note:** Cloud Run is set to `--no-allow-unauthenticated`. Only the service account can call it.

---

## 8. Setup BigQuery

```bash
# ── CREATE BIGQUERY DATASET ───────────────────────────────────
bq --project_id=$PROJECT_ID mk \
  --dataset \
  --location=$REGION \
  --description="Mobile Brands sales data — medallion pipeline" \
  ${PROJECT_ID}:mobile_brands

echo "✅ BigQuery dataset created: $PROJECT_ID.mobile_brands"

# ── CREATE ALL TABLES ─────────────────────────────────────────
echo "Creating BigQuery tables..."
python scripts/setup_bigquery.py \
  --project_id=$PROJECT_ID \
  --dataset=mobile_brands

echo "✅ BigQuery tables created:"
echo "   - $PROJECT_ID.mobile_brands.silver_sales"
echo "   - $PROJECT_ID.mobile_brands.gold_brand_kpi"
echo "   - $PROJECT_ID.mobile_brands.gold_region_summary"
echo "   - $PROJECT_ID.mobile_brands.gold_daily_trend"
```

### Verify in Console
1. Go to https://console.cloud.google.com/bigquery
2. Select project → `mobile_brands` dataset
3. Confirm 4 tables are visible

---

## 9. Deploy Cloud Composer

> ⚠️ **This is the longest step — Composer takes 20–30 minutes to provision.**

```bash
# ── COMPOSER ENVIRONMENT NAME ─────────────────────────────────
export COMPOSER_ENV=${ENV}-mb-composer

echo "Creating Composer environment: $COMPOSER_ENV"
echo "⏱️  This will take 20–30 minutes..."

gcloud composer environments create $COMPOSER_ENV \
  --location=$REGION \
  --image-version=composer-2.6.6-airflow-2.7.3 \
  --service-account=$SA_EMAIL \
  --environment-size=small \
  --project=$PROJECT_ID

echo "✅ Composer environment created: $COMPOSER_ENV"

# ── GET COMPOSER BUCKET ───────────────────────────────────────
export COMPOSER_BUCKET=$(gcloud composer environments describe $COMPOSER_ENV \
  --location=$REGION \
  --project=$PROJECT_ID \
  --format="value(config.dagGcsPrefix)" | sed 's|gs://||' | cut -d'/' -f1)

echo "✅ Composer GCS bucket: gs://$COMPOSER_BUCKET"
```

### How to check progress
Go to: https://console.cloud.google.com/composer/environments → watch status change from **CREATING** → **RUNNING**

---

## 10. Upload DAG & PySpark Scripts

```bash
# ── UPLOAD DAG ────────────────────────────────────────────────
echo "Uploading DAG to Composer..."
gcloud storage cp dags/mobile_brands_dag.py \
  gs://$COMPOSER_BUCKET/dags/mobile_brands_dag.py

echo "✅ DAG uploaded"

# ── UPLOAD PYSPARK SCRIPTS TO COMPOSER dags/ ─────────────────
# (The DAG's upload_pyspark_scripts task also copies these at runtime)
gcloud storage cp dataproc/bronze_job.py gs://$COMPOSER_BUCKET/dags/dataproc/bronze_job.py
gcloud storage cp dataproc/silver_job.py gs://$COMPOSER_BUCKET/dags/dataproc/silver_job.py
gcloud storage cp dataproc/gold_job.py  gs://$COMPOSER_BUCKET/dags/dataproc/gold_job.py
gcloud storage cp config/env_config.py  gs://$COMPOSER_BUCKET/dags/config/env_config.py

echo "✅ PySpark scripts uploaded to Composer"

# ── UPLOAD SCRIPTS TO PIPELINE BUCKET ────────────────────────
# So Dataproc can access them directly
gcloud storage cp dataproc/bronze_job.py gs://$PIPELINE_BUCKET/scripts/bronze_job.py
gcloud storage cp dataproc/silver_job.py gs://$PIPELINE_BUCKET/scripts/silver_job.py
gcloud storage cp dataproc/gold_job.py  gs://$PIPELINE_BUCKET/scripts/gold_job.py

echo "✅ PySpark scripts uploaded to pipeline bucket"
```

---

## 11. Configure Airflow Variables & Connections

### Set Airflow Variables

```bash
# ── SET MB_ENV VARIABLE ───────────────────────────────────────
gcloud composer environments run $COMPOSER_ENV \
  --location=$REGION \
  --project=$PROJECT_ID \
  variables set -- MB_ENV $ENV

# ── SET CLOUD RUN URL VARIABLE ────────────────────────────────
gcloud composer environments run $COMPOSER_ENV \
  --location=$REGION \
  --project=$PROJECT_ID \
  variables set -- MB_CLOUD_RUN_URL $CLOUD_RUN_URL

echo "✅ Airflow variables set: MB_ENV=$ENV, MB_CLOUD_RUN_URL=$CLOUD_RUN_URL"
```

### Set Airflow Connection for Cloud Run

```bash
# ── CREATE HTTP CONNECTION TO CLOUD RUN ───────────────────────
# The DAG uses this connection to call Cloud Run POST /extract

# Extract just the host from the URL (remove https://)
CLOUD_RUN_HOST=$(echo $CLOUD_RUN_URL | sed 's|https://||')

gcloud composer environments run $COMPOSER_ENV \
  --location=$REGION \
  --project=$PROJECT_ID \
  connections add -- \
  --conn-id="mb_${ENV}_cloud_run_conn" \
  --conn-type=http \
  --conn-host="$CLOUD_RUN_HOST" \
  --conn-schema=https \
  --conn-port=443

echo "✅ Airflow connection created: mb_${ENV}_cloud_run_conn"
```

### Verify in Airflow UI

1. Open Composer → Click **Open Airflow UI**
2. Go to **Admin → Variables** → check `MB_ENV` and `MB_CLOUD_RUN_URL`
3. Go to **Admin → Connections** → check `mb_dv_cloud_run_conn` (or `mb_pd_cloud_run_conn`)
4. Go to **DAGs** → look for `dv_mobile_brands_pipeline`
5. **Toggle the DAG ON** (it's paused by default)

---

## 12. Setup Cloud Build CI/CD

### Step 1 — Connect GitHub Repository (Manual — One Time)

1. Go to https://console.cloud.google.com/cloud-build/triggers
2. Select your project
3. Click **Connect Repository**
4. Choose **GitHub (Cloud Build GitHub App)**
5. Authenticate and select: `Daya484/mobile-brands`
6. Click **Done**

### Step 2 — Create Build Triggers

```bash
# ── SET YOUR GITHUB DETAILS ───────────────────────────────────
export GITHUB_OWNER=Daya484
export GITHUB_REPO=mobile-brands

# ── RUN TRIGGER SETUP SCRIPT ─────────────────────────────────
bash scripts/setup_triggers.sh

echo "✅ Cloud Build triggers created"
```

### What the triggers do

| Trigger | Branch Pattern | Target Env |
|---------|---------------|-----------|
| `mb-dv-trigger` | `feature/*` or `develop` | `dv-env` |
| `mb-pd-trigger` | `main` | `prod-env` |

### Step 3 — Add Cloud Build IAM Permission

```bash
# Get Cloud Build service account
CB_SA=$(gcloud projects describe $PROJECT_ID \
  --format="value(projectNumber)")@cloudbuild.gserviceaccount.com

# Grant it permission to deploy Cloud Run and Composer
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CB_SA" \
  --role="roles/run.admin" --quiet

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CB_SA" \
  --role="roles/storage.admin" --quiet

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$CB_SA" \
  --role="roles/iam.serviceAccountUser" --quiet

echo "✅ Cloud Build permissions granted"
```

---

## 13. Test End-to-End

### Step 1 — Upload a Test Excel File

```bash
# Upload a sample Excel file to the source bucket
# (Your file must have sheets named: Samsung, Apple, Oppo, Vivo, OnePlus)

gsutil cp your_sample_sales.xlsx gs://${SOURCE_BUCKET}/INDIA/sales_test_$(date +%Y%m%d).xlsx

echo "✅ Test file uploaded to gs://${SOURCE_BUCKET}/INDIA/"
```

### Step 2 — Manually Trigger the DAG

```bash
# Trigger the DAG manually (don't wait for 6:30 AM)
gcloud composer environments run $COMPOSER_ENV \
  --location=$REGION \
  --project=$PROJECT_ID \
  dags trigger -- ${ENV}_mobile_brands_pipeline

echo "✅ DAG triggered manually"
```

### Step 3 — Monitor Task Progress

```bash
# Watch DAG runs via CLI
gcloud composer environments run $COMPOSER_ENV \
  --location=$REGION \
  --project=$PROJECT_ID \
  dags list-runs -- -d ${ENV}_mobile_brands_pipeline
```

**Or in Airflow UI:**
1. Open Composer → **Open Airflow UI**
2. Find `dv_mobile_brands_pipeline`
3. Click the latest run → view each task's log

### Step 4 — Verify Data in GCS

```bash
# Check raw/ landing zone (CSV files)
gsutil ls gs://$PIPELINE_BUCKET/raw/

# Check bronze/ (Parquet)
gsutil ls gs://$PIPELINE_BUCKET/bronze/

# Check silver/ (Parquet)
gsutil ls gs://$PIPELINE_BUCKET/silver/

# Check gold/ (Parquet)
gsutil ls gs://$PIPELINE_BUCKET/gold/
```

### Step 5 — Verify Data in BigQuery

```bash
# Quick row count check on all 4 tables
bq query --use_legacy_sql=false --project_id=$PROJECT_ID "
SELECT 'silver_sales'      AS table_name, COUNT(*) AS rows FROM \`$PROJECT_ID.mobile_brands.silver_sales\`
UNION ALL
SELECT 'gold_brand_kpi',    COUNT(*) FROM \`$PROJECT_ID.mobile_brands.gold_brand_kpi\`
UNION ALL
SELECT 'gold_region_summary', COUNT(*) FROM \`$PROJECT_ID.mobile_brands.gold_region_summary\`
UNION ALL
SELECT 'gold_daily_trend',  COUNT(*) FROM \`$PROJECT_ID.mobile_brands.gold_daily_trend\`
"
```

### Step 6 — Test CI/CD Pipeline

```bash
# Push a small code change to trigger Cloud Build
git checkout -b feature/test-cicd
echo "# test" >> README.md
git add README.md
git commit -m "test: verify CI/CD pipeline triggers on push"
git push origin feature/test-cicd
```

Then check: https://console.cloud.google.com/cloud-build/builds → watch the build run.

---

## 14. Monitoring & Troubleshooting

### Useful GCP Console Links

| Service | URL |
|---------|-----|
| Cloud Run | https://console.cloud.google.com/run |
| Cloud Composer / Airflow | https://console.cloud.google.com/composer |
| Dataproc | https://console.cloud.google.com/dataproc |
| BigQuery | https://console.cloud.google.com/bigquery |
| GCS Buckets | https://console.cloud.google.com/storage |
| Cloud Build | https://console.cloud.google.com/cloud-build |
| Artifact Registry | https://console.cloud.google.com/artifacts |

---

### Common Issues & Fixes

#### ❌ Cloud Run returns 403 Forbidden
```bash
# The caller doesn't have run.invoker permission
# Fix: Ensure Composer's service account has the role
gcloud run services add-iam-policy-binding $SERVICE_NAME \
  --region=$REGION \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/run.invoker"
```

#### ❌ Dataproc cluster fails to create
```bash
# Check quota limits
gcloud compute regions describe $REGION --project=$PROJECT_ID | grep -i quota

# Most common: not enough CPUs in region — try a different region
# Or reduce num_workers in the DAG config
```

#### ❌ BigQuery write fails from Dataproc
```bash
# Ensure spark-bigquery jar is available and temp bucket is set
# In the PySpark job, verify:
# .config("temporaryGcsBucket", bucket)
# The service account must have bigquery.admin on the project
```

#### ❌ DAG not appearing in Airflow UI
```bash
# Check for Python syntax errors in the DAG
gcloud composer environments run $COMPOSER_ENV \
  --location=$REGION \
  --project=$PROJECT_ID \
  dags list

# If DAG is missing, check Airflow logs:
# Airflow UI → Browse → Logs → check scheduler logs
```

#### ❌ Bronze layer finds no CSV files
```bash
# Verify extract.py ran successfully — check raw/ bucket
gsutil ls gs://$PIPELINE_BUCKET/raw/

# If empty, Cloud Run extraction failed — check Cloud Run logs:
gcloud run services logs read $SERVICE_NAME \
  --region=$REGION \
  --project=$PROJECT_ID \
  --limit=50
```

#### ❌ Cloud Build fails on Docker push
```bash
# Re-authenticate Docker with Artifact Registry
gcloud auth configure-docker ${REGION}-docker.pkg.dev --quiet

# Check if the repository exists
gcloud artifacts repositories list --location=$REGION --project=$PROJECT_ID
```

---

### View Logs

```bash
# Cloud Run logs
gcloud run services logs read $SERVICE_NAME \
  --region=$REGION --project=$PROJECT_ID --limit=100

# Cloud Build logs (last build)
gcloud builds list --project=$PROJECT_ID --limit=5

# Dataproc job logs (from GCS)
gsutil cat gs://$PIPELINE_BUCKET/logs/dataproc/*.log 2>/dev/null || \
  echo "Check Dataproc UI for job logs"
```

---

## ✅ Deployment Checklist

Use this to track your progress:

```
STEP 1 — Prerequisites
  [ ] gcloud CLI installed and authenticated
  [ ] Docker installed
  [ ] Two GCP projects created: dv-env and prod-env
  [ ] Billing enabled on both projects

STEP 2 — Per Environment (repeat for DV and PD)
  [ ] GCP APIs enabled (10 APIs)
  [ ] Service account created: mb-pipeline-sa
  [ ] 10 IAM roles granted to service account
  [ ] Source GCS bucket created
  [ ] Pipeline GCS bucket created (with 5 folder prefixes)
  [ ] Artifact Registry repository created
  [ ] Docker image built and pushed
  [ ] Cloud Run deployed and health check passes
  [ ] BigQuery dataset created
  [ ] 4 BigQuery tables created
  [ ] Cloud Composer created (20–30 min)
  [ ] DAG uploaded to Composer
  [ ] PySpark scripts uploaded
  [ ] Airflow variable MB_ENV set
  [ ] Airflow variable MB_CLOUD_RUN_URL set
  [ ] Airflow connection mb_{env}_cloud_run_conn created
  [ ] DAG is visible in Airflow UI and toggled ON

STEP 3 — CI/CD (once per GitHub repo)
  [ ] GitHub repo connected to Cloud Build
  [ ] Cloud Build triggers created (dv + pd)
  [ ] Cloud Build service account granted roles

STEP 4 — End-to-End Test
  [ ] Sample Excel file uploaded to source bucket
  [ ] DAG triggered manually
  [ ] All 8 tasks completed successfully
  [ ] CSV files visible in raw/ bucket
  [ ] Parquet files visible in bronze/, silver/, gold/
  [ ] 4 BigQuery tables have rows
  [ ] CI/CD test push triggers Cloud Build
```

---

## 🔁 Repeat for Second Environment

After completing all steps for DV (`ENV=dv`, `PROJECT_ID=dv-env`), repeat from **Step 2** with:

```bash
export ENV=pd
export PROJECT_ID=prod-env
export REGION=us-central1
```

---

## 💡 Quick Reference — Key Values

| Item | DV | PD |
|------|----|----|
| Project ID | `dv-env` | `prod-env` |
| Source Bucket | `dv-mobile-brands` | `mobile-brands` |
| Pipeline Bucket | `dv-mb-pipeline-bucket` | `pd-mb-pipeline-bucket` |
| Cloud Run Service | `dv-mb-extractor` | `pd-mb-extractor` |
| Composer Env | `dv-mb-composer` | `pd-mb-composer` |
| Dataproc Cluster | `dv-mb-dataproc` | `pd-mb-dataproc` |
| DAG ID | `dv_mobile_brands_pipeline` | `pd_mobile_brands_pipeline` |
| Airflow Conn ID | `mb_dv_cloud_run_conn` | `mb_pd_cloud_run_conn` |
| Docker Machine | `n1-standard-2` | `n1-standard-4` |
| Dataproc Workers | 2 | 3 |
