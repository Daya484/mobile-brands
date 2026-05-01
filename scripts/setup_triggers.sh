#!/usr/bin/env bash
# ==============================================================================
# setup_triggers.sh — Create Cloud Build Triggers for DV and PD environments
# ==============================================================================
#
# This script creates TWO Cloud Build triggers in your repo:
#
#   Trigger 1 (DV):  feature/* or develop branches  →  dv-env project
#   Trigger 2 (PD):  main branch                    →  prod-env project
#
# Both triggers use the SAME cloudbuild.yaml but with different substitutions.
# Change detection ensures only affected components are rebuilt per commit.
#
# Prerequisites:
#   - gcloud CLI authenticated
#   - GitHub repo connected to Cloud Build (manually in Console first time)
#   - Cloud Build API enabled in BOTH projects (dv-env and prod-env)
#
# Usage:
#   export GITHUB_OWNER="your-github-username"
#   export GITHUB_REPO="your-repo-name"
#   bash scripts/setup_triggers.sh
# ==============================================================================

set -euo pipefail

# ── REQUIRED — set these before running ──────────────────────────────────────
GITHUB_OWNER="${GITHUB_OWNER:-YOUR_GITHUB_USERNAME}"
GITHUB_REPO="${GITHUB_REPO:-YOUR_REPO_NAME}"
REGION="${REGION:-us-central1}"

# ── DV PROJECT SETTINGS ───────────────────────────────────────────────────────
DV_PROJECT="dv-env"
DV_BUCKET="dv-mb-pipeline-bucket"
DV_SOURCE_BUCKET="dv-mobile-brands"
DV_SERVICE="dv-mb-extractor"
DV_SA="mb-pipeline-sa@dv-env.iam.gserviceaccount.com"
DV_AR="${REGION}-docker.pkg.dev/${DV_PROJECT}/mb-repo"

# ── PD PROJECT SETTINGS ───────────────────────────────────────────────────────
PD_PROJECT="prod-env"
PD_BUCKET="pd-mb-pipeline-bucket"
PD_SOURCE_BUCKET="mobile-brands"
PD_SERVICE="pd-mb-extractor"
PD_SA="mb-pipeline-sa@prod-env.iam.gserviceaccount.com"
PD_AR="${REGION}-docker.pkg.dev/${PD_PROJECT}/mb-repo"

echo "======================================================================"
echo "  Setting up Cloud Build Triggers for Mobile Brands Pipeline"
echo "  GitHub: ${GITHUB_OWNER}/${GITHUB_REPO}"
echo "======================================================================"

# ─────────────────────────────────────────────────────────────────────────────
# HELPER: Get Composer bucket for a given env
# ─────────────────────────────────────────────────────────────────────────────
get_composer_bucket() {
  local project=$1
  local composer_env=$2
  local loc=$3
  gcloud composer environments describe "${composer_env}" \
    --location="${loc}" \
    --project="${project}" \
    --format="value(config.dagGcsPrefix)" 2>/dev/null | sed 's|/dags||' || echo "FILL_IN_MANUALLY"
}

echo ""
echo "▶ Fetching Composer bucket names..."
DV_COMPOSER_BUCKET=$(get_composer_bucket "${DV_PROJECT}" "dv-mb-composer" "${REGION}")
PD_COMPOSER_BUCKET=$(get_composer_bucket "${PD_PROJECT}" "pd-mb-composer" "${REGION}")
echo "  DV Composer bucket: ${DV_COMPOSER_BUCKET}"
echo "  PD Composer bucket: ${PD_COMPOSER_BUCKET}"


# ─────────────────────────────────────────────────────────────────────────────
# TRIGGER 1: FEATURE BRANCHES → DV
# Branch pattern: feature/.* or develop
# Project: dv-env
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "▶ Creating DV trigger (feature/* and develop → dv-env)..."

gcloud builds triggers create github \
  --name="mb-pipeline-dv-trigger" \
  --description="[DV] Mobile Brands: feature/develop branches → dv-env (auto change detection)" \
  --repo-owner="${GITHUB_OWNER}" \
  --repo-name="${GITHUB_REPO}" \
  --branch-pattern="^(feature/.*|develop)$" \
  --build-config="mobile-brands/cloudbuild.yaml" \
  --project="${DV_PROJECT}" \
  --region="${REGION}" \
  --substitutions=\
"_ENV=dv,"\
"_PROJECT_ID=${DV_PROJECT},"\
"_REGION=${REGION},"\
"_PIPELINE_BUCKET=${DV_BUCKET},"\
"_SOURCE_BUCKET=${DV_SOURCE_BUCKET},"\
"_COMPOSER_BUCKET=${DV_COMPOSER_BUCKET},"\
"_ARTIFACT_REGISTRY=${DV_AR},"\
"_CLOUD_RUN_SERVICE=${DV_SERVICE},"\
"_SERVICE_ACCOUNT=${DV_SA}" \
  2>/dev/null || echo "  ⚠️  DV trigger may already exist — update substitutions manually if needed."

echo "✅ DV trigger created."


# ─────────────────────────────────────────────────────────────────────────────
# TRIGGER 2: MAIN BRANCH → PD
# Branch pattern: main (exact)
# Project: prod-env
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "▶ Creating PD trigger (main → prod-env)..."

gcloud builds triggers create github \
  --name="mb-pipeline-pd-trigger" \
  --description="[PD] Mobile Brands: main branch → prod-env (auto change detection)" \
  --repo-owner="${GITHUB_OWNER}" \
  --repo-name="${GITHUB_REPO}" \
  --branch-pattern="^main$" \
  --build-config="mobile-brands/cloudbuild.yaml" \
  --project="${PD_PROJECT}" \
  --region="${REGION}" \
  --substitutions=\
"_ENV=pd,"\
"_PROJECT_ID=${PD_PROJECT},"\
"_REGION=${REGION},"\
"_PIPELINE_BUCKET=${PD_BUCKET},"\
"_SOURCE_BUCKET=${PD_SOURCE_BUCKET},"\
"_COMPOSER_BUCKET=${PD_COMPOSER_BUCKET},"\
"_ARTIFACT_REGISTRY=${PD_AR},"\
"_CLOUD_RUN_SERVICE=${PD_SERVICE},"\
"_SERVICE_ACCOUNT=${PD_SA}" \
  2>/dev/null || echo "  ⚠️  PD trigger may already exist — update substitutions manually if needed."

echo "✅ PD trigger created."


# ─────────────────────────────────────────────────────────────────────────────
# SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "======================================================================"
echo "  Cloud Build Triggers Setup Complete!"
echo "======================================================================"
echo ""
echo "  TRIGGER 1 — DV (Development)"
echo "    Project:  ${DV_PROJECT}"
echo "    Branches: feature/* or develop"
echo "    Builds:   Only changed components (Cloud Run / Dataproc / DAG)"
echo ""
echo "  TRIGGER 2 — PD (Production)"
echo "    Project:  ${PD_PROJECT}"
echo "    Branch:   main"
echo "    Builds:   Only changed components (Cloud Run / Dataproc / DAG)"
echo ""
echo "  Git Workflow:"
echo "    git checkout -b feature/my-change"
echo "    # make code changes"
echo "    git push origin feature/my-change  →  DV build auto-triggers"
echo ""
echo "    git checkout main && git merge feature/my-change"
echo "    git push origin main               →  PD build auto-triggers"
echo "======================================================================"
