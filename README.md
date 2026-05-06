# 📱 Mobile Brands Sales Data Pipeline — GCP Batch Processing

> A production-grade, daily batch data pipeline built on Google Cloud Platform that ingests mobile brand sales data from multiple country sources, processes it through a **Bronze → Silver → Gold medallion architecture** using PySpark on Dataproc, and delivers clean KPI analytics to BigQuery — every morning at 6:30 AM IST.

---

## 🎯 Project Idea

Mobile brand distributors (Samsung, Apple, Oppo, Vivo, OnePlus) operate across **6 countries** (India, America, Australia, China, South Africa, South Korea). Each distributor submits their **daily sales Excel files** to a central GCS bucket.

The goal of this pipeline is to:
- **Automatically collect** all daily sales files from 6 country sources
- **Split and organise** data by brand and region
- **Clean, validate and transform** the raw data through medallion layers
- **Produce business KPIs** such as market share, revenue rankings, top brands per region
- **Make fresh data available** in BigQuery every morning before business hours

---

## 🏗️ System Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                    MOBILE BRANDS GCP DATA PIPELINE                               │
│                                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │                    SOURCE DATA (Daily Excel Files)                       │    │
│  │   gs://mobile-brands/                                                   │    │
│  │   ├── INDIA/        sales_May01.xlsx  (Samsung, Apple, Oppo, Vivo, OnePlus) │
│  │   ├── AMERICA/      sales_May01.xlsx                                    │    │
│  │   ├── AUSTRALIA/    sales_May01.xlsx                                    │    │
│  │   ├── CHINA/        sales_May01.xlsx                                    │    │
│  │   ├── SOUTH AFRICA/ sales_May01.xlsx                                    │    │
│  │   └── SOUTH KOREA/  sales_May01.xlsx                                    │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
│                              │  (files sit in GCS)                               │
│                              │                                                   │
│  ┌───────────────────────────▼─────────────────────────────────────────────┐    │
│  │              CLOUD COMPOSER (Airflow) — Fires at 6:30 AM IST            │    │
│  │                  schedule_interval = "0 1 * * *"                        │    │
│  │                                                                         │    │
│  │  Task 1: trigger_cloud_run_extraction                                   │    │
│  │  Task 2: upload_pyspark_scripts          (parallel with Task 1)        │    │
│  │  Task 3: create_dataproc_cluster                                        │    │
│  │  Task 4: bronze_layer  ──▶  Task 5: silver_layer  ──▶  Task 6: gold_layer│   │
│  │  Task 7: delete_dataproc_cluster (always, even on failure)              │    │
│  │  Task 8: data_quality_check                                             │    │
│  └───────────────────────────────────────────────────────────────────────-─┘    │
│        │               │                      │                                  │
│        ▼               ▼                      ▼                                  │
│  ┌──────────┐   ┌────────────┐      ┌──────────────────────────────────┐        │
│  │ Cloud Run│   │  GCS       │      │   Dataproc Cluster (Ephemeral)   │        │
│  │ Extractor│──▶│  raw/      │─────▶│   1 Master + 2/3 Workers         │        │
│  │ FastAPI  │   │  bronze/   │      │                                  │        │
│  │ /extract │   │  silver/   │      │  ┌──────────────────────────┐   │        │
│  └──────────┘   │  gold/     │      │  │ Bronze Job (bronze_job.py)│   │        │
│        │        │  scripts/  │      │  │ raw Excel → Parquet       │   │        │
│        │        └────────────┘      │  │ + metadata columns        │   │        │
│        │                            │  └────────────┬─────────────┘   │        │
│  Splits Excel                       │               ▼                  │        │
│  by brand sheets                    │  ┌──────────────────────────┐   │        │
│  in parallel                        │  │ Silver Job (silver_job.py)│   │        │
│  (6 regions, 5 brands)              │  │ Type cast + deduplicate   │   │        │
│                                     │  │ + normalize + validate    │   │        │
│                                     │  └────────────┬─────────────┘   │        │
│                                     │               ▼                  │        │
│                                     │  ┌──────────────────────────┐   │        │
│                                     │  │ Gold Job (gold_job.py)   │   │        │
│                                     │  │ KPI aggregations:         │   │        │
│                                     │  │ • Brand market share      │   │        │
│                                     │  │ • Region summary          │   │        │
│                                     │  │ • Daily trends (7d avg)   │   │        │
│                                     │  └────────────┬─────────────┘   │        │
│                                     └───────────────┼──────────────────┘        │
│                                                      │                           │
│                                                      ▼                           │
│                                     ┌────────────────────────────┐              │
│                                     │         BigQuery            │              │
│                                     │   mobile_brands dataset     │              │
│                                     │                             │              │
│                                     │  silver_sales               │              │
│                                     │  gold_brand_kpi             │              │
│                                     │  gold_region_summary        │              │
│                                     │  gold_daily_trend           │              │
│                                     └────────────────────────────┘              │
│                                                                                  │
│  ┌──────────────────────────────────────────────────────────────────────────┐   │
│  │  CLOUD BUILD CI/CD  (triggers on every GitHub push)                      │   │
│  │                                                                          │   │
│  │  feature/* or develop ──▶ DV Build (dv-env project)                     │   │
│  │  main               ──▶ PD Build (prod-env project)                     │   │
│  │                                                                          │   │
│  │  Smart change detection — only rebuilds what changed:                   │   │
│  │  • cloud-run/** changed  → Docker build → Artifact Registry → Cloud Run │   │
│  │  • dataproc/**  changed  → Upload PySpark scripts to GCS                │   │
│  │  • dags/**      changed  → Sync DAG to Composer bucket                  │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 GCP Services Used

| Service | Role | Why Used |
|---------|------|----------|
| **Cloud Storage (GCS)** | Source files + inter-layer storage | Scalable, cheap storage for Excel & Parquet files |
| **Cloud Run** | Extraction microservice | Serverless, auto-scales, runs only when called |
| **Cloud Composer** | Pipeline orchestration (Airflow) | Schedules and coordinates all tasks reliably |
| **Dataproc** | PySpark processing (Bronze/Silver/Gold) | Distributed processing for large-scale data |
| **BigQuery** | Analytics data warehouse | SQL-queryable KPI tables for dashboards |
| **Cloud Build** | CI/CD automation | Auto-deploys code changes from GitHub to GCP |
| **Artifact Registry** | Docker image storage | Stores versioned Cloud Run container images |
| **IAM** | Security & access control | Service accounts with least-privilege roles |

---

## 🗂️ Medallion Architecture (Bronze → Silver → Gold)

```
RAW (Excel source)   →  RAW/CSV (landing)  →  BRONZE (Parquet)     →  SILVER (Parquet)      →  GOLD (BigQuery)
──────────────────────────────────────────────────────────────────────────────────────────────────────────────
Original .xlsx           Cloud Run splits       No transformation         Type casting              KPI aggregations
files from               Excel by brand,        Only adds metadata:       Null filtering            per brand/region
distributors             saves each sheet       - source_file             Deduplication             Market share %
(INDIA, AMERICA,         as CSV to raw/         - ingestion_time          Brand normalization        Revenue ranking
 CHINA, etc.)            No openpyxl            - pipeline_layer          Revenue recompute         Daily trends
                         needed on cluster      - region, brand           Date parsing              7-day rolling avg
```

### GCS Bucket Layout

```
gs://mb-pipeline-bucket/
├── raw/                          ← Cloud Run writes CSV here (split by brand)
│   ├── INDIA/
│   │   ├── samsung/  sales_May01.csv    ← Samsung sheet extracted → CSV
│   │   ├── apple/    sales_May01.csv    ← Apple sheet extracted → CSV
│   │   ├── oppo/     sales_May01.csv
│   │   ├── vivo/     sales_May01.csv
│   │   └── oneplus/  sales_May01.csv
│   ├── AMERICA/
│   │   └── samsung/  sales_May01.csv   ... etc.
│   └── (AUSTRALIA, CHINA, SOUTH AFRICA, SOUTH KOREA)/
│
├── bronze/                       ← Bronze PySpark reads CSV → writes Parquet
│   └── year=2024/month=5/day=1/
│       ├── region=INDIA/brand=samsung/  *.parquet
│       └── ...
│
├── silver/                       ← Silver PySpark reads Bronze → writes Parquet
│   └── year=2024/month=5/day=1/
│       └── brand=Samsung/region=INDIA/  *.parquet
│
├── gold/                         ← Gold PySpark reads Silver → writes Parquet
│   ├── brand_kpi/
│   ├── region_summary/
│   └── daily_trend/
│
└── scripts/                      ← PySpark scripts (synced by Cloud Build)
    ├── bronze_job.py
    ├── silver_job.py
    └── gold_job.py
```

---

## 📊 BigQuery Tables

### `silver_sales`
Clean, validated, deduplicated sales records.

| Column | Type | Description |
|--------|------|-------------|
| `sale_date` | DATE | Date of sale |
| `brand` | STRING | Samsung / Apple / Oppo / Vivo / OnePlus |
| `region` | STRING | INDIA / AMERICA / CHINA / AUSTRALIA / SOUTH AFRICA / SOUTH KOREA |
| `model` | STRING | Device model name |
| `units_sold` | INTEGER | Number of units sold |
| `price_usd` | FLOAT64 | Price in USD |
| `revenue_usd` | FLOAT64 | Total revenue (units × price) |
| `ingestion_time` | TIMESTAMP | When file was first ingested |
| `processed_time` | TIMESTAMP | When Silver job ran |

### `gold_brand_kpi`
Monthly KPIs per brand and region.

| Column | Type | Description |
|--------|------|-------------|
| `brand` | STRING | Brand name |
| `region` | STRING | Country/region |
| `year` / `month` | INTEGER | Time period |
| `total_units` | INTEGER | Total units sold |
| `total_revenue` | FLOAT64 | Total revenue in USD |
| `market_share_pct` | FLOAT64 | % of region's total units |
| `revenue_rank` | INTEGER | Rank by revenue within region+month |

### `gold_region_summary`
Monthly market overview per region.

| Column | Type | Description |
|--------|------|-------------|
| `region` | STRING | Country/region |
| `total_market_units` | INTEGER | Total units across all brands |
| `total_market_revenue` | FLOAT64 | Total market revenue |
| `top_brand` | STRING | Best-selling brand |
| `top_brand_share_pct` | FLOAT64 | Top brand's market share % |
| `active_brands` | INTEGER | Number of brands with sales |

### `gold_daily_trend`
Daily granularity for time-series analytics.

| Column | Type | Description |
|--------|------|-------------|
| `brand` | STRING | Brand name |
| `sale_date` | DATE | Specific day |
| `daily_units` | INTEGER | Units sold that day |
| `daily_revenue` | FLOAT64 | Revenue that day |
| `rolling_7d_avg_units` | FLOAT64 | 7-day rolling average |

---

## 🔄 Daily Pipeline Flow

```
[Night / Before 6:30 AM IST]
────────────────────────────────────────────────────────────────
  Distributors / Python script uploads Excel files to GCS:
  gs://mobile-brands/INDIA/sales_May01.xlsx
  gs://mobile-brands/AMERICA/sales_May01.xlsx
  gs://mobile-brands/CHINA/sales_May01.xlsx    ... etc.

[6:30 AM IST — Composer DAG triggers automatically]
────────────────────────────────────────────────────────────────
  TASK 1 + 2 (parallel):
    ├── Cloud Run POST /extract
    │     → reads all 6 regions in parallel threads
    │     → splits each Excel file into 5 brand sheets
    │     → converts each sheet to CSV format
    │     → writes to gs://mb-pipeline-bucket/raw/{REGION}/{brand}/*.csv
    │     → duration: ~2–5 min
    │
    └── Upload PySpark scripts to GCS (from Composer dags/ folder)

  TASK 3: Create Dataproc Cluster
    → 1 master + 2 workers (dev) / 3 workers (prod)
    → ephemeral: spins up only for this run
    → duration: ~3–5 min

  TASK 4: Bronze Layer (bronze_job.py on Dataproc)
    → reads raw/ CSV files via spark.read.csv() — native Spark, no openpyxl needed
    → 30 combinations processed: 6 regions × 5 brands
    → adds metadata: source_file, ingestion_time, region, brand, pipeline_layer
    → writes Parquet to gs://mb-pipeline-bucket/bronze/year=YYYY/month=MM/day=DD/
    → duration: ~5–10 min

  TASK 5: Silver Layer (silver_job.py on Dataproc)
    → reads Bronze Parquet
    → type casts: strings → dates, integers, floats
    → normalises: "samsung electronics" → "Samsung"
    → deduplicates: keeps latest record per (date, brand, region, model)
    → filters: removes nulls, negatives, invalid records
    → writes Parquet to gs://mb-pipeline-bucket/silver/
    → writes to BigQuery: mobile_brands.silver_sales
    → duration: ~5–10 min

  TASK 6: Gold Layer (gold_job.py on Dataproc)
    → reads Silver Parquet (cached in memory)
    → computes 3 KPI aggregations:
        • gold_brand_kpi:      market share, revenue rank per brand/region/month
        • gold_region_summary: top brand, market size per region/month
        • gold_daily_trend:    daily units + 7-day rolling average
    → writes Parquet to gs://mb-pipeline-bucket/gold/
    → writes 3 tables to BigQuery
    → duration: ~5–10 min

  TASK 7: Delete Dataproc Cluster (always runs)
    → triggered even if previous tasks failed (ALL_DONE rule)
    → prevents cost from idle clusters

  TASK 8: Data Quality Check
    → queries row counts from all 4 BigQuery tables
    → raises alert if any table is empty

[~7:00–7:30 AM IST]
────────────────────────────────────────────────────────────────
  ✅ Fresh data available in BigQuery
  ✅ Dashboards/reports show today's brand KPIs
  ✅ Total pipeline duration: ~20–30 minutes
```

---

## 🌿 Multi-Environment Setup (DV & PD)

Two completely isolated GCP projects — development and production.

| Property | Development (dv) | Production (pd) |
|----------|------------------|-----------------|
| **GCP Project** | `dv-env` | `prod-env` |
| **GCS Bucket** | `dv-mb-pipeline-bucket` | `pd-mb-pipeline-bucket` |
| **Source Bucket** | `dv-mobile-brands` | `mobile-brands` |
| **Cloud Run** | `dv-mb-extractor` | `pd-mb-extractor` |
| **Dataproc** | `dv-mb-dataproc` (n1-standard-2) | `pd-mb-dataproc` (n1-standard-4) |
| **Composer** | `dv-mb-composer` | `pd-mb-composer` |
| **DAG ID** | `dv_mobile_brands_pipeline` | `pd_mobile_brands_pipeline` |
| **Retries** | 1 | 2 |

### Git Branch → Environment Mapping

```
GitHub
├── feature/my-change  ──▶  Cloud Build  ──▶  dv-env (test safely)
├── develop            ──▶  Cloud Build  ──▶  dv-env
└── main               ──▶  Cloud Build  ──▶  prod-env (live)
```

### CI/CD — Smart Change Detection

Every GitHub push triggers Cloud Build, but **only rebuilds what changed**:

| Files Changed | What Rebuilds |
|---------------|---------------|
| `cloud-run/**` | Docker image → Artifact Registry → Cloud Run deploy |
| `dataproc/**` | PySpark scripts → uploaded to GCS + Composer |
| `dags/**` | DAG → synced to Composer bucket |
| `config/**` | Triggers all three above |
| Unrelated files | Nothing — no rebuild |

---

## 📁 Project Structure

```
mobile-brands/
│
├── extract.py                    # Cloud Run extraction: Excel → CSV → raw/ (parallel, 6 regions)
├── transformed.py                # Original standalone script (CSV output, not part of pipeline)
│
├── cloud-run/
│   ├── Dockerfile                # Multi-stage Docker build (Python 3.11-slim)
│   ├── main.py                   # FastAPI service — POST /extract endpoint
│   ├── gcs_trigger.py            # (optional) event-driven GCS → Pub/Sub trigger
│   └── requirements.txt          # fastapi, uvicorn, pandas, openpyxl, httpx
│
├── dataproc/
│   ├── bronze_job.py             # PySpark: raw Excel → Bronze Parquet + metadata
│   ├── silver_job.py             # PySpark: Bronze → Silver Parquet + BigQuery
│   └── gold_job.py               # PySpark: Silver → 3 Gold KPI tables + BigQuery
│
├── dags/
│   └── mobile_brands_dag.py      # Airflow DAG: 8 tasks, schedule = 6:30 AM IST
│
├── config/
│   └── env_config.py             # Single source of truth: dv/pd env settings
│
├── scripts/
│   ├── setup_bigquery.py         # Creates BQ dataset + 4 tables (run once)
│   ├── deploy.sh                 # Bootstrap: provisions all GCP resources
│   ├── setup_triggers.sh         # Creates Cloud Build triggers for dv + pd
│   └── setup_gcs_events.sh       # (optional) GCS event → Pub/Sub → Cloud Run
│
└── cloudbuild.yaml               # CI/CD: test → build → push → deploy → sync
```

---

## ⚙️ Deployment Guide

### Prerequisites
```bash
# Authenticate with GCP
gcloud auth login
gcloud auth application-default login
```

### Step 1 — Deploy Dev Environment
```bash
cd k:/antigravity/mobile-brands
ENV=dv bash scripts/deploy.sh
```

### Step 2 — Deploy Prod Environment
```bash
ENV=pd bash scripts/deploy.sh
```

This provisions (per environment):
- ✅ Enables all required GCP APIs
- ✅ Creates Service Account with IAM roles
- ✅ Creates GCS buckets + folder prefixes
- ✅ Creates Artifact Registry Docker repository
- ✅ Creates BigQuery dataset + 4 tables
- ✅ Builds and deploys Cloud Run extractor
- ✅ Creates Cloud Composer environment
- ✅ Uploads DAG + PySpark scripts to Composer
- ✅ Sets Airflow Variables (MB_ENV, MB_CLOUD_RUN_URL)
- ✅ Creates Airflow Connection for Cloud Run

### Step 3 — Connect GitHub to Cloud Build
In GCP Console → Cloud Build → Triggers → Connect Repository *(manual, once per project)*

### Step 4 — Create Cloud Build Triggers
```bash
export GITHUB_OWNER="your-github-username"
export GITHUB_REPO="your-repo-name"
bash scripts/setup_triggers.sh
```

### Step 5 — Test End-to-End
```bash
# Upload a sample Excel file
gsutil cp sample_sales.xlsx gs://mobile-brands/INDIA/sales_test.xlsx

# Manually trigger DAG in Composer UI
# OR via CLI:
gcloud composer environments run dv-mb-composer \
  --location=us-central1 \
  dags trigger -- dv_mobile_brands_pipeline
```

---

## 🔐 IAM Roles Required

| Role | Purpose |
|------|---------|
| `roles/storage.admin` | Read/write GCS buckets |
| `roles/bigquery.admin` | Create and write BigQuery tables |
| `roles/dataproc.editor` | Create/delete Dataproc clusters |
| `roles/run.invoker` | Allow Composer to call Cloud Run |
| `roles/composer.worker` | Composer service account permissions |
| `roles/artifactregistry.writer` | Push Docker images |

---

## 🔄 Airflow Variables (set in each Composer environment)

| Variable | DV Value | PD Value |
|----------|----------|----------|
| `MB_ENV` | `dv` | `pd` |
| `MB_CLOUD_RUN_URL` | `https://dv-mb-extractor-xxx.run.app` | `https://pd-mb-extractor-xxx.run.app` |

---

## 📅 Schedule Reference

| Cron Expression | UTC Time | IST Time |
|-----------------|----------|----------|
| `"0 1 * * *"` | 01:00 UTC | **06:30 IST** ← current |
| `"30 18 * * *"` | 18:30 UTC | 00:00 IST (midnight) |
| `"0 2 * * *"` | 02:00 UTC | 07:30 IST |
| `"0 3 * * *"` | 03:00 UTC | 08:30 IST |

---

## 🛠️ Tech Stack

| Technology | Version | Use |
|------------|---------|-----|
| Python | 3.11 | Extract + Transform logic |
| Apache Spark / PySpark | 3.x | Distributed data processing |
| Apache Airflow | 2.x | DAG orchestration |
| FastAPI | 0.111 | Cloud Run REST API |
| pandas + openpyxl | latest | Excel file reading |
| Dataproc | 2.1 (Debian 11) | Managed Spark cluster |
| Cloud Composer | 2.x | Managed Airflow |
| Docker | 3.11-slim | Cloud Run container |

---

## 📌 Key Design Decisions

| Decision | Reason |
|----------|--------|
| **Ephemeral Dataproc cluster** | Created per DAG run, deleted after — saves cost vs always-on cluster |
| **Cloud Run for extraction** | Serverless — no server to manage, scales to zero when not running |
| **Parquet with Snappy compression** | ~75% smaller than CSV, columnar for fast analytics reads |
| **Bronze mode = overwrite per date** | Each daily run owns its date partition — no duplicate accumulation |
| **Silver mode = overwrite** | Ensures today's view is always fresh, not an accumulation |
| **Silver cached in Gold job** | Silver DataFrame reused 3 times — avoids re-reading from GCS 3x |
| **ALL_DONE on cluster delete** | Cluster is always deleted even if Spark jobs fail — prevents idle cost |
| **Separate DAG ID per env** | `dv_*` and `pd_*` DAGs coexist in same Composer — no confusion |

---

## 👤 Author

Built as a production-grade GCP data engineering portfolio project demonstrating:
- Multi-environment CI/CD with Cloud Build
- Medallion architecture with PySpark on Dataproc
- Serverless microservices with Cloud Run
- Managed orchestration with Cloud Composer (Airflow)
- BigQuery as the analytics serving layer

---

## 🚀 Quick Start — Deploy to GCP (Cloud Shell)

```bash
# 1. Clone & enter the repo
git clone https://github.com/Daya484/mobile-brands.git
cd mobile-brands

# 2. Make executable
chmod +x gcp_deployment.sh

# 3. Deploy Development environment
ENV=dv bash gcp_deployment.sh

# 4. Deploy Production environment
ENV=pd bash gcp_deployment.sh

# 5. Deploy BOTH environments
bash gcp_deployment.sh
```

> See [DEPLOYMENT.md](./DEPLOYMENT.md) for the full step-by-step guide.