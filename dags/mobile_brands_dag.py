"""
Cloud Composer (Airflow) DAG — Mobile Brands Batch Pipeline (Multi-Env)
=======================================================================
SCHEDULED BATCH MODE:
  Files are generated once a day by Python code → uploaded to GCS.
  At a fixed time every day, Composer triggers this DAG to process them.

Flow:
  [Your Python script] → generates Excel files → GCS source bucket
                                                          ↓ (waits)
  [Composer DAG fires at scheduled time]
     1. trigger_cloud_run_extraction  → Cloud Run reads Excel → splits brand sheets → raw/
     2. upload_pyspark_scripts        → copies latest Spark jobs to GCS
     3. create_dataproc_cluster       → ephemeral cluster spins up
     4. bronze_layer                  → raw Excel → Parquet + metadata
     5. silver_layer                  → clean + type + dedup → Parquet + BigQuery
     6. gold_layer                    → KPI aggregations → BigQuery
     7. delete_dataproc_cluster       → always runs (cost control)
     8. data_quality_check            → BigQuery row-count validation

Schedule: Change schedule_interval below to your preferred daily time.

Required Airflow Variables:
  MB_ENV           : "dv" or "pd"
  MB_CLOUD_RUN_URL : Cloud Run service URL
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.http import SimpleHttpOperator
from airflow.utils.trigger_rule import TriggerRule


# ─────────────────────────────────────────────────────────────────────────────
# ENVIRONMENT RESOLUTION
# ─────────────────────────────────────────────────────────────────────────────
# Read ENV from Airflow Variable; default to "dv" (safe fallback)
# Each Composer environment should have MB_ENV set to its own env name.
# ─────────────────────────────────────────────────────────────────────────────

_ENV_NAME = Variable.get("MB_ENV", default_var=os.getenv("MB_ENV", "dv"))

_ENV_CONFIGS = {
    "dv": {
        "project_id":        "dv-env",
        "bucket":            "dv-mb-pipeline-bucket",
        "source_bucket":     "dv-mobile-brands",
        "bq_dataset":        "mobile_brands",
        "region":            "us-central1",
        "cloud_run_service":  "dv-mb-extractor",
        "dataproc_cluster":  "dv-mb-dataproc",
        "machine_type":      "n1-standard-2",
        "num_workers":       2,
    },
    "pd": {
        "project_id":        "pd-env-495516",
        "bucket":            "pd-mb-pipeline-bucket",
        "source_bucket":     "mobile-brands",
        "bq_dataset":        "mobile_brands",
        "region":            "us-central1",
        "cloud_run_service":  "pd-mb-extractor",
        "dataproc_cluster":  "pd-mb-dataproc",
        "machine_type":      "n1-standard-4",
        "num_workers":       3,
    },
}

if _ENV_NAME not in _ENV_CONFIGS:
    raise ValueError(f"Unknown MB_ENV='{_ENV_NAME}'. Must be 'dv' or 'pd'.")

_C = _ENV_CONFIGS[_ENV_NAME]   # shortcut

PROJECT_ID     = _C["project_id"]
GCS_BUCKET     = _C["bucket"]
SOURCE_BUCKET  = _C["source_bucket"]
GCP_REGION     = _C["region"]
BQ_DATASET     = _C["bq_dataset"]
CLUSTER_NAME   = _C["dataproc_cluster"]
SCRIPTS_GCS    = f"gs://{GCS_BUCKET}/scripts"

# Cloud Run URL — read from Variable (set by deploy script after Cloud Run deploy)
CLOUD_RUN_URL  = Variable.get(
    "MB_CLOUD_RUN_URL",
    default_var=os.getenv("MB_CLOUD_RUN_URL", f"https://{_C['cloud_run_service']}.run.app"),
)

# BigQuery tables
BQ_SILVER_TABLE      = f"{PROJECT_ID}.{BQ_DATASET}.silver_sales"
BQ_BRAND_KPI_TABLE   = f"{PROJECT_ID}.{BQ_DATASET}.gold_brand_kpi"
BQ_REGION_SUM_TABLE  = f"{PROJECT_ID}.{BQ_DATASET}.gold_region_summary"
BQ_DAILY_TREND_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.gold_daily_trend"

SPARK_BQ_JAR = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.36.1.jar"


# ─────────────────────────────────────────────────────────────────────────────
# DATAPROC CLUSTER CONFIG (env-aware sizing)
# ─────────────────────────────────────────────────────────────────────────────

DATAPROC_CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": _C["machine_type"],
        "disk_config": {
            "boot_disk_type":    "pd-ssd",
            "boot_disk_size_gb": 100,
        },
    },
    "worker_config": {
        "num_instances":   _C["num_workers"],
        "machine_type_uri": _C["machine_type"],
        "disk_config": {
            "boot_disk_type":    "pd-ssd",
            "boot_disk_size_gb": 100,
        },
    },
    "software_config": {
        "image_version":       "2.1-debian11",
        "optional_components": ["JUPYTER"],
        "properties": {
            "spark:spark.jars": SPARK_BQ_JAR,
            "spark:spark.sql.adaptive.enabled": "true",
            "spark:spark.sql.adaptive.coalescePartitions.enabled": "true",
        },
    },
    "gce_cluster_config": {
        "service_account_scopes": ["https://www.googleapis.com/auth/cloud-platform"],
    },
    # Auto-delete cluster after 30 min idle (failsafe cost protection)
    "lifecycle_config": {
        "idle_delete_ttl": {"seconds": 1800},
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# DEFAULT ARGS
# ─────────────────────────────────────────────────────────────────────────────

default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2 if _ENV_NAME == "pd" else 1,   # fewer retries in dev
    "retry_delay":      timedelta(minutes=5),
    "start_date":       datetime(2024, 1, 1),
}


# ─────────────────────────────────────────────────────────────────────────────
# PYTHON CALLABLES
# ─────────────────────────────────────────────────────────────────────────────

def check_data_quality(**context):
    """
    Validates BigQuery table row counts after pipeline run.
    Raises ValueError if any mandatory table is empty.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT_ID)

    checks = [
        (BQ_SILVER_TABLE,      "Silver Sales"),
        (BQ_BRAND_KPI_TABLE,   "Gold Brand KPI"),
        (BQ_REGION_SUM_TABLE,  "Gold Region Summary"),
        (BQ_DAILY_TREND_TABLE, "Gold Daily Trend"),
    ]

    failures = []
    for table, label in checks:
        try:
            result = list(client.query(f"SELECT COUNT(*) FROM `{table}`").result())
            count  = result[0][0]
            print(f"[{_ENV_NAME.upper()}] ✅ {label} → {count:,} rows")
            if count == 0:
                failures.append(f"{label} ({table}) has 0 rows!")
        except Exception as exc:
            failures.append(f"{label}: {exc}")
            print(f"[{_ENV_NAME.upper()}] ❌ {label}: {exc}")

    if failures:
        raise ValueError(f"[{_ENV_NAME.upper()}] Data quality FAILED:\n" + "\n".join(failures))

    print(f"[{_ENV_NAME.upper()}] ✅ All data quality checks passed.")


# ─────────────────────────────────────────────────────────────────────────────
# DAG — one DAG per env (dv_mobile_brands_pipeline / pd_mobile_brands_pipeline)
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id=f"{_ENV_NAME}_mobile_brands_pipeline",
    default_args=default_args,
    description=(
        f"[{_ENV_NAME.upper()}] Mobile Brands Batch Pipeline: "
        f"Cloud Run → Dataproc Bronze/Silver/Gold → BigQuery | project={PROJECT_ID}"
    ),
    # ─────────────────────────────────────────────────────────────────────
    # CHANGE THIS to your preferred daily processing time (UTC).
    # Your Python script generates files → GCS, then this DAG runs at
    # the time below and processes everything in the bucket.
    #
    # Common options:
    #   "0 1 * * *"   → 01:00 UTC = 06:30 IST  (runs after midnight files)
    #   "0 2 * * *"   → 02:00 UTC = 07:30 IST
    #   "30 18 * * *" → 18:30 UTC = 00:00 IST  (midnight IST batch)
    #   "0 6 * * *"   → 06:00 UTC = 11:30 IST  (late morning IST)
    # ─────────────────────────────────────────────────────────────────────
    schedule_interval="0 1 * * *",   # 01:00 UTC = 06:30 IST daily
    catchup=False,
    max_active_runs=1,
    tags=["mobile-brands", "dataproc", "medallion", "batch", _ENV_NAME],
) as dag:

    # ── TASK 1: Trigger Cloud Run Extraction ─────────────────────────────────
    # Cloud Run reads ALL Excel files currently in the source GCS bucket,
    # splits brand sheets, and writes them to the raw/ landing zone.
    # This processes everything that was uploaded since the last run.
    trigger_extraction = SimpleHttpOperator(
        task_id="trigger_cloud_run_extraction",
        http_conn_id=f"mb_{_ENV_NAME}_cloud_run_conn",
        endpoint="/extract",
        method="POST",
        headers={"Content-Type": "application/json"},
        data=(
            f'{{"source_bucket": "{SOURCE_BUCKET}", '
            f'"dest_bucket": "{GCS_BUCKET}"}}'
        ),
        response_check=lambda response: response.json().get("status") in ("success", "empty"),
        log_response=True,
        extra_options={"timeout": 900},
    )

    # ── TASK 2: Upload PySpark scripts to GCS ────────────────────────────────
    # Cloud Build already placed the latest scripts in Composer dags/dataproc/.
    # This task copies them to the pipeline GCS bucket so Dataproc can access them.
    upload_scripts = BashOperator(
        task_id="upload_pyspark_scripts",
        bash_command=(
            f"gsutil -m cp "
            f"/home/airflow/gcs/dags/dataproc/bronze_job.py {SCRIPTS_GCS}/bronze_job.py && "
            f"gsutil -m cp "
            f"/home/airflow/gcs/dags/dataproc/silver_job.py {SCRIPTS_GCS}/silver_job.py && "
            f"gsutil -m cp "
            f"/home/airflow/gcs/dags/dataproc/gold_job.py   {SCRIPTS_GCS}/gold_job.py && "
            f"gsutil -m cp "
            f"/home/airflow/gcs/dags/config/env_config.py   {SCRIPTS_GCS}/env_config.py && "
            f"echo 'Scripts uploaded from Composer dags/ to {SCRIPTS_GCS}/'"
        ),
    )

    # ── TASK 3: Create Dataproc Cluster ─────────────────────────────────────
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=GCP_REGION,
        cluster_config=DATAPROC_CLUSTER_CONFIG,
    )

    # ── TASK 4: BRONZE Layer ─────────────────────────────────────────────────
    bronze_job = DataprocSubmitJobOperator(
        task_id="bronze_layer",
        project_id=PROJECT_ID,
        region=GCP_REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{SCRIPTS_GCS}/bronze_job.py",
                "args": [
                    f"--project_id={PROJECT_ID}",
                    f"--bucket={GCS_BUCKET}",
                    "--execution_date={{ ds }}",
                ],
                "jar_file_uris": [SPARK_BQ_JAR],
            },
        },
    )

    # ── TASK 5: SILVER Layer ─────────────────────────────────────────────────
    silver_job = DataprocSubmitJobOperator(
        task_id="silver_layer",
        project_id=PROJECT_ID,
        region=GCP_REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{SCRIPTS_GCS}/silver_job.py",
                "args": [
                    f"--project_id={PROJECT_ID}",
                    f"--bucket={GCS_BUCKET}",
                    f"--bq_dataset={BQ_DATASET}",
                ],
                "jar_file_uris": [SPARK_BQ_JAR],
            },
        },
    )

    # ── TASK 6: GOLD Layer ───────────────────────────────────────────────────
    gold_job = DataprocSubmitJobOperator(
        task_id="gold_layer",
        project_id=PROJECT_ID,
        region=GCP_REGION,
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"{SCRIPTS_GCS}/gold_job.py",
                "args": [
                    f"--project_id={PROJECT_ID}",
                    f"--bucket={GCS_BUCKET}",
                    f"--bq_dataset={BQ_DATASET}",
                ],
                "jar_file_uris": [SPARK_BQ_JAR],
            },
        },
    )

    # ── TASK 7: Delete Dataproc Cluster (always runs) ────────────────────────
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=GCP_REGION,
        trigger_rule=TriggerRule.ALL_DONE,   # Run even if upstream jobs fail
    )

    # ── TASK 8: Data Quality Check ───────────────────────────────────────────
    quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=check_data_quality,
        provide_context=True,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # TASK DEPENDENCIES
    # ─────────────────────────────────────────────────────────────────────────
    #
    #  trigger_extraction ──┐
    #                        ├──▶ create_cluster ──▶ bronze ──▶ silver ──▶ gold
    #  upload_scripts ───────┘                                       │
    #                                                                 ├──▶ delete_cluster (ALL_DONE)
    #                                                                 └──▶ quality_check
    #
    [trigger_extraction, upload_scripts] >> create_cluster
    create_cluster >> bronze_job >> silver_job >> gold_job
    gold_job >> [delete_cluster, quality_check]
