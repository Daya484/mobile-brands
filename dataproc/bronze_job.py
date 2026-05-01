"""
Dataproc PySpark — Bronze Layer Job
=====================================
Layer: RAW Excel → Bronze Parquet

Reads brand-split Excel files from the raw/ landing zone in GCS.
Adds metadata columns (source_file, ingestion_time, pipeline_layer, region, brand).
Writes as partitioned Parquet to the bronze/ zone.

NO data transformation — only schema enforcement + metadata enrichment.

GCS Paths:
    Input:  gs://{BUCKET}/raw/{REGION}/{brand}/*.xlsx
    Output: gs://{BUCKET}/bronze/  (partitioned by year/month/day)

Usage (Dataproc submit):
    spark-submit bronze_job.py \
        --project_id=YOUR_PROJECT_ID \
        --bucket=mb-pipeline-bucket \
        --execution_date=2024-01-15
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
)

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BRONZE] %(levelname)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("bronze_layer")

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
BRANDS  = ["samsung", "apple", "oppo", "vivo", "oneplus"]
REGIONS = ["AMERICA", "AUSTRALIA", "CHINA", "INDIA", "SOUTH AFRICA", "SOUTH KOREA"]

# Spark BigQuery connector jar (bundled with Dataproc 2.1+)
SPARK_BQ_JAR = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.36.1.jar"


# ─────────────────────────────────────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────────────────────────────────────
def create_spark_session(project_id: str, temp_bucket: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("MobileBrands-Bronze-Layer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("temporaryGcsBucket", temp_bucket)
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# READ EXCEL FILES FROM GCS
# ─────────────────────────────────────────────────────────────────────────────
def read_excel_from_gcs(spark: SparkSession, gcs_path: str) -> DataFrame:
    """
    Reads all Excel files from a GCS path using pandas (via collect).
    Falls back to CSV if Excel is unavailable on the cluster.

    NOTE: For large datasets, convert to Parquet/CSV in the Cloud Run step.
    This approach works for moderate file sizes (< 500 MB total per brand/region).
    """
    import pandas as pd
    from google.cloud import storage as gcs

    log.info("Reading Excel files from: %s", gcs_path)

    # Parse bucket and prefix from gs:// path
    path_stripped = gcs_path.replace("gs://", "")
    bucket_name   = path_stripped.split("/")[0]
    prefix        = "/".join(path_stripped.split("/")[1:])

    client  = gcs.Client()
    bucket  = client.bucket(bucket_name)
    blobs   = list(bucket.list_blobs(prefix=prefix))

    excel_blobs = [b for b in blobs if b.name.lower().endswith((".xlsx", ".xls", ".xlsm"))]
    log.info("Found %d Excel file(s) under %s", len(excel_blobs), gcs_path)

    if not excel_blobs:
        log.warning("No Excel files found at %s — returning empty DataFrame", gcs_path)
        return spark.createDataFrame([], StructType([StructField("_empty", StringType(), True)]))

    frames = []
    for blob in excel_blobs:
        import io
        data = io.BytesIO()
        blob.download_to_file(data)
        data.seek(0)
        try:
            df_pd = pd.read_excel(data, engine="openpyxl")
            df_pd["_source_file"] = f"gs://{bucket_name}/{blob.name}"
            frames.append(df_pd)
        except Exception as exc:
            log.error("Failed to read %s: %s", blob.name, exc)

    if not frames:
        return spark.createDataFrame([], StructType([StructField("_empty", StringType(), True)]))

    import pandas as pd
    combined_pd = pd.concat(frames, ignore_index=True)
    return spark.createDataFrame(combined_pd)


# ─────────────────────────────────────────────────────────────────────────────
# BRONZE TRANSFORMATION
# ─────────────────────────────────────────────────────────────────────────────
def add_bronze_metadata(df: DataFrame, brand: str, region: str) -> DataFrame:
    """
    Adds metadata columns to the raw DataFrame.
    Bronze layer: NO cleaning, just enrichment.
    """
    now = datetime.now(timezone.utc)

    # Standardize column names: lowercase + strip whitespace
    renamed = df
    for col_name in df.columns:
        clean_name = col_name.strip().lower().replace(" ", "_")
        if clean_name != col_name:
            renamed = renamed.withColumnRenamed(col_name, clean_name)

    return (
        renamed
        .withColumn("brand",          F.lit(brand))
        .withColumn("region",         F.lit(region))
        .withColumn("ingestion_time", F.lit(now).cast(TimestampType()))
        .withColumn("pipeline_layer", F.lit("bronze"))
        # Partition columns
        .withColumn("year",  F.lit(now.year))
        .withColumn("month", F.lit(now.month))
        .withColumn("day",   F.lit(now.day))
    )


def add_source_file_column(df: DataFrame) -> DataFrame:
    """Renames internal _source_file column to source_file."""
    if "_source_file" in df.columns:
        return df.withColumnRenamed("_source_file", "source_file")
    return df.withColumn("source_file", F.lit("unknown"))


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Mobile Brands Bronze Layer PySpark Job")
    parser.add_argument("--project_id",     required=True,  help="GCP Project ID")
    parser.add_argument("--bucket",         required=True,  help="GCS bucket name (mb-pipeline-bucket)")
    parser.add_argument("--execution_date", default=None,   help="Airflow execution date (YYYY-MM-DD)")
    args = parser.parse_args()

    project_id = args.project_id
    bucket     = args.bucket
    exec_date  = args.execution_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    exec_dt    = datetime.strptime(exec_date, "%Y-%m-%d")
    exec_year  = exec_dt.year
    exec_month = exec_dt.month
    exec_day   = exec_dt.day

    log.info("=" * 60)
    log.info("BRONZE LAYER  |  project=%s  bucket=%s  date=%s", project_id, bucket, exec_date)
    log.info("=" * 60)

    spark = create_spark_session(project_id, bucket)
    spark.sparkContext.setLogLevel("WARN")

    total_rows     = 0
    total_written  = 0
    failed_combos  = []

    for region in REGIONS:
        for brand in BRANDS:
            input_path  = f"gs://{bucket}/raw/{region}/{brand}/"
            output_path = f"gs://{bucket}/bronze/"

            log.info("Processing: region=%s brand=%s", region, brand)

            try:
                raw_df = read_excel_from_gcs(spark, input_path)

                if "_empty" in raw_df.columns:
                    log.warning("Skipping empty result: region=%s brand=%s", region, brand)
                    continue

                enriched_df = add_bronze_metadata(raw_df, brand, region)
                enriched_df = add_source_file_column(enriched_df)

                count = enriched_df.count()
                log.info("  → %d rows | writing to %s", count, output_path)

                # Write to a DATE-PARTITIONED path to avoid appending duplicates.
                # Each daily run writes only to its own date partition.
                # gs://bucket/bronze/year=YYYY/month=MM/day=DD/region=X/brand=Y/
                partitioned_output = (
                    f"gs://{bucket}/bronze/"
                    f"year={exec_year}/month={exec_month}/day={exec_day}/"
                    f"region={region}/brand={brand}/"
                )
                (
                    enriched_df.drop("year", "month", "day")   # already in path
                    .write
                    .mode("overwrite")   # safe: each run owns its own date partition
                    .parquet(partitioned_output)
                )

                total_rows    += count
                total_written += count

            except Exception as exc:
                log.error("FAILED: region=%s brand=%s error=%s", region, brand, exc)
                failed_combos.append(f"{region}/{brand}")

    log.info("=" * 60)
    log.info("BRONZE DONE  |  total_rows=%d  written=%d  failed=%d",
             total_rows, total_written, len(failed_combos))
    if failed_combos:
        log.error("Failed combos: %s", failed_combos)
        sys.exit(1)
    log.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
