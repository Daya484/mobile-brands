"""
Dataproc PySpark — Silver Layer Job
=====================================
Layer: Bronze Parquet → Silver Parquet + BigQuery

Reads Bronze Parquet data, applies:
  - Type casting (strings → proper types)
  - Null filtering on mandatory fields
  - Deduplication
  - Brand & region normalization
  - Revenue recomputation
  - Date parsing

Writes cleaned data to Silver GCS zone and BigQuery silver_sales table.

GCS Paths:
    Input:  gs://{BUCKET}/bronze/  (Parquet)
    Output: gs://{BUCKET}/silver/  (Parquet)

BigQuery:
    Output: {PROJECT}.mobile_brands.silver_sales

Usage:
    spark-submit silver_job.py \
        --project_id=YOUR_PROJECT_ID \
        --bucket=mb-pipeline-bucket \
        --bq_dataset=mobile_brands
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, LongType, DateType, TimestampType, StringType,
)

# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SILVER] %(levelname)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("silver_layer")


# ─────────────────────────────────────────────────────────────────────────────
# BRAND & REGION NORMALIZATION MAPS
# ─────────────────────────────────────────────────────────────────────────────
VALID_BRANDS = {"samsung", "apple", "oppo", "vivo", "oneplus"}

BRAND_CANONICAL = {
    "samsung": "Samsung",
    "apple":   "Apple",
    "oppo":    "Oppo",
    "vivo":    "Vivo",
    "oneplus": "OnePlus",
}

REGION_CANONICAL = {
    "america":      "AMERICA",
    "australia":    "AUSTRALIA",
    "china":        "CHINA",
    "india":        "INDIA",
    "south africa": "SOUTH AFRICA",
    "south korea":  "SOUTH KOREA",
}


# ─────────────────────────────────────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────────────────────────────────────
def create_spark_session(project_id: str, temp_bucket: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("MobileBrands-Silver-Layer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("temporaryGcsBucket", temp_bucket)
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# SILVER TRANSFORMATIONS
# ─────────────────────────────────────────────────────────────────────────────

def normalize_brands(df: DataFrame) -> DataFrame:
    """Normalize brand names to canonical forms."""
    lowered = F.lower(F.trim(F.col("brand")))
    expr    = F.lit(None).cast(StringType())
    for raw, canonical in BRAND_CANONICAL.items():
        expr = F.when(lowered == raw, canonical).otherwise(expr)
    # Keep original if no match (will be filtered later)
    return df.withColumn("brand", F.coalesce(expr, F.col("brand")))


def normalize_regions(df: DataFrame) -> DataFrame:
    """Normalize region names to uppercase canonical forms."""
    lowered = F.lower(F.trim(F.col("region")))
    expr    = F.lit(None).cast(StringType())
    for raw, canonical in REGION_CANONICAL.items():
        expr = F.when(lowered == raw, canonical).otherwise(expr)
    return df.withColumn("region", F.coalesce(expr, F.upper(F.col("region"))))


def cast_types(df: DataFrame) -> DataFrame:
    """
    Type-cast raw string columns to proper types.
    Handles multiple date formats gracefully.
    """
    # Try multiple date formats
    date_col = (
        F.coalesce(
            F.to_date(F.col("date"), "yyyy-MM-dd"),
            F.to_date(F.col("date"), "dd/MM/yyyy"),
            F.to_date(F.col("date"), "MM/dd/yyyy"),
            F.to_date(F.col("date"), "dd-MM-yyyy"),
            F.to_date(F.col("date"), "MMM d, yyyy"),
        )
    )

    return (
        df
        .withColumn("sale_date",   date_col)
        .withColumn("units_sold",  F.col("units_sold").cast(LongType()))
        .withColumn("price_usd",   F.col("price_usd").cast(DoubleType()))
        .withColumn("revenue_usd", F.col("revenue_usd").cast(DoubleType()))
    )


def recompute_revenue(df: DataFrame) -> DataFrame:
    """Recompute revenue_usd to ensure correctness."""
    return df.withColumn(
        "revenue_usd",
        (F.col("units_sold").cast(DoubleType()) * F.col("price_usd").cast(DoubleType()))
    )


def filter_valid_records(df: DataFrame) -> DataFrame:
    """Drop records with null mandatory fields or invalid values."""
    return df.filter(
        F.col("brand").isNotNull() &
        F.col("region").isNotNull() &
        F.col("sale_date").isNotNull() &
        F.col("units_sold").isNotNull() &
        F.col("price_usd").isNotNull() &
        (F.col("units_sold") > 0) &
        (F.col("price_usd") > 0)
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Remove duplicates on (sale_date, brand, region, model).
    Keeps row with the latest ingestion_time.
    """
    from pyspark.sql.window import Window
    w = (
        Window
        .partitionBy("sale_date", "brand", "region", "model")
        .orderBy(F.col("ingestion_time").desc())
    )
    return (
        df
        .withColumn("_rank", F.row_number().over(w))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def add_date_parts(df: DataFrame) -> DataFrame:
    """Add partition columns from sale_date."""
    return (
        df
        .withColumn("year",  F.year(F.col("sale_date")))
        .withColumn("month", F.month(F.col("sale_date")))
        .withColumn("day",   F.dayofmonth(F.col("sale_date")))
    )


def add_silver_metadata(df: DataFrame) -> DataFrame:
    """Add processed_time and update pipeline_layer."""
    now = datetime.now(timezone.utc)
    return (
        df
        .withColumn("processed_time", F.lit(now).cast(TimestampType()))
        .withColumn("pipeline_layer", F.lit("silver"))
    )


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Mobile Brands Silver Layer PySpark Job")
    parser.add_argument("--project_id",  required=True, help="GCP Project ID")
    parser.add_argument("--bucket",      required=True, help="GCS bucket (mb-pipeline-bucket)")
    parser.add_argument("--bq_dataset",  default="mobile_brands", help="BigQuery dataset name")
    args = parser.parse_args()

    project_id = args.project_id
    bucket     = args.bucket
    bq_dataset = args.bq_dataset

    input_path  = f"gs://{bucket}/bronze/"
    output_path = f"gs://{bucket}/silver/"
    bq_table    = f"{project_id}.{bq_dataset}.silver_sales"

    log.info("=" * 60)
    log.info("SILVER LAYER  |  project=%s  bucket=%s", project_id, bucket)
    log.info("  Input:  %s", input_path)
    log.info("  Output: %s", output_path)
    log.info("  BQ:     %s", bq_table)
    log.info("=" * 60)

    spark = create_spark_session(project_id, bucket)
    spark.sparkContext.setLogLevel("WARN")

    # ── READ Bronze ──────────────────────────────────────────
    log.info("Reading Bronze Parquet ...")
    bronze_df = spark.read.parquet(input_path)
    bronze_count = bronze_df.count()
    log.info("Bronze records: %d", bronze_count)

    # ── TRANSFORMATIONS ──────────────────────────────────────
    log.info("Applying Silver transformations ...")
    silver_df = (
        bronze_df
        .transform(cast_types)
        .transform(normalize_brands)
        .transform(normalize_regions)
        .transform(recompute_revenue)
        .transform(filter_valid_records)
        .transform(deduplicate)
        .transform(add_date_parts)
        .transform(add_silver_metadata)
    )

    # Select final Silver columns
    silver_final = silver_df.select(
        "sale_date", "brand", "region", "model",
        "units_sold", "price_usd", "revenue_usd",
        "distributor", "retailer",
        "source_file", "ingestion_time", "processed_time", "pipeline_layer",
        "year", "month", "day",
    )

    silver_count = silver_final.count()
    dropped = bronze_count - silver_count
    log.info("Silver records: %d  (dropped %d invalid)", silver_count, dropped)

    # ── WRITE to GCS ─────────────────────────────────────────
    log.info("Writing Silver Parquet to %s ...", output_path)
    (
        silver_final.write
        .partitionBy("year", "month", "day", "brand", "region")
        .mode("overwrite")
        .parquet(output_path)
    )
    log.info("Silver Parquet written successfully.")

    # ── WRITE to BigQuery ────────────────────────────────────
    bq_df = silver_final.drop("year", "month", "day")  # BQ doesn't need partition cols
    log.info("Writing to BigQuery table: %s ...", bq_table)
    (
        bq_df.write
        .format("bigquery")
        .option("table", bq_table)
        .option("temporaryGcsBucket", bucket)
        .option("writeMethod", "indirect")
        .mode("overwrite")
        .save()
    )
    log.info("BigQuery write complete.")

    log.info("=" * 60)
    log.info("SILVER DONE  |  input=%d  output=%d  dropped=%d",
             bronze_count, silver_count, dropped)
    log.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
