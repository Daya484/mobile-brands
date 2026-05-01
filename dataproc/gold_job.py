"""
Dataproc PySpark — Gold Layer Job
====================================
Layer: Silver Parquet → Gold Parquet + BigQuery (KPI Tables)

Reads cleaned Silver data and computes business-level KPIs:
  1. gold_brand_kpi      — per brand + region + month KPIs (market share, revenue)
  2. gold_region_summary — per region + month market totals
  3. gold_daily_trend    — daily granularity for time-series analysis

Writes to:
  - GCS: gs://{BUCKET}/gold/brand_kpi/     (Parquet)
  - GCS: gs://{BUCKET}/gold/region_summary/ (Parquet)
  - GCS: gs://{BUCKET}/gold/daily_trend/   (Parquet)
  - BQ:  {PROJECT}.mobile_brands.gold_brand_kpi
  - BQ:  {PROJECT}.mobile_brands.gold_region_summary
  - BQ:  {PROJECT}.mobile_brands.gold_daily_trend

Usage:
    spark-submit gold_job.py \
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
from pyspark.sql.types import TimestampType, DoubleType
from pyspark.sql.window import Window

# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [GOLD] %(levelname)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("gold_layer")


# ─────────────────────────────────────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────────────────────────────────────
def create_spark_session(project_id: str, temp_bucket: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("MobileBrands-Gold-Layer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("temporaryGcsBucket", temp_bucket)
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# GOLD KPI COMPUTATIONS
# ─────────────────────────────────────────────────────────────────────────────

def compute_brand_kpi(silver_df: DataFrame) -> DataFrame:
    """
    KPI Table 1: Brand-level monthly KPIs.

    Columns:
      brand, region, year, month,
      total_units, total_revenue, avg_price_usd,
      distinct_models, market_share_pct
    """
    # Aggregate per brand + region + year + month
    brand_agg = silver_df.groupBy("brand", "region", "year", "month").agg(
        F.sum("units_sold").alias("total_units"),
        F.sum("revenue_usd").cast(DoubleType()).alias("total_revenue"),
        F.avg("price_usd").cast(DoubleType()).alias("avg_price_usd"),
        F.countDistinct("model").alias("distinct_models"),
        F.max("processed_time").alias("last_updated"),
    )

    # Market share within each region + month
    window_region_month = Window.partitionBy("region", "year", "month")
    total_units_per_region = F.sum("total_units").over(window_region_month)

    brand_kpi = brand_agg.withColumn(
        "market_share_pct",
        F.round(
            (F.col("total_units").cast(DoubleType()) / total_units_per_region) * 100,
            2
        )
    )

    # Revenue rank within region+month
    rank_window = Window.partitionBy("region", "year", "month").orderBy(F.col("total_revenue").desc())
    brand_kpi   = brand_kpi.withColumn("revenue_rank", F.rank().over(rank_window))

    # Add Gold metadata
    now = datetime.now(timezone.utc)
    brand_kpi = (
        brand_kpi
        .withColumn("pipeline_layer",  F.lit("gold"))
        .withColumn("computed_at",     F.lit(now).cast(TimestampType()))
    )

    log.info("gold_brand_kpi: %d rows", brand_kpi.count())
    return brand_kpi


def compute_region_summary(silver_df: DataFrame) -> DataFrame:
    """
    KPI Table 2: Region-level monthly market summary.

    Columns:
      region, year, month,
      total_market_units, total_market_revenue, avg_market_price,
      active_brands, top_brand, top_brand_share_pct
    """
    # Overall market aggregation per region + month
    region_agg = silver_df.groupBy("region", "year", "month").agg(
        F.sum("units_sold").alias("total_market_units"),
        F.sum("revenue_usd").cast(DoubleType()).alias("total_market_revenue"),
        F.avg("price_usd").cast(DoubleType()).alias("avg_market_price"),
        F.countDistinct("brand").alias("active_brands"),
        F.max("processed_time").alias("last_updated"),
    )

    # Find top brand per region + month
    brand_units = silver_df.groupBy("region", "year", "month", "brand").agg(
        F.sum("units_sold").alias("brand_units")
    )
    top_brand_window = Window.partitionBy("region", "year", "month").orderBy(
        F.col("brand_units").desc()
    )
    top_brands = (
        brand_units
        .withColumn("_rank", F.rank().over(top_brand_window))
        .filter(F.col("_rank") == 1)
        .select("region", "year", "month", F.col("brand").alias("top_brand"),
                F.col("brand_units").alias("top_brand_units"))
    )

    region_summary = region_agg.join(top_brands, on=["region", "year", "month"], how="left")

    # Top brand market share
    region_summary = region_summary.withColumn(
        "top_brand_share_pct",
        F.round(
            (F.col("top_brand_units").cast(DoubleType()) / F.col("total_market_units")) * 100,
            2
        )
    ).drop("top_brand_units")

    # Add Gold metadata
    now = datetime.now(timezone.utc)
    region_summary = (
        region_summary
        .withColumn("pipeline_layer", F.lit("gold"))
        .withColumn("computed_at",    F.lit(now).cast(TimestampType()))
    )

    log.info("gold_region_summary: %d rows", region_summary.count())
    return region_summary


def compute_daily_trend(silver_df: DataFrame) -> DataFrame:
    """
    KPI Table 3: Daily granularity for brand + region time-series.

    Columns:
      brand, region, sale_date, year, month, day,
      daily_units, daily_revenue, daily_avg_price,
      mom_units_change_pct  (month-over-month %, rolling)
    """
    daily_agg = silver_df.groupBy(
        "brand", "region", "sale_date", "year", "month", "day"
    ).agg(
        F.sum("units_sold").alias("daily_units"),
        F.sum("revenue_usd").cast(DoubleType()).alias("daily_revenue"),
        F.avg("price_usd").cast(DoubleType()).alias("daily_avg_price"),
        F.countDistinct("model").alias("models_sold"),
    )

    # 7-day rolling average of daily_units (window function)
    w7 = (
        Window
        .partitionBy("brand", "region")
        .orderBy("sale_date")
        .rowsBetween(-6, 0)
    )
    daily_agg = daily_agg.withColumn(
        "rolling_7d_avg_units",
        F.round(F.avg("daily_units").over(w7), 2)
    )

    # Add Gold metadata
    now = datetime.now(timezone.utc)
    daily_agg = (
        daily_agg
        .withColumn("pipeline_layer", F.lit("gold"))
        .withColumn("computed_at",    F.lit(now).cast(TimestampType()))
    )

    log.info("gold_daily_trend: %d rows", daily_agg.count())
    return daily_agg


# ─────────────────────────────────────────────────────────────────────────────
# WRITE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def write_gold_parquet(df: DataFrame, path: str, partition_cols: list) -> None:
    """Write Gold Parquet to GCS."""
    log.info("Writing Parquet to %s ...", path)
    df.write.partitionBy(*partition_cols).mode("overwrite").parquet(path)
    log.info("Parquet written: %s", path)


def write_gold_bq(df: DataFrame, table: str, bucket: str) -> None:
    """Write Gold DataFrame to BigQuery."""
    log.info("Writing BigQuery table: %s ...", table)
    (
        df.write
        .format("bigquery")
        .option("table", table)
        .option("temporaryGcsBucket", bucket)
        .option("writeMethod", "indirect")
        .mode("overwrite")
        .save()
    )
    log.info("BigQuery write complete: %s", table)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Mobile Brands Gold Layer PySpark Job")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--bucket",     required=True, help="GCS bucket (mb-pipeline-bucket)")
    parser.add_argument("--bq_dataset", default="mobile_brands", help="BigQuery dataset")
    args = parser.parse_args()

    project_id = args.project_id
    bucket     = args.bucket
    bq_dataset = args.bq_dataset

    input_path = f"gs://{bucket}/silver/"

    log.info("=" * 60)
    log.info("GOLD LAYER  |  project=%s  bucket=%s", project_id, bucket)
    log.info("  Input:  %s", input_path)
    log.info("=" * 60)

    spark = create_spark_session(project_id, bucket)
    spark.sparkContext.setLogLevel("WARN")

    # ── READ Silver ───────────────────────────────────────────
    log.info("Reading Silver Parquet ...")
    silver_df    = spark.read.parquet(input_path)
    silver_count = silver_df.count()
    log.info("Silver records: %d", silver_count)

    # Cache Silver for multiple downstream passes
    silver_df.cache()

    # ── GOLD KPI 1: Brand KPI ─────────────────────────────────
    brand_kpi_df = compute_brand_kpi(silver_df)
    write_gold_parquet(brand_kpi_df, f"gs://{bucket}/gold/brand_kpi/", ["year", "month", "region"])
    write_gold_bq(brand_kpi_df, f"{project_id}.{bq_dataset}.gold_brand_kpi", bucket)

    # ── GOLD KPI 2: Region Summary ────────────────────────────
    region_df = compute_region_summary(silver_df)
    write_gold_parquet(region_df, f"gs://{bucket}/gold/region_summary/", ["year", "month"])
    write_gold_bq(region_df, f"{project_id}.{bq_dataset}.gold_region_summary", bucket)

    # ── GOLD KPI 3: Daily Trend ───────────────────────────────
    daily_df = compute_daily_trend(silver_df)
    write_gold_parquet(daily_df, f"gs://{bucket}/gold/daily_trend/", ["year", "month", "brand"])
    write_gold_bq(daily_df, f"{project_id}.{bq_dataset}.gold_daily_trend", bucket)

    silver_df.unpersist()

    log.info("=" * 60)
    log.info("GOLD DONE  |  silver_input=%d", silver_count)
    log.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
