"""
BigQuery Schema Setup Script
==============================
Creates the mobile_brands dataset and all required tables
in BigQuery if they don't already exist.

Tables created:
  - silver_sales       (Silver layer output)
  - gold_brand_kpi     (Gold KPI per brand/region/month)
  - gold_region_summary (Gold market totals per region/month)
  - gold_daily_trend   (Gold daily time-series)

Usage:
    python setup_bigquery.py --project_id=YOUR_PROJECT_ID --dataset=mobile_brands
"""

import argparse
from google.cloud import bigquery
from google.api_core.exceptions import Conflict

# ─────────────────────────────────────────────────────────────────────────────
# TABLE SCHEMAS
# ─────────────────────────────────────────────────────────────────────────────

SILVER_SALES_SCHEMA = [
    bigquery.SchemaField("sale_date",      "DATE",      mode="NULLABLE"),
    bigquery.SchemaField("brand",          "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("region",         "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("model",          "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("units_sold",     "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("price_usd",      "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("revenue_usd",    "FLOAT64",   mode="NULLABLE"),
    bigquery.SchemaField("distributor",    "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("retailer",       "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("source_file",    "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("ingestion_time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("processed_time", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("pipeline_layer", "STRING",    mode="REQUIRED"),
]

GOLD_BRAND_KPI_SCHEMA = [
    bigquery.SchemaField("brand",              "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("region",             "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("year",               "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("month",              "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("total_units",        "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("total_revenue",      "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("avg_price_usd",      "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("distinct_models",    "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("market_share_pct",   "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("revenue_rank",       "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("last_updated",       "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("pipeline_layer",     "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("computed_at",        "TIMESTAMP", mode="REQUIRED"),
]

GOLD_REGION_SUMMARY_SCHEMA = [
    bigquery.SchemaField("region",                "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("year",                  "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("month",                 "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("total_market_units",    "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("total_market_revenue",  "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("avg_market_price",      "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("active_brands",         "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("top_brand",             "STRING",  mode="NULLABLE"),
    bigquery.SchemaField("top_brand_share_pct",   "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("last_updated",          "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("pipeline_layer",        "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("computed_at",           "TIMESTAMP", mode="REQUIRED"),
]

GOLD_DAILY_TREND_SCHEMA = [
    bigquery.SchemaField("brand",                "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("region",               "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("sale_date",            "DATE",    mode="NULLABLE"),
    bigquery.SchemaField("year",                 "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("month",                "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("day",                  "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("daily_units",          "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("daily_revenue",        "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("daily_avg_price",      "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("models_sold",          "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("rolling_7d_avg_units", "FLOAT64", mode="NULLABLE"),
    bigquery.SchemaField("pipeline_layer",       "STRING",  mode="REQUIRED"),
    bigquery.SchemaField("computed_at",          "TIMESTAMP", mode="REQUIRED"),
]

TABLES = {
    "silver_sales":        SILVER_SALES_SCHEMA,
    "gold_brand_kpi":      GOLD_BRAND_KPI_SCHEMA,
    "gold_region_summary": GOLD_REGION_SUMMARY_SCHEMA,
    "gold_daily_trend":    GOLD_DAILY_TREND_SCHEMA,
}


# ─────────────────────────────────────────────────────────────────────────────
# SETUP FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def create_dataset(client: bigquery.Client, project_id: str, dataset_id: str) -> None:
    """Creates BigQuery dataset if it doesn't exist."""
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset_ref.location = "US"
    dataset_ref.description = "Mobile Brands sales data pipeline — medallion architecture"
    try:
        client.create_dataset(dataset_ref, exists_ok=True)
        print(f"✅ Dataset ready: {project_id}.{dataset_id}")
    except Exception as exc:
        print(f"❌ Failed to create dataset: {exc}")
        raise


def create_table(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    table_id: str,
    schema: list,
) -> None:
    """Creates a BigQuery table if it doesn't exist."""
    table_ref = bigquery.Table(f"{project_id}.{dataset_id}.{table_id}", schema=schema)
    try:
        client.create_table(table_ref)
        print(f"  ✅ Created table: {table_id}")
    except Conflict:
        print(f"  ⚠️  Table already exists: {table_id} (skipped)")
    except Exception as exc:
        print(f"  ❌ Failed to create {table_id}: {exc}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Setup BigQuery tables for Mobile Brands pipeline")
    parser.add_argument("--project_id", required=True, help="GCP Project ID")
    parser.add_argument("--dataset",    default="mobile_brands", help="BigQuery dataset name")
    args = parser.parse_args()

    project_id = args.project_id
    dataset_id = args.dataset

    print(f"\n🚀 Setting up BigQuery — project={project_id}  dataset={dataset_id}")
    print("=" * 60)

    client = bigquery.Client(project=project_id)

    create_dataset(client, project_id, dataset_id)

    print(f"\nCreating tables in {project_id}.{dataset_id}:")
    for table_id, schema in TABLES.items():
        create_table(client, project_id, dataset_id, table_id, schema)

    print("\n" + "=" * 60)
    print("✅ BigQuery setup complete!")
    print(f"   Dataset: {project_id}.{dataset_id}")
    print(f"   Tables:  {', '.join(TABLES.keys())}")


if __name__ == "__main__":
    main()
