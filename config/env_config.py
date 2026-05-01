"""
Environment Configuration — Mobile Brands Pipeline
====================================================
Single source of truth for dv (dev) and pd (prod) environment settings.

Rules:
  - dv  → project: dv-env   | prefix: dv-  | branch: feature/*, develop
  - pd  → project: prod-env  | prefix: pd-  | branch: main

Used by:
  - Airflow DAG (mobile_brands_dag.py)
  - PySpark jobs (via --env argument)
  - deploy scripts
"""

from dataclasses import dataclass
from typing import Literal

EnvName = Literal["dv", "pd"]


@dataclass(frozen=True)
class EnvConfig:
    env:              str   # "dv" | "pd"
    project_id:       str   # GCP project ID
    bucket:           str   # GCS pipeline bucket
    source_bucket:    str   # Source Excel bucket
    bq_dataset:       str   # BigQuery dataset
    region:           str   # GCP region
    cloud_run_service: str  # Cloud Run service name
    composer_env:     str   # Composer environment name
    dataproc_cluster: str   # Dataproc cluster name prefix
    ar_repo:          str   # Artifact Registry repo name
    machine_type:     str   # Dataproc worker machine type
    num_workers:      int   # Dataproc worker count


ENVIRONMENTS: dict[EnvName, EnvConfig] = {
    "dv": EnvConfig(
        env              = "dv",
        project_id       = "dv-env",
        bucket           = "dv-mb-pipeline-bucket",
        source_bucket    = "dv-mobile-brands",
        bq_dataset       = "mobile_brands",
        region           = "us-central1",
        cloud_run_service= "dv-mb-extractor",
        composer_env     = "dv-mb-composer",
        dataproc_cluster = "dv-mb-dataproc",
        ar_repo          = "mb-repo",
        machine_type     = "n1-standard-2",   # smaller/cheaper for dev
        num_workers      = 2,
    ),
    "pd": EnvConfig(
        env              = "pd",
        project_id       = "prod-env",
        bucket           = "pd-mb-pipeline-bucket",
        source_bucket    = "mobile-brands",
        bq_dataset       = "mobile_brands",
        region           = "us-central1",
        cloud_run_service= "pd-mb-extractor",
        composer_env     = "pd-mb-composer",
        dataproc_cluster = "pd-mb-dataproc",
        ar_repo          = "mb-repo",
        machine_type     = "n1-standard-4",   # production-grade
        num_workers      = 3,
    ),
}


def get_env(env_name: EnvName) -> EnvConfig:
    """Returns EnvConfig for the given environment name."""
    if env_name not in ENVIRONMENTS:
        raise ValueError(f"Unknown environment '{env_name}'. Must be one of: {list(ENVIRONMENTS.keys())}")
    return ENVIRONMENTS[env_name]


def ar_image_path(env: EnvConfig, tag: str = "latest") -> str:
    """Returns the full Artifact Registry image path for Cloud Run."""
    return (
        f"{env.region}-docker.pkg.dev"
        f"/{env.project_id}/{env.ar_repo}"
        f"/mb-extractor:{tag}"
    )


def service_account_email(env: EnvConfig) -> str:
    """Returns the pipeline service account email."""
    return f"mb-pipeline-sa@{env.project_id}.iam.gserviceaccount.com"
