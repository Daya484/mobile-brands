"""
Mobile Brands — Extraction Module
===================================
Reads all country folders and Excel files from the source GCS bucket.
Each Excel file contains brand sheets: Samsung, Apple, Oppo, Vivo, OnePlus.
Extracts each sheet and writes it as a separate file to the raw/ landing zone
in the pipeline bucket.

Source Bucket Layout:
    mobile-brands/
        AMERICA/       file1.xlsx, file2.xlsx ...
        AUSTRALIA/     ...
        CHINA/         ...
        INDIA/         ...
        SOUTH AFRICA/  ...
        SOUTH KOREA/   ...

Destination (raw landing zone):
    mb-pipeline-bucket/
        raw/
            AMERICA/samsung/file1.csv
            AMERICA/apple/file1.csv
            ...

Note: Files are saved as CSV (not Excel) so that the Dataproc Bronze job
can read them efficiently with spark.read.csv() — no openpyxl needed.
"""

import io
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Optional

from google.cloud import storage
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

SOURCE_BUCKET_NAME = "mobile-brands"
DEST_BUCKET_NAME   = "mb-pipeline-bucket"
RAW_PREFIX         = "raw"

BRAND_SHEETS = ["Samsung", "Apple", "Oppo", "Vivo", "OnePlus"]

BRAND_FOLDER_MAP = {
    "Samsung": "samsung",
    "Apple":   "apple",
    "Oppo":    "oppo",
    "Vivo":    "vivo",
    "OnePlus": "oneplus",
}

EXCEL_EXTENSIONS = (".xlsx", ".xls", ".xlsm")
MAX_WORKERS      = 10

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)-8s]  [%(threadName)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# PER-THREAD GCS CLIENT
# ─────────────────────────────────────────────────────────────────────────────
_thread_local = threading.local()


def _get_client() -> storage.Client:
    """Return (or lazily create) a storage.Client local to the current thread."""
    if not hasattr(_thread_local, "client"):
        _thread_local.client = storage.Client()
    return _thread_local.client


# Thread-safe counters
_lock           = threading.Lock()
_total_files    = 0
_total_uploaded = 0
_total_errors   = 0


def _inc(files: int = 0, uploaded: int = 0, errors: int = 0) -> None:
    """Atomically increment shared counters."""
    global _total_files, _total_uploaded, _total_errors
    with _lock:
        _total_files    += files
        _total_uploaded += uploaded
        _total_errors   += errors


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def list_excel_blobs(bucket: storage.Bucket) -> dict[str, list[storage.Blob]]:
    """Returns a dict mapping folder prefix → list of Excel blobs."""
    folders: dict[str, list[storage.Blob]] = {}
    for blob in bucket.list_blobs():
        if blob.name.endswith("/"):
            continue
        if not blob.name.lower().endswith(EXCEL_EXTENSIONS):
            log.debug("Skipping non-Excel file: %s", blob.name)
            continue
        parts = blob.name.split("/")
        if len(parts) < 2:
            log.warning("File at root (no folder): %s — skipping.", blob.name)
            continue
        folder_prefix = parts[0]
        folders.setdefault(folder_prefix, []).append(blob)
    return folders


def _download_blob_bytes(blob: storage.Blob) -> bytes:
    """Downloads a blob and returns raw bytes."""
    buffer = io.BytesIO()
    blob.download_to_file(buffer)
    buffer.seek(0)
    return buffer.read()


def _read_brand_sheets(file_bytes: bytes, source_blob_name: str) -> dict[str, pd.DataFrame]:
    """Reads all configured brand sheets from an Excel file."""
    sheets: dict[str, pd.DataFrame] = {}
    try:
        excel_file = pd.ExcelFile(io.BytesIO(file_bytes), engine="openpyxl")
    except Exception as exc:
        log.error("Cannot parse Excel file '%s': %s", source_blob_name, exc)
        return sheets

    available = excel_file.sheet_names
    for brand in BRAND_SHEETS:
        if brand not in available:
            log.warning("Sheet '%s' NOT found in '%s' — skipping.", brand, source_blob_name)
            continue
        try:
            df = excel_file.parse(brand)
            sheets[brand] = df
        except Exception as exc:
            log.error("Error reading sheet '%s' in '%s': %s", brand, source_blob_name, exc)
    return sheets


def _df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """
    Converts a DataFrame to UTF-8 CSV bytes.
    CSV is preferred over Excel for the raw landing zone because:
      - Spark reads CSV natively (no openpyxl on cluster needed)
      - Faster read/write than Excel
      - Smaller file size for tabular data
    """
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue().encode("utf-8")


def _excel_to_csv_filename(filename: str) -> str:
    """Replaces Excel extension with .csv — e.g. sales_May01.xlsx → sales_May01.csv"""
    for ext in EXCEL_EXTENSIONS:
        if filename.lower().endswith(ext):
            return filename[: -len(ext)] + ".csv"
    return filename + ".csv"


def _upload_to_gcs(dest_bucket: storage.Bucket, dest_blob_name: str, data: bytes) -> None:
    """Uploads bytes to a destination GCS blob with metadata."""
    blob = dest_bucket.blob(dest_blob_name)
    blob.metadata = {
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
        "pipeline":       "mobile-brands",
        "layer":          "raw",
        "format":         "csv",
    }
    blob.upload_from_file(
        io.BytesIO(data),
        content_type="text/csv",
    )
    log.info("✔ Uploaded → gs://%s/%s", dest_bucket.name, dest_blob_name)


# ─────────────────────────────────────────────────────────────────────────────
# PARALLEL WORKERS
# ─────────────────────────────────────────────────────────────────────────────

def _process_single_file(blob_name: str, region: str) -> None:
    """
    Worker function executed in a thread for ONE Excel file.
    Extracts brand sheets and uploads to raw/ landing zone.
    """
    client      = _get_client()
    src_bucket  = client.bucket(SOURCE_BUCKET_NAME)
    dest_bucket = client.bucket(DEST_BUCKET_NAME)
    blob        = src_bucket.blob(blob_name)
    file_name   = blob_name.split("/")[-1]
    _inc(files=1)

    log.info("▶ Processing: gs://%s/%s", SOURCE_BUCKET_NAME, blob_name)

    # Download
    try:
        file_bytes = _download_blob_bytes(blob)
    except Exception as exc:
        log.error("✖ Download failed '%s': %s", blob_name, exc)
        _inc(errors=1)
        return

    # Extract sheets
    sheets = _read_brand_sheets(file_bytes, blob_name)
    if not sheets:
        log.warning("No brand sheets found in '%s'.", blob_name)
        return

    # Upload each brand sheet as CSV to raw landing zone
    # Path: raw/{REGION}/{brand}/{filename}.csv
    csv_file_name = _excel_to_csv_filename(file_name)   # sales_May01.xlsx → sales_May01.csv
    for brand_name, df in sheets.items():
        brand_folder   = BRAND_FOLDER_MAP[brand_name]
        dest_blob_name = f"{RAW_PREFIX}/{region}/{brand_folder}/{csv_file_name}"
        try:
            csv_bytes = _df_to_csv_bytes(df)
            _upload_to_gcs(dest_bucket, dest_blob_name, csv_bytes)
            _inc(uploaded=1)
        except Exception as exc:
            log.error("✖ Upload failed '%s': %s", dest_blob_name, exc)
            _inc(errors=1)


def _process_folder(folder_name: str, blob_names: list[str]) -> None:
    """Worker function for ONE country folder — processes all files in parallel."""
    log.info("══ Starting folder: %s  (%d file(s)) ══", folder_name, len(blob_names))
    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix=folder_name) as pool:
        futures = {
            pool.submit(_process_single_file, blob_name, folder_name): blob_name
            for blob_name in blob_names
        }
        for future in as_completed(futures):
            exc = future.exception()
            if exc:
                log.error("Unhandled error for '%s': %s", futures[future], exc)
                _inc(errors=1)
    log.info("══ Finished folder: %s ══", folder_name)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def run_extraction(
    source_bucket: Optional[str] = None,
    dest_bucket: Optional[str] = None,
) -> dict:
    """
    Main extraction function.
    Returns a summary dict with counts for the Airflow/Cloud Run caller.
    """
    # global declarations MUST come before any use of the variable
    global _total_files, _total_uploaded, _total_errors
    global SOURCE_BUCKET_NAME, DEST_BUCKET_NAME
    _total_files = _total_uploaded = _total_errors = 0

    src  = source_bucket or SOURCE_BUCKET_NAME
    dest = dest_bucket   or DEST_BUCKET_NAME

    # Allow overriding via arguments
    SOURCE_BUCKET_NAME = src
    DEST_BUCKET_NAME   = dest

    client     = storage.Client()
    src_bucket = client.bucket(SOURCE_BUCKET_NAME)

    log.info("=" * 70)
    log.info("Source : gs://%s", SOURCE_BUCKET_NAME)
    log.info("Dest   : gs://%s/%s/", DEST_BUCKET_NAME, RAW_PREFIX)
    log.info("=" * 70)

    log.info("Scanning source bucket for Excel files ...")
    folder_map = list_excel_blobs(src_bucket)

    folder_name_map: dict[str, list[str]] = {
        folder: [blob.name for blob in blobs]
        for folder, blobs in folder_map.items()
    }

    if not folder_name_map:
        log.warning("No Excel files found in gs://%s — exiting.", SOURCE_BUCKET_NAME)
        return {
            "status": "empty", 
            "folders": 0,
            "files": 0, 
            "uploaded": 0, 
            "errors": 0,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    log.info("Found %d folder(s): %s", len(folder_name_map), list(folder_name_map.keys()))

    # Process ALL folders in parallel
    with ThreadPoolExecutor(
        max_workers=len(folder_name_map),
        thread_name_prefix="country"
    ) as folder_pool:
        futures = {
            folder_pool.submit(_process_folder, folder_name, blob_names): folder_name
            for folder_name, blob_names in folder_name_map.items()
        }
        for future in as_completed(futures):
            exc = future.exception()
            if exc:
                log.error("Unhandled error in folder '%s': %s", futures[future], exc)
                _inc(errors=1)

    summary = {
        "status":   "error" if _total_errors > 0 else "success",
        "folders":  len(folder_name_map),
        "files":    _total_files,
        "uploaded": _total_uploaded,
        "errors":   _total_errors,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    log.info("=" * 70)
    log.info("DONE | Folders: %d | Files: %d | Uploaded: %d | Errors: %d",
             len(folder_name_map), _total_files, _total_uploaded, _total_errors)
    log.info("=" * 70)

    return summary


if __name__ == "__main__":
    result = run_extraction()
    print(result)
