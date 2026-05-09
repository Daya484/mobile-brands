import io
import os
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from google.cloud import storage

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
SOURCE_BUCKET    = "mobile_brands"
SOURCE_PREFIX    = "landing"       # The source sub-folder
DEST_BUCKET      = "mobile_brands" 
TRANSFORMED_ROOT = "transformed"   # The destination sub-folder

BRAND_SHEETS = ["Samsung", "Apple", "Oppo", "Vivo", "OnePlus"]

BRAND_FOLDER_MAP = {
    "Samsung": "samsung",
    "Apple":   "apple",
    "Oppo":    "oppo",
    "Vivo":    "vivo",
    "OnePlus": "oneplus",
}

EXCEL_EXTENSIONS = (".xlsx", ".xls", ".xlsm")
MAX_WORKERS = 10

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# THREAD‑LOCAL GCS CLIENT
# ─────────────────────────────────────────────────────────────────────────────
_thread_local = threading.local()

def get_client() -> storage.Client:
    if not hasattr(_thread_local, "client"):
        _thread_local.client = storage.Client()
    return _thread_local.client

# Counters
_lock = threading.Lock()
_total_files = 0
_total_uploaded = 0
_total_errors = 0

def inc(files=0, uploaded=0, errors=0):
    global _total_files, _total_uploaded, _total_errors
    with _lock:
        _total_files += files
        _total_uploaded += uploaded
        _total_errors += errors

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def list_excel_files(bucket: storage.Bucket) -> dict[str, list[str]]:
    """Return {country_folder: [blob_name, ...]} by looking in the landing/ folder"""
    result = {}

    # We only list blobs starting with 'landing/'
    blobs = bucket.list_blobs(prefix=f"{SOURCE_PREFIX}/")

    for blob in blobs:
        # 1. Skip if it's just the folder itself
        if blob.name == f"{SOURCE_PREFIX}/":
            continue

        # 2. Check for excel extensions
        if not blob.name.lower().endswith(EXCEL_EXTENSIONS):
            continue
            
        parts = blob.name.split("/")
        # Path structure: landing / COUNTRY / filename.xlsx
        # Index:           0     /    1    /    2
        if len(parts) < 3:
            continue
            
        country_folder = parts[1]
        result.setdefault(country_folder, []).append(blob.name)

    return result


def download_blob(blob: storage.Blob) -> bytes:
    buf = io.BytesIO()
    blob.download_to_file(buf)
    buf.seek(0)
    return buf.read()


def read_excel_sheets(file_bytes: bytes, blob_name: str) -> dict[str, pd.DataFrame]:
    sheets = {}
    try:
        xls = pd.ExcelFile(io.BytesIO(file_bytes), engine="openpyxl")
    except Exception as e:
        log.error("Failed to parse Excel %s: %s", blob_name, e)
        return sheets

    for brand in BRAND_SHEETS:
        if brand not in xls.sheet_names:
            continue
        try:
            sheets[brand] = xls.parse(brand)
        except Exception as e:
            log.error("Failed sheet %s in %s: %s", brand, blob_name, e)

    return sheets


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")


def excel_to_csv_name(filename: str) -> str:
    for ext in EXCEL_EXTENSIONS:
        if filename.lower().endswith(ext):
            return filename[:-len(ext)] + ".csv"
    return filename + ".csv"


# ─────────────────────────────────────────────────────────────────────────────
# WORKERS
# ─────────────────────────────────────────────────────────────────────────────

def process_file(blob_name: str):
    client = get_client()
    src_bucket = client.bucket(SOURCE_BUCKET)
    dest_bucket = client.bucket(DEST_BUCKET)
    blob = src_bucket.blob(blob_name)

    inc(files=1)
    log.info("Processing %s", blob_name)

    try:
        file_bytes = download_blob(blob)
    except Exception as e:
        log.error("Download failed %s: %s", blob_name, e)
        inc(errors=1)
        return

    sheets = read_excel_sheets(file_bytes, blob_name)
    if not sheets:
        return

    csv_name = excel_to_csv_name(os.path.basename(blob_name))

    for brand, df in sheets.items():
        # Destination path: transformed / brand / filename.csv
        dest_path = f"{TRANSFORMED_ROOT}/{BRAND_FOLDER_MAP[brand]}/{csv_name}"
        try:
            data = df_to_csv_bytes(df)
            dest_bucket.blob(dest_path).upload_from_string(
                data,
                content_type="text/csv"
            )
            log.info("✔ Uploaded gs://%s/%s", DEST_BUCKET, dest_path)
            inc(uploaded=1)
        except Exception as e:
            log.error("Upload failed %s: %s", dest_path, e)
            inc(errors=1)


def process_country(folder: str, blobs: list[str]):
    log.info("=== Country %s (%d files) ===", folder, len(blobs))
    with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix=folder) as pool:
        list(pool.map(process_file, blobs))


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    client = storage.Client()
    bucket = client.bucket(SOURCE_BUCKET)

    log.info("=" * 70)
    log.info("Source: gs://%s/%s/", SOURCE_BUCKET, SOURCE_PREFIX)
    log.info("Dest  : gs://%s/%s/ (CSV)", DEST_BUCKET, TRANSFORMED_ROOT)
    log.info("=" * 70)

    folder_map = list_excel_files(bucket)
    if not folder_map:
        log.warning("No Excel files found in %s/%s/", SOURCE_BUCKET, SOURCE_PREFIX)
        return

    with ThreadPoolExecutor(
        max_workers=len(folder_map),
        thread_name_prefix="country"
    ) as pool:
        futures = [
            pool.submit(process_country, folder, blobs)
            for folder, blobs in folder_map.items()
        ]
        for f in as_completed(futures):
            pass

    log.info("=" * 70)
    log.info(
        "DONE | Files: %d | Uploaded: %d | Errors: %d",
        _total_files, _total_uploaded, _total_errors
    )
    log.info("=" * 70)


if __name__ == "__main__":
    main()