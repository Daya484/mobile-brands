import logging
import threading
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
BUCKET_NAME      = "mobile_brands"
SOURCE_ROOT      = "transformed/"
ARCHIVE_ROOT     = "archive_transformed"
TODAY_FOLDER     = date.today().strftime("%Y-%m-%d")
DEST_BASE_PREFIX = f"{ARCHIVE_ROOT}/{TODAY_FOLDER}/"

# The folders you want to keep visible in the UI even when empty
BRANDS           = ["apple", "samsung", "oppo", "oneplus", "vivo"]
MAX_WORKERS      = 15

# ─────────────────────────────────────────────────────────────────────────────
# THREAD-SAFE COUNTERS
# ─────────────────────────────────────────────────────────────────────────────
success_count = 0
failed_count = 0
failed_files = []
counter_lock = threading.Lock()

def update_counters(is_success, file_info=None):
    global success_count, failed_count
    with counter_lock:
        if is_success:
            success_count += 1
        else:
            failed_count += 1
            if file_info:
                failed_files.append(file_info)

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s"
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# FUNCTIONS
# ─────────────────────────────────────────────────────────────────────────────

def preserve_folder_structure():
    """Creates 0-byte placeholders so folders stay visible in the UI."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    log.info("Ensuring source folder structure is preserved...")
    for brand in BRANDS:
        folder_path = f"{SOURCE_ROOT}{brand}/"
        blob = bucket.blob(folder_path)
        if not blob.exists():
            # Uploading an empty string creates a placeholder object
            blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')
            log.info(f"✔ Created/Verified placeholder: {folder_path}")

def move_single_blob(blob_name):
    """Moves a file while maintaining subfolder structure."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    
    # Strip 'transformed/' and prefix with 'archive_transformed/YYYY-MM-DD/'
    relative_path = blob_name.replace(SOURCE_ROOT, "", 1)
    new_name = f"{DEST_BASE_PREFIX}{relative_path}"

    try:
        # 1. Copy to Archive
        bucket.copy_blob(blob, bucket, new_name)
        # 2. Delete from Source
        blob.delete()
        update_counters(True)
        return f"✔ SUCCESS: {blob_name}"
    except Exception as e:
        error_msg = f"✘ FAILED: {blob_name} | Error: {str(e)}"
        update_counters(False, error_msg)
        return error_msg

def main():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    # 1. First, make sure folders don't disappear
    preserve_folder_structure()

    # 2. List all files
    log.info(f"Scanning {SOURCE_ROOT} for data files...")
    blobs = list(bucket.list_blobs(prefix=SOURCE_ROOT))
    
    # Filter: Only move files, ignore the 0-byte folder placeholders themselves
    file_names = [
        b.name for b in blobs 
        if not b.name.endswith('/') and b.size > 0
    ]

    if not file_names:
        log.info("No data files found to move.")
        print(f"\nMigration complete. 0 files moved. Structure preserved.")
        return

    log.info(f"Found {len(file_names)} files. Starting parallel archive...")

    # 3. Parallel Execution
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(move_single_blob, name) for name in file_names]
        for future in as_completed(futures):
            # We log the result of each thread as it finishes
            res = future.result()
            if "FAILED" in res:
                log.error(res)

    # 4. Final Report
    print("\n" + "="*60)
    print("                FINAL MIGRATION REPORT")
    print("="*60)
    print(f"Total Data Files Found: {len(file_names)}")
    print(f"Successfully Archived:  {success_count}")
    print(f"Failed to Move:         {failed_count}")
    
    if failed_count > 0:
        print("\nERRORS ENCOUNTERED:")
        for error in failed_files:
            print(f"  - {error}")
    
    print("\nStatus: Source folders preserved via placeholders.")
    print(f"Archive Path: gs://{BUCKET_NAME}/{DEST_BASE_PREFIX}")
    print("="*60)

if __name__ == "__main__":
    main()