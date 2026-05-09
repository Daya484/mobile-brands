import logging
import threading
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
BUCKET_NAME      = "mobile_brands"
SOURCE_ROOT      = "landing/"
ARCHIVE_ROOT     = "archive_landing"
TODAY_FOLDER     = date.today().strftime("%Y-%m-%d")
DEST_BASE_PREFIX = f"{ARCHIVE_ROOT}/{TODAY_FOLDER}/"

# Updated to match your Country folders in the landing directory
COUNTRIES        = ["INDIA", "CHINA", "AMERICA", "SOUTH KOREA", "AUSTRALIA", "SOUTH AFRICA"]
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
    """Creates 0-byte placeholders so Country folders stay visible in landing/"""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    log.info(f"Ensuring {SOURCE_ROOT} structure is preserved...")
    for country in COUNTRIES:
        folder_path = f"{SOURCE_ROOT}{country}/"
        blob = bucket.blob(folder_path)
        if not blob.exists():
            # Create a placeholder object
            blob.upload_from_string('', content_type='application/x-www-form-urlencoded;charset=UTF-8')
            log.info(f"✔ Created placeholder: {folder_path}")

def move_single_blob(blob_name):
    """Moves a file from landing to archive_landing with date-based folder."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    
    # Strip 'landing/' and prefix with 'archive_landing/YYYY-MM-DD/'
    relative_path = blob_name.replace(SOURCE_ROOT, "", 1)
    new_name = f"{DEST_BASE_PREFIX}{relative_path}"

    try:
        # 1. Copy to Archive Bucket/Folder
        bucket.copy_blob(blob, bucket, new_name)
        # 2. Delete from landing
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

    # 1. Ensure Country folders don't disappear from landing/
    preserve_folder_structure()

    # 2. List all data files in landing/
    log.info(f"Scanning {SOURCE_ROOT} for files...")
    blobs = list(bucket.list_blobs(prefix=SOURCE_ROOT))
    
    # Filter: Move files, ignore folder placeholders and empty folders
    file_names = [
        b.name for b in blobs 
        if not b.name.endswith('/') and b.size > 0
    ]

    if not file_names:
        log.info(f"No data files found in {SOURCE_ROOT} to move.")
        return

    log.info(f"Found {len(file_names)} files. Moving to {DEST_BASE_PREFIX}...")

    # 3. Parallel Execution
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(move_single_blob, name) for name in file_names]
        for future in as_completed(futures):
            res = future.result()
            if "FAILED" in res:
                log.error(res)

    # 4. Final Summary
    print("\n" + "="*60)
    print("             LANDING -> ARCHIVE REPORT")
    print("="*60)
    print(f"Total Files Processed: {len(file_names)}")
    print(f"Successfully Archived: {success_count}")
    print(f"Failed to Move:        {failed_count}")
    
    if failed_count > 0:
        print("\nERRORS:")
        for error in failed_files:
            print(f"  - {error}")
    
    print("\nSource:  gs://mobile_brands/landing/")
    print(f"Archive: gs://mobile_brands/{DEST_BASE_PREFIX}")
    print("="*60)

if __name__ == "__main__":
    main()