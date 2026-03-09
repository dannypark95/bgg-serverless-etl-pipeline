import urllib.request
import urllib.parse
import csv
import os
import time
from google.cloud import storage
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")

if not PROJECT_ID or not BUCKET_NAME:
    raise ValueError("Required env vars PROJECT_ID and BUCKET_NAME must be set")

CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
RAW_DUMP_FILENAME = f"bg_ranks_raw_{CURRENT_DATE}.csv" 
MASTER_LIST_FILENAME = f"bgg_master_list_{CURRENT_DATE}.csv"
CHECKPOINT_FILE = "csv_progress.txt"

# Cloud Run writable temporary directory
LOCAL_RAW_PATH = os.path.join("/tmp", RAW_DUMP_FILENAME)
LOCAL_MASTER_PATH = os.path.join("/tmp", "temp_master.csv")

# Optimization: Save checkpoint every 50k rows to avoid GCS 429 errors
CHECKPOINT_INTERVAL = 50000 

storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)

def get_checkpoint():
    try:
        blob = bucket.blob(CHECKPOINT_FILE)
        if blob.exists():
            return int(blob.download_as_text())
    except Exception:
        pass
    return 0

def save_checkpoint(index):
    try:
        # Saving to Cloud Storage
        bucket.blob(CHECKPOINT_FILE).upload_from_string(str(index))
    except Exception as e:
        print(f"⚠️ Checkpoint upload failed (likely rate limit): {e}")

def download_raw_from_gcs():
    print(f"📥 Downloading {RAW_DUMP_FILENAME} to {LOCAL_RAW_PATH}...")
    try:
        blob = bucket.blob(RAW_DUMP_FILENAME)
        blob.download_to_filename(LOCAL_RAW_PATH)
        return True
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return False

def main():
    if not download_raw_from_gcs():
        return

    start_row = get_checkpoint()
    base_games = []
    
    print(f"🚀 Processing CSV from row: {start_row}")
    
    with open(LOCAL_RAW_PATH, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i < start_row:
                continue
            
            # Filter Logic: Keep ranked base games, skip expansions/unranked
            if row.get('is_expansion') == '1' or not row.get('rank') or row.get('rank') == '0':
                continue
            
            base_games.append({
                'bgg_id': row.get('id'),
                'parent_id': '',
                'parent_name': '',
                'is_expansion': 'False'
            })

            # Save progress at intervals to survive crashes
            if i > 0 and i % CHECKPOINT_INTERVAL == 0:
                save_checkpoint(i)
                print(f"✅ Checkpoint at row {i}")

    # Write results to the absolute /tmp path
    print(f"✍️ Writing filtered data to {LOCAL_MASTER_PATH}...")
    with open(LOCAL_MASTER_PATH, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['bgg_id', 'parent_id', 'parent_name', 'is_expansion'])
        writer.writeheader()
        writer.writerows(base_games)

    # Upload final file to GCS
    if os.path.exists(LOCAL_MASTER_PATH):
        print(f"📤 Uploading {MASTER_LIST_FILENAME} to Cloud Storage...")
        bucket.blob(MASTER_LIST_FILENAME).upload_from_filename(LOCAL_MASTER_PATH)
        # Clear checkpoint on total success
        bucket.blob(CHECKPOINT_FILE).delete()
        print("🎉 CSV Filtering complete.")
    else:
        print("❌ Error: Temp file was not created.")

if __name__ == "__main__":
    main()