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

# Use CURR_DATE from env if set (for workflow timezone alignment), else local/UTC
CURRENT_DATE = os.getenv("CURR_DATE") or datetime.now().strftime("%Y-%m-%d")
RAW_DUMP_FILENAME = f"bg_ranks_raw_{CURRENT_DATE}.csv" 
MASTER_LIST_FILENAME = f"bgg_master_list_{CURRENT_DATE}.csv"
# Cloud Run writable temporary directory
LOCAL_RAW_PATH = os.path.join("/tmp", RAW_DUMP_FILENAME)
LOCAL_MASTER_PATH = os.path.join("/tmp", "temp_master.csv")

storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)

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

    base_games = []
    print("🚀 Processing CSV...")
    
    with open(LOCAL_RAW_PATH, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            # Filter Logic: Keep ranked base games, skip expansions/unranked
            if row.get('is_expansion') == '1' or not row.get('rank') or row.get('rank') == '0':
                continue
            
            base_games.append({
                'bgg_id': row.get('id'),
                'parent_id': '',
                'parent_name': '',
                'is_expansion': 'False'
            })

    # Write results to the absolute /tmp path
    print(f"✍️ Writing filtered data to {LOCAL_MASTER_PATH}...")
    with open(LOCAL_MASTER_PATH, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['bgg_id', 'parent_id', 'parent_name', 'is_expansion'])
        writer.writeheader()
        writer.writerows(base_games)

    # Upload final file to GCS (with retry for 429 rate limit)
    if os.path.exists(LOCAL_MASTER_PATH):
        print(f"📤 Uploading {MASTER_LIST_FILENAME} to Cloud Storage...")
        blob = bucket.blob(MASTER_LIST_FILENAME)
        for attempt in range(3):
            try:
                blob.upload_from_filename(LOCAL_MASTER_PATH)
                break
            except Exception as e:
                if ("429" in str(e) or "rate limit" in str(e).lower()) and attempt < 2:
                    wait = 30 * (attempt + 1)
                    print(f"⚠️ Rate limited, retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    raise
        print("🎉 CSV Filtering complete.")
    else:
        print("❌ Error: Temp file was not created.")

if __name__ == "__main__":
    main()