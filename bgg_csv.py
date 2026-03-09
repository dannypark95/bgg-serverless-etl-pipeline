import pandas as pd
import os
from google.cloud import storage
from datetime import datetime

# --- CONFIG ---
PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
RAW_DUMP_FILENAME = f"bg_ranks_raw_{CURRENT_DATE}.csv"
MASTER_LIST_FILENAME = f"bgg_master_list_{CURRENT_DATE}.csv"
CHECKPOINT_FILE = "csv_progress.txt"
RATING_THRESHOLD = 73

storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)

def get_checkpoint():
    try:
        blob = bucket.blob(CHECKPOINT_FILE)
        return int(blob.download_as_text())
    except:
        return 0

def save_checkpoint(row_index):
    bucket.blob(CHECKPOINT_FILE).upload_from_string(str(row_index))

def extract_logic():
    start_row = get_checkpoint()
    print(f"🚀 Processing CSV from row: {start_row}")

    # Ensure the master list exists (append mode)
    local_master = "temp_master.csv"
    mode = 'a' if start_row > 0 else 'w'
    header = True if start_row == 0 else False

    # Download raw file locally (assuming it's in GCS)
    if not os.path.exists(RAW_DUMP_FILENAME):
        bucket.blob(RAW_DUMP_FILENAME).download_to_filename(RAW_DUMP_FILENAME)

    chunk_size = 20000
    current_row = 0
    
    # Process in chunks to stay under memory and time limits
    for chunk in pd.read_csv(RAW_DUMP_FILENAME, chunksize=chunk_size):
        current_row += len(chunk)
        if current_row <= start_row:
            continue
            
        # 73-rating threshold + Base Games only
        mask = (chunk['is_expansion'] == 0) & ((chunk['rank'] > 0) | (chunk['usersrated'] >= RATING_THRESHOLD))
        filtered = chunk[mask][['id', 'name']].copy()
        filtered.columns = ['bgg_id', 'name']
        
        # Save chunk
        filtered.to_csv(local_master, mode=mode, header=header, index=False)
        mode = 'a'
        header = False
        
        # Checkpoint every chunk
        save_checkpoint(current_row)
        print(f"✅ Checkpoint at row {current_row}")

    # Upload final result
    bucket.blob(MASTER_LIST_FILENAME).upload_from_filename(local_master)
    print("🎉 CSV Filtering complete.")

if __name__ == "__main__":
    extract_logic()