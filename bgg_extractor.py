import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
import csv
import os
import time
import sqlite3
import hashlib
import json
from collections import defaultdict
from tqdm import tqdm
from google.cloud import storage, firestore
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
MY_BGG_TOKEN = os.getenv("BGG_TOKEN")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "boardgames")
CACHE_DB = os.getenv("CACHE_DB", "bgg_sync_cache.sqlite")
PROGRESS_FILE = "extractor_progress.txt"

# Use CURR_DATE from env if set (for workflow timezone alignment), else local/UTC
CURRENT_DATE = os.getenv("CURR_DATE") or datetime.now().strftime("%Y-%m-%d")
MASTER_LIST_FILENAME = f"bgg_master_list_{CURRENT_DATE}.csv"

CHUNK_SIZE = 20 
SLEEP_SUCCESS = 5  # BGG recommends 5s between requests
SLEEP_FAIL = 60   # Longer backoff on 401/rate limit

# Supported languages for localized maps
LANGUAGES = ["en", "ko", "de", "es", "fr", "ja", "ru", "zh"]

storage_client = storage.Client(project=PROJECT_ID)
db = firestore.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)

# --- HELPER FUNCTIONS ---

def download_files_from_gcs():
    """Syncs the master list and cache DB from GCS for stateless environments."""
    print("📥 Syncing files from Google Cloud Storage...")
    try:
        blob = bucket.blob(MASTER_LIST_FILENAME)
        blob.download_to_filename(MASTER_LIST_FILENAME)
        print(f"  ✅ Downloaded {MASTER_LIST_FILENAME}")
    except Exception as e:
        print(f"❌ Failed to download {MASTER_LIST_FILENAME}. Error: {e}")
        return False

    try:
        cache_blob = bucket.blob(CACHE_DB)
        if cache_blob.exists():
            cache_blob.download_to_filename(CACHE_DB)
            print(f"  ✅ Downloaded existing cache database: {CACHE_DB}")
    except Exception as e:
        print(f"⚠️ No cache found or error downloading: {e}")
    return True

def upload_cache_to_gcs():
    """Saves the SQLite cache back to GCS."""
    print(f"\n☁️ Uploading updated {CACHE_DB} to Firebase Storage...")
    try:
        blob = bucket.blob(CACHE_DB)
        blob.upload_from_filename(CACHE_DB)
        print("✅ Cache DB successfully secured in the cloud!")
    except Exception as e:
        print(f"❌ Error uploading cache: {e}")

def get_progress():
    """Reads the last successful chunk index from GCS."""
    try:
        blob = bucket.blob(PROGRESS_FILE)
        return int(blob.download_as_text())
    except:
        return 0

def save_progress(idx):
    """Saves the current chunk index to GCS."""
    try:
        bucket.blob(PROGRESS_FILE).upload_from_string(str(idx))
    except Exception as e:
        print(f"⚠️ Failed to save progress: {e}")

def init_cache():
    conn = sqlite3.connect(CACHE_DB)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS game_hashes (bgg_id TEXT PRIMARY KEY, hash TEXT)''')
    conn.commit()
    # Migrate old schema: if table exists with wrong columns (e.g. no "hash"), recreate
    try:
        c.execute("SELECT hash FROM game_hashes LIMIT 1")
    except sqlite3.OperationalError as e:
        if "no such column" in str(e).lower():
            c.execute("DROP TABLE IF EXISTS game_hashes")
            c.execute('''CREATE TABLE game_hashes (bgg_id TEXT PRIMARY KEY, hash TEXT)''')
            conn.commit()
        else:
            raise
    return conn

def get_cached_hash(c, bgg_id):
    c.execute("SELECT hash FROM game_hashes WHERE bgg_id=?", (str(bgg_id),))
    row = c.fetchone()
    return row[0] if row else None

def update_cache_hash(c, bgg_id, new_hash):
    c.execute("REPLACE INTO game_hashes (bgg_id, hash) VALUES (?, ?)", (str(bgg_id), new_hash))

def generate_hash(data_dict):
    """Generates a hash of the content, excluding the server timestamp."""
    hash_data = {k: v for k, v in data_dict.items() if k != 'updated_at'}
    data_str = json.dumps(hash_data, sort_keys=True)
    return hashlib.md5(data_str.encode('utf-8')).hexdigest()

def generate_localized_dict(content, default_lang="en"):
    """Initializes the map structure for 8 languages."""
    ldict = {lang: "" for lang in LANGUAGES}
    ldict[default_lang] = content if content else ""
    return ldict


def build_games_with_parents(games):
    """
    Group master list rows by bgg_id, collecting all parent relationships.
    Dedupes by parent_id (same parent can appear with different names; keep one per parent_id).
    Returns list of dicts: [{bgg_id, name, is_expansion, parents: [{parent_id, parent_name}, ...]}, ...]
    """
    by_id = defaultdict(lambda: {"name": "", "is_expansion": False, "parents_by_id": {}})  # pid -> best pname
    for row in games:
        bid = str(row["bgg_id"])
        by_id[bid]["name"] = row.get("name", "")
        if row.get("is_expansion") == "True":
            by_id[bid]["is_expansion"] = True
        pid = str(row.get("parent_id", "")).strip()
        pname = str(row.get("parent_name", "")).strip()
        if pid:
            # Keep longest name per parent_id (prefer full title over short)
            existing = by_id[bid]["parents_by_id"].get(pid, "")
            if len(pname) > len(existing):
                by_id[bid]["parents_by_id"][pid] = pname

    return [
        {
            "bgg_id": bid,
            "name": info["name"],
            "is_expansion": info["is_expansion"],
            "parents": [{"parent_id": p, "parent_name": n} for p, n in info["parents_by_id"].items()],
        }
        for bid, info in by_id.items()
    ]


# --- MAIN EXECUTION ---

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="Reset progress (use after schema change, e.g. multi-parent)")
    args = parser.parse_args()

    if not download_files_from_gcs():
        return

    if args.reset:
        try:
            bucket.blob(PROGRESS_FILE).upload_from_string("0")
            print("  🔄 Progress reset to 0")
        except Exception as e:
            print(f"  ⚠️ Could not reset progress: {e}")

    conn = init_cache()
    c = conn.cursor()

    # Read the master list (populated by bgg_csv.py)
    games = []
    with open(MASTER_LIST_FILENAME, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        games = list(reader)

    # Group by bgg_id so expansions with multiple parents get all relationships
    games = build_games_with_parents(games)
    print(f"  Master list: {len(games):,} unique games (multi-parent merged)")

    # Calculate chunks and find where we left off
    start_chunk_idx = get_progress()
    chunks = [games[i:i + CHUNK_SIZE] for i in range(0, len(games), CHUNK_SIZE)]
    
    print(f"🔄 Resuming from chunk {start_chunk_idx} of {len(chunks)}")

    batch = db.batch()
    batch_count = 0
    updates_made = 0
    skipped_count = 0

    for i, chunk in enumerate(tqdm(chunks, desc="Syncing")):
        if i < start_chunk_idx:
            continue
            
        bgg_ids = [game['bgg_id'] for game in chunk]
        chunk_dict = {str(game['bgg_id']): game for game in chunk}
        url = f"https://boardgamegeek.com/xmlapi2/thing?id={','.join(bgg_ids)}&stats=1"
        
        headers = {
            'User-Agent': 'BoardGameCatalog/1.0 (https://boardgamegeek.com/applications)',
        }
        bgg_token = os.getenv("BGG_TOKEN")
        if bgg_token:
            headers['Authorization'] = f'Bearer {bgg_token}'
        
        success = False
        for attempt in range(3):
            try:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req) as response:
                    root = ET.fromstring(response.read())
                
                for item in root.findall('item'):
                    bgg_id = item.get('id')
                    csv_row = chunk_dict.get(bgg_id, {})
                    parents = csv_row.get('parents', [])

                    # Extract Data
                    title_en = item.find("name[@type='primary']").get('value') if item.find("name[@type='primary']") is not None else "Unknown"
                    desc_en = item.find('description').text if item.find('description') is not None else ""
                    summary_en = desc_en[:250] + "..." if len(desc_en) > 250 else desc_en
                    
                    stats = item.find('statistics/ratings')
                    rating = round(float(stats.find('average').get('value')), 2) if stats is not None else 0.0
                    weight = round(float(stats.find('averageweight').get('value')), 2) if stats is not None else 0.0

                    doc_data = {
                        "bgg_id": bgg_id,
                        "title": generate_localized_dict(title_en),
                        "description": generate_localized_dict(desc_en),
                        "summary_description": generate_localized_dict(summary_en),
                        "year_published": int(item.find('yearpublished').get('value')) if item.find('yearpublished') is not None else 0,
                        "min_players": int(item.find('minplayers').get('value')) if item.find('minplayers') is not None else 0,
                        "max_players": int(item.find('maxplayers').get('value')) if item.find('maxplayers') is not None else 0,
                        "rating": rating,
                        "weight": weight,
                        "image_url": item.find('thumbnail').text if item.find('thumbnail') is not None else "",
                        "is_expansion": csv_row.get('is_expansion', False),
                        "parents": parents,
                        "updated_at": firestore.SERVER_TIMESTAMP
                    }

                    # Hash Check
                    current_hash = generate_hash(doc_data)
                    if current_hash != get_cached_hash(c, bgg_id):
                        doc_ref = db.collection(COLLECTION_NAME).document(bgg_id)
                        batch.set(doc_ref, doc_data, merge=True)
                        update_cache_hash(c, bgg_id, current_hash)
                        batch_count += 1
                        updates_made += 1
                        
                        if batch_count >= 400:
                            batch.commit()
                            conn.commit()
                            save_progress(i) # Checkpoint
                            batch = db.batch()
                            batch_count = 0
                    else:
                        skipped_count += 1

                success = True
                break
            except Exception as e:
                tqdm.write(f"⚠️ Attempt {attempt+1} failed: {e}")
                time.sleep(SLEEP_FAIL)

        if success:
            time.sleep(SLEEP_SUCCESS)

    # Final commit
    if batch_count > 0:
        batch.commit()
        conn.commit()
        save_progress(len(chunks)) # Mark as 100% complete

    conn.close()
    upload_cache_to_gcs()
    print(f"\n✅ Extractor Finished. Updated: {updates_made}, Skipped: {skipped_count}")

if __name__ == "__main__":
    main()