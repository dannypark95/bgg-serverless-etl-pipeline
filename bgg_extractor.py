import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
import csv
import os
import time
import sqlite3
import hashlib
import json
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

CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
MASTER_LIST_FILENAME = f"bgg_master_list_{CURRENT_DATE}.csv"

CHUNK_SIZE = 20 
SLEEP_SUCCESS = 2.5
SLEEP_FAIL = 10

# Supported languages for our dictionary structure
LANGUAGES = ["en", "ko", "de", "es", "fr", "ja", "ru", "zh"]

storage_client = storage.Client(project=PROJECT_ID)
db = firestore.Client(project=PROJECT_ID)

def download_files_from_gcs():
    """Syncs the master list and cache DB from Cloud Storage for stateless environments."""
    print("📥 Syncing files from Google Cloud Storage...")
    bucket = storage_client.bucket(BUCKET_NAME)
    
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
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(CACHE_DB)
        blob.upload_from_filename(CACHE_DB)
        print("✅ Cache DB successfully secured in the cloud!")
    except Exception as e:
        print(f"❌ Error uploading cache: {e}")

def init_cache():
    conn = sqlite3.connect(CACHE_DB)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS game_hashes (bgg_id TEXT PRIMARY KEY, hash TEXT)''')
    conn.commit()
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

def main():
    if not download_files_from_gcs():
        return

    conn = init_cache()
    c = conn.cursor()

    # Read the master list (filtered by our 73-rating logic)
    games_to_process = []
    with open(MASTER_LIST_FILENAME, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            games_to_process.append(row)

    chunks = [games_to_process[i:i + CHUNK_SIZE] for i in range(0, len(games_to_process), CHUNK_SIZE)]
    print(f"\n📊 Processing {len(games_to_process)} games in {len(chunks)} chunks...")

    batch = db.batch()
    batch_count = 0
    updates_made = 0
    skipped_count = 0

    for chunk in tqdm(chunks, desc="Syncing with Firestore"):
        bgg_ids = [game['bgg_id'] for game in chunk]
        url = f"https://boardgamegeek.com/xmlapi2/thing?id={','.join(bgg_ids)}&stats=1"
        
        success = False
        for attempt in range(3):
            try:
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req) as response:
                    root = ET.fromstring(response.read())
                
                for item in root.findall('item'):
                    bgg_id = item.get('id')
                    
                    # Core Data Extraction
                    title_en = item.find("name[@type='primary']").get('value') if item.find("name[@type='primary']") is not None else "Unknown"
                    desc_en = item.find('description').text if item.find('description') is not None else ""
                    
                    stats = item.find('statistics/ratings')
                    rating = round(float(stats.find('average').get('value')), 2) if stats is not None else 0.0
                    weight = round(float(stats.find('averageweight').get('value')), 2) if stats is not None else 0.0

                    # NEW: Build Localized Map Structure
                    doc_data = {
                        "bgg_id": bgg_id,
                        "title": generate_localized_dict(title_en),
                        "description": generate_localized_dict(desc_en),
                        "summary_description": generate_localized_dict(desc_en[:250] + "..." if len(desc_en) > 250 else desc_en),
                        "year_published": int(item.find('yearpublished').get('value')) if item.find('yearpublished') is not None else 0,
                        "min_players": int(item.find('minplayers').get('value')) if item.find('minplayers') is not None else 0,
                        "max_players": int(item.find('maxplayers').get('value')) if item.find('maxplayers') is not None else 0,
                        "rating": rating,
                        "weight": weight,
                        "image_url": item.find('thumbnail').text if item.find('thumbnail') is not None else "",
                        "updated_at": firestore.SERVER_TIMESTAMP
                    }

                    # Use MD5 Hash to prevent redundant Firestore writes
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
                            batch = db.batch()
                            batch_count = 0
                    else:
                        skipped_count += 1

                success = True
                break
            except Exception as e:
                tqdm.write(f"⚠️ Error: {e}. Retrying...")
                time.sleep(SLEEP_FAIL)

        if success:
            time.sleep(SLEEP_SUCCESS)

    if batch_count > 0:
        batch.commit()
        conn.commit()

    conn.close()
    upload_cache_to_gcs()
    print(f"\n✅ Done! Updated: {updates_made}, Cached: {skipped_count}")

if __name__ == "__main__":
    main()