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

PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "boardgames")
CACHE_DB = os.getenv("CACHE_DB", "bgg_sync_cache.sqlite")

CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
MASTER_LIST_FILENAME = f"bgg_master_list_{CURRENT_DATE}.csv"

CHUNK_SIZE = 20 
SLEEP_SUCCESS = 2.5
SLEEP_FAIL = 10

storage_client = storage.Client(project=PROJECT_ID)
db = firestore.Client(project=PROJECT_ID)

def download_files_from_gcs():
    """Downloads the master list and the cache DB from Cloud Storage since containers are stateless."""
    print("📥 Syncing files from Google Cloud Storage...")
    bucket = storage_client.bucket(BUCKET_NAME)
    
    try:
        blob = bucket.blob(MASTER_LIST_FILENAME)
        blob.download_to_filename(MASTER_LIST_FILENAME)
        print(f"  ✅ Downloaded {MASTER_LIST_FILENAME}")
    except Exception as e:
        print(f"❌ Failed to download {MASTER_LIST_FILENAME}. Did the CSV job finish? Error: {e}")
        return False

    try:
        cache_blob = bucket.blob(CACHE_DB)
        if cache_blob.exists():
            cache_blob.download_to_filename(CACHE_DB)
            print(f"  ✅ Downloaded existing cache database: {CACHE_DB}")
        else:
            print(f"  ℹ️ No cache found. Starting fresh cache.")
    except Exception as e:
        print(f"⚠️ Could not download cache DB: {e}")
        
    return True

def upload_cache_to_gcs():
    """Saves the updated SQLite cache back to the cloud."""
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
    # Remove timestamps before hashing so it only triggers on real data changes
    hash_data = {k: v for k, v in data_dict.items() if k != 'updated_at'}
    data_str = json.dumps(hash_data, sort_keys=True)
    return hashlib.md5(data_str.encode('utf-8')).hexdigest()

def main():
    if not PROJECT_ID:
        print("❌ Error: PROJECT_ID is missing from your .env file.")
        return

    print("=" * 55)
    print(" 🚀 STARTING BGG DATA EXTRACTOR")
    print(f" 🔌 Target Database : {PROJECT_ID}")
    print("=" * 55 + "\n")

    if not download_files_from_gcs():
        return

    conn = init_cache()
    c = conn.cursor()

    # Read the master list
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
        chunk_dict = {game['bgg_id']: game for game in chunk}
        bgg_ids = list(chunk_dict.keys())
        
        url = f"https://boardgamegeek.com/xmlapi2/thing?id={','.join(bgg_ids)}&stats=1"
        
        success = False
        for attempt in range(3):
            try:
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req) as response:
                    xml_data = response.read()
                    
                root = ET.fromstring(xml_data)
                
                for item in root.findall('item'):
                    bgg_id = item.get('id')
                    csv_row = chunk_dict.get(bgg_id, {})
                    
                    # Safely extract text nodes
                    title_node = item.find("name[@type='primary']")
                    title = title_node.get('value') if title_node is not None else "Unknown"
                    desc_node = item.find('description')
                    description = desc_node.text if desc_node is not None else ""
                    thumb_node = item.find('thumbnail')
                    thumbnail_url = thumb_node.text if thumb_node is not None else ""
                    image_node = item.find('image')
                    image_url = image_node.text if image_node is not None else ""
                    
                    # Safely extract numbers
                    year_pub = item.find('yearpublished')
                    min_pl = item.find('minplayers')
                    max_pl = item.find('maxplayers')
                    min_time = item.find('minplaytime')
                    max_time = item.find('maxplaytime')
                    age_node = item.find('minage')
                    
                    # Extract stats
                    stats = item.find('statistics/ratings')
                    rating = 0.0
                    weight = 0.0
                    rank = None
                    
                    if stats is not None:
                        rating_node = stats.find('average')
                        rating = round(float(rating_node.get('value')), 2) if rating_node is not None else 0.0
                        weight_node = stats.find('averageweight')
                        weight = round(float(weight_node.get('value')), 2) if weight_node is not None else 0.0
                        
                        rank_node = stats.find("ranks/rank[@name='boardgame']")
                        if rank_node is not None and rank_node.get('value') != 'Not Ranked':
                            rank = int(rank_node.get('value'))

                    # Build the Firestore document
                    doc_data = {
                        "bgg_id": bgg_id,
                        "title": title,
                        "title_ko": title, # Default to English, scraper will fix this later
                        "year_published": int(year_pub.get('value')) if year_pub is not None else 0,
                        "min_players": int(min_pl.get('value')) if min_pl is not None else 0,
                        "max_players": int(max_pl.get('value')) if max_pl is not None else 0,
                        "min_playtime": int(min_time.get('value')) if min_time is not None else 0,
                        "max_playtime": int(max_time.get('value')) if max_time is not None else 0,
                        "age": int(age_node.get('value')) if age_node is not None else 0,
                        "description": description,
                        "summary_description": description[:250] + "..." if description and len(description) > 250 else description,
                        "image_url": thumbnail_url,
                        "image_url_original": image_url,
                        "rating": rating,
                        "weight": weight,
                        "rank_overall": rank,
                        "category": [l.get('value') for l in item.findall("link[@type='boardgamecategory']")],
                        "mechanic": [l.get('value') for l in item.findall("link[@type='boardgamemechanic']")],
                        "designer": [l.get('value') for l in item.findall("link[@type='boardgamedesigner']")],
                        "publisher": [l.get('value') for l in item.findall("link[@type='boardgamepublisher']")],
                        "artist": [l.get('value') for l in item.findall("link[@type='boardgameartist']")],
                        "expansions": [{"id": l.get('id'), "name": l.get('value')} for l in item.findall("link[@type='boardgameexpansion']") if l.get('inbound') != 'true'],
                        "url": f"https://boardgamegeek.com/boardgame/{bgg_id}",
                        "is_expansion": csv_row.get('is_expansion') == 'True',
                        "parent_id": csv_row.get('parent_id') if csv_row.get('parent_id') else None,
                        "parent_name": csv_row.get('parent_name') if csv_row.get('parent_name') else None,
                        "updated_at": firestore.SERVER_TIMESTAMP
                    }

                    # Check MD5 Hash to save Firestore Writes
                    current_hash = generate_hash(doc_data)
                    cached_hash = get_cached_hash(c, bgg_id)
                    
                    if current_hash != cached_hash:
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
                tqdm.write(f"⚠️ Error on chunk: {e}. Retrying in {SLEEP_FAIL}s...")
                time.sleep(SLEEP_FAIL)

        if success:
            time.sleep(SLEEP_SUCCESS)

    if batch_count > 0:
        batch.commit()
        conn.commit()

    conn.close()
    
    print("\n" + "=" * 55)
    print(" 🎉 EXTRACTOR COMPLETE")
    print("=" * 55)
    print(f"  🟢 Total Updates Written : {updates_made}")
    print(f"  🟡 Cached (Skipped)      : {skipped_count}")
    print("=" * 55 + "\n")

    upload_cache_to_gcs()

if __name__ == "__main__":
    main()