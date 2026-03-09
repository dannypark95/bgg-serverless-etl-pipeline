import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
import csv
import os
import time
from tqdm import tqdm
from google.cloud import storage
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")
# It looks for the token in your .env, but falls back to your hardcoded one
MY_BGG_TOKEN = os.getenv("BGG_TOKEN", "24b17ef8-b80d-4df1-9f4f-c6a503fbdeeb") 

CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")
RAW_DUMP_FILENAME = f"bg_ranks_raw_{CURRENT_DATE}.csv" 
MASTER_LIST_FILENAME = f"bgg_master_list_{CURRENT_DATE}.csv"

# Matching your successful script exactly
CHUNK_SIZE = 20 
SLEEP_SUCCESS = 4
SLEEP_FAIL = 10

storage_client = storage.Client(project=PROJECT_ID)

def download_raw_from_gcs():
    print(f"📥 Downloading raw dump {RAW_DUMP_FILENAME} from Firebase...")
    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(RAW_DUMP_FILENAME)
        blob.download_to_filename(RAW_DUMP_FILENAME)
        return True
    except Exception as e:
        print(f"❌ Could not download {RAW_DUMP_FILENAME}. Error: {e}")
        return False

def extract_base_games():
    base_games_list = []
    print("📊 Extracting highly-rated base games from the raw dump...")
    with open(RAW_DUMP_FILENAME, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('is_expansion') == '1' or not row.get('rank') or row.get('rank') == '0':
                continue
            if row.get('id') and row.get('name'):
                base_games_list.append({'id': row.get('id'), 'name': row.get('name')})
    return base_games_list

def fetch_expansions_sync(base_games_list):
    expansions = []
    chunks = [base_games_list[i:i + CHUNK_SIZE] for i in range(0, len(base_games_list), CHUNK_SIZE)]
    
    # Exact headers from your working script
    headers = {
        'User-Agent': 'Mozilla/5.0', 
        'Authorization': f'Bearer {MY_BGG_TOKEN}'
    }

    print(f"\n🔍 Querying API for expansions across {len(base_games_list)} base games...")
    
    # We use standard tqdm since we aren't using asyncio anymore
    for chunk in tqdm(chunks, desc="Finding Expansions"):
        base_ids = [game['id'] for game in chunk]
        
        # Build URL using urllib exactly like your working script
        params = {"id": ",".join(base_ids)}
        url = f"https://boardgamegeek.com/xmlapi2/thing?{urllib.parse.urlencode(params)}"
        
        success = False
        for attempt in range(3):
            try:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req) as response:
                    xml_data = response.read()
                    
                try:
                    root = ET.fromstring(xml_data)
                except ET.ParseError:
                    tqdm.write(f"⚠️ XML Parse Error on chunk starting with ID {base_ids[0]}")
                    break

                for item in root.findall('item'):
                    parent_id = item.get('id')
                    parent_name = next((g['name'] for g in chunk if g['id'] == parent_id), "Unknown")
                    
                    for link in item.findall("link[@type='boardgameexpansion']"):
                        if link.get('inbound') != 'true':
                            expansions.append({
                                'bgg_id': link.get('id'),
                                'parent_id': parent_id,
                                'parent_name': parent_name,
                                'is_expansion': 'True'
                            })
                success = True
                break  # Success! Break out of the retry loop.
                    
            except Exception as e:
                tqdm.write(f"⚠️ API Error on attempt {attempt+1}: {e}")
                time.sleep(SLEEP_FAIL)

        # Apply your exact sleep logic
        if success:
            time.sleep(SLEEP_SUCCESS)
        else:
            tqdm.write(f"❌ Failed to process chunk starting with ID {base_ids[0]}. Skipping.")

    return expansions

def upload_master_to_gcs():
    print(f"\n☁️ Uploading {MASTER_LIST_FILENAME} to Firebase Storage...")
    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(MASTER_LIST_FILENAME)
        blob.upload_from_filename(MASTER_LIST_FILENAME)
        print("✅ Master List successfully uploaded!")
    except Exception as e:
        print(f"❌ Error uploading: {e}")

def main():
    if not download_raw_from_gcs(): 
        return
        
    base_games_list = extract_base_games()
    all_expansions = fetch_expansions_sync(base_games_list)

    print(f"\n💾 Saving Master List ({len(base_games_list)} Bases, {len(all_expansions)} Expansions)...")
    with open(MASTER_LIST_FILENAME, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['bgg_id', 'parent_id', 'parent_name', 'is_expansion'])
        for bg in base_games_list: 
            writer.writerow([bg['id'], '', '', 'False'])
        for exp in all_expansions: 
            writer.writerow([exp['bgg_id'], exp['parent_id'], exp['parent_name'], exp['is_expansion']])

    upload_master_to_gcs()

    # Clean up local raw dump
    if os.path.exists(RAW_DUMP_FILENAME): 
        os.remove(RAW_DUMP_FILENAME)
        
    print(f"\n📁 Local copy successfully saved as: {MASTER_LIST_FILENAME}")

if __name__ == "__main__":
    main()