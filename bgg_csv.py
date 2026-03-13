import json
import pandas as pd
import os
import time
import urllib.request
import xml.etree.ElementTree as ET
from google.cloud import storage
from google.cloud.exceptions import NotFound
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# --- CONFIG ---
PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME")

if not PROJECT_ID or not BUCKET_NAME:
    raise ValueError("Required env vars PROJECT_ID and BUCKET_NAME must be set")

# Use CURR_DATE from env if set (for workflow timezone alignment), else local/UTC
CURRENT_DATE = os.getenv("CURR_DATE") or datetime.now().strftime("%Y-%m-%d")
RAW_DUMP_FILENAME = f"bg_ranks_raw_{CURRENT_DATE}.csv"
MASTER_LIST_FILENAME = f"bgg_master_list_{CURRENT_DATE}.csv"
CHECKPOINT_FILENAME = f"bgg_csv_checkpoint_{CURRENT_DATE}.json"
RATING_THRESHOLD = 73

# BGG API: max 20 items per request, 5s between requests
BGG_CHUNK_SIZE = 20
BGG_SLEEP_SECONDS = 5

# Checkpoint every N base games (saves to GCS)
CHECKPOINT_INTERVAL = 500

# Exit ~5 min before Cloud Run timeout so we can save checkpoint cleanly
TASK_TIMEOUT = int(os.getenv("TASK_TIMEOUT", "3600"))
TIMEOUT_BUFFER = 300

# Cloud Run writable directory
LOCAL_RAW_PATH = os.path.join("/tmp", RAW_DUMP_FILENAME)
LOCAL_MASTER_PATH = os.path.join("/tmp", "temp_master.csv")
LOCAL_CHECKPOINT_PATH = os.path.join("/tmp", "checkpoint.json")

storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)


def download_raw_from_gcs():
    """Download raw dump from GCS if not present."""
    if os.path.exists(LOCAL_RAW_PATH):
        return True
    print(f"📥 Downloading {RAW_DUMP_FILENAME} from GCS...")
    try:
        bucket.blob(RAW_DUMP_FILENAME).download_to_filename(LOCAL_RAW_PATH)
        return True
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return False


def load_checkpoint():
    """Load checkpoint from GCS. Returns (processed_ids, master_list_rows) or (set(), []) if none."""
    try:
        bucket.blob(CHECKPOINT_FILENAME).download_to_filename(LOCAL_CHECKPOINT_PATH)
        with open(LOCAL_CHECKPOINT_PATH, encoding="utf-8") as f:
            data = json.load(f)
        processed = set(str(x) for x in data.get("processed_ids", []))
        rows = data.get("master_list_rows", [])
        print(f"📂 Resuming from checkpoint: {len(processed)} base games done, {len(rows)} total rows")
        return processed, rows
    except NotFound:
        return set(), []
    except Exception as e:
        print(f"⚠️ Could not load checkpoint: {e}")
        return set(), []


def save_checkpoint(processed_ids, master_list_rows):
    """Save checkpoint to GCS."""
    data = {
        "processed_ids": list(processed_ids),
        "master_list_rows": master_list_rows,
    }
    with open(LOCAL_CHECKPOINT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    bucket.blob(CHECKPOINT_FILENAME).upload_from_filename(
        LOCAL_CHECKPOINT_PATH,
        content_type="application/json",
    )
    print(f"  💾 Checkpoint saved ({len(processed_ids)} base games, {len(master_list_rows)} rows)")


def delete_checkpoint():
    """Remove checkpoint from GCS after successful completion."""
    try:
        bucket.blob(CHECKPOINT_FILENAME).delete()
        print("  🗑️ Checkpoint deleted")
    except NotFound:
        pass


def delete_old_checkpoints():
    """Remove checkpoint files from previous dates to avoid accumulating stale data."""
    prefix = "bgg_csv_checkpoint_"
    current = CHECKPOINT_FILENAME
    deleted = 0
    for blob in bucket.list_blobs(prefix=prefix):
        if blob.name != current:
            try:
                blob.delete()
                deleted += 1
                print(f"  🗑️ Deleted old checkpoint: {blob.name}")
            except Exception as e:
                print(f"  ⚠️ Could not delete {blob.name}: {e}")
    if deleted:
        print(f"  Cleaned up {deleted} old checkpoint(s)")


def fetch_expansions_for_base_games(base_games, processed_ids, master_list_rows, start_time, total_base_games):
    """
    Query BGG API for each base game and fetch its expansion links.
    Appends to master_list_rows and updates processed_ids. Saves checkpoint periodically.
    Returns (master_list_rows, processed_ids, completed) where completed=True if all done.
    total_base_games: full count for progress display (e.g. 30730).
    """
    headers = {
        "User-Agent": "BoardGameCatalog/1.0 (https://boardgamegeek.com/applications)",
    }
    bgg_token = os.getenv("BGG_TOKEN")
    if bgg_token:
        headers["Authorization"] = f"Bearer {bgg_token}"

    base_id_to_name = {str(g["bgg_id"]): g["name"] for g in base_games}
    total = total_base_games
    last_checkpoint_at = len(processed_ids)

    for i in range(0, len(base_games), BGG_CHUNK_SIZE):
        # Timeout check: exit early so we can save checkpoint
        if time.time() - start_time > (TASK_TIMEOUT - TIMEOUT_BUFFER):
            print(f"⏳ Approaching timeout. Saving checkpoint and exiting. Next run will resume.")
            save_checkpoint(processed_ids, master_list_rows)
            return master_list_rows, processed_ids, False

        chunk = base_games[i : i + BGG_CHUNK_SIZE]
        bgg_ids = [str(g["bgg_id"]) for g in chunk]
        url = f"https://boardgamegeek.com/xmlapi2/thing?id={','.join(bgg_ids)}&stats=1"

        for attempt in range(3):
            try:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req) as response:
                    root = ET.fromstring(response.read())

                for item in root.findall("item"):
                    base_id = item.get("id")
                    base_name_elem = item.find("name[@type='primary']")
                    base_name = (
                        base_name_elem.get("value")
                        if base_name_elem is not None
                        else base_id_to_name.get(base_id, "Unknown")
                    )

                    # Add base game
                    master_list_rows.append(
                        {
                            "bgg_id": base_id,
                            "name": base_name,
                            "parent_id": "",
                            "parent_name": "",
                            "is_expansion": "False",
                        }
                    )
                    processed_ids.add(base_id)

                    # Add expansions from links
                    for link in item.findall("link"):
                        if link.get("type") == "boardgameexpansion":
                            exp_id = link.get("id")
                            exp_name = link.get("value", "Unknown")
                            if exp_id:
                                master_list_rows.append(
                                    {
                                        "bgg_id": exp_id,
                                        "name": exp_name,
                                        "parent_id": base_id,
                                        "parent_name": base_name,
                                        "is_expansion": "True",
                                    }
                                )

                break  # success
            except Exception as e:
                print(f"⚠️ BGG API attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    time.sleep(10)
                else:
                    raise

        time.sleep(BGG_SLEEP_SECONDS)
        done = len(processed_ids)
        if done % 100 == 0 or done >= total:
            print(f"  ✅ Fetched expansions for {done}/{total} base games...")

        # Periodic checkpoint
        if done - last_checkpoint_at >= CHECKPOINT_INTERVAL:
            save_checkpoint(processed_ids, master_list_rows)
            last_checkpoint_at = done

    return master_list_rows, processed_ids, True


def extract_logic():
    if not download_raw_from_gcs():
        return

    # Clean up checkpoints from previous dates
    delete_old_checkpoints()

    print("🚀 Step 1: Filtering base games from CSV...")

    df = pd.read_csv(LOCAL_RAW_PATH)

    # Base games only: is_expansion == 0, (rank > 0 OR usersrated >= 73)
    mask = (df["is_expansion"] == 0) & (
        (df["rank"] > 0) | (df["usersrated"] >= RATING_THRESHOLD)
    )
    base_games = df.loc[mask, ["id", "name"]].copy()
    base_games.columns = ["bgg_id", "name"]
    base_games = base_games.to_dict("records")

    print(f"  Found {len(base_games)} base games meeting threshold.")

    # Load checkpoint if resuming
    processed_ids, master_list_rows = load_checkpoint()
    base_games_to_process = [g for g in base_games if str(g["bgg_id"]) not in processed_ids]

    # Sample run: limit base games to process (e.g. workflow passes SAMPLE_MAX_BASE_GAMES=50)
    sample_max = os.getenv("SAMPLE_MAX_BASE_GAMES")
    if sample_max:
        n = int(sample_max)
        base_games_to_process = base_games_to_process[:n]
        print(f"⚠️ SAMPLE MODE: limiting to first {len(base_games_to_process)} base games (SAMPLE_MAX_BASE_GAMES={sample_max})")

    if not base_games_to_process and len(master_list_rows) > 0:
        # All done - write final CSV
        print("  All base games already processed (from checkpoint).")
    elif base_games_to_process:
        print(f"🚀 Step 2: Fetching expansion links from BGG API ({len(base_games_to_process)} remaining)...")
        start_time = time.time()
        master_list_rows, processed_ids, completed = fetch_expansions_for_base_games(
            base_games_to_process, processed_ids, master_list_rows, start_time, len(base_games)
        )
        if not completed:
            print("⏸️ Saved checkpoint. Re-run the job to continue.")
            return

    print(f"  Master list: {len(master_list_rows)} items (base + expansions)")

    # Write to temp file
    filtered_df = pd.DataFrame(master_list_rows)
    filtered_df.to_csv(LOCAL_MASTER_PATH, index=False)
    print(f"✍️ Writing to {LOCAL_MASTER_PATH}...")

    # Upload to GCS
    print(f"📤 Uploading {MASTER_LIST_FILENAME} to Cloud Storage...")
    bucket.blob(MASTER_LIST_FILENAME).upload_from_filename(LOCAL_MASTER_PATH)
    delete_checkpoint()
    print("🎉 CSV Filtering complete.")


if __name__ == "__main__":
    extract_logic()
