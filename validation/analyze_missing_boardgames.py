#!/usr/bin/env python3
"""
Ad-hoc analysis: list board games in the master list that are NOT in Firestore.
Outputs a local CSV of missing games.
"""
import argparse
import csv
import os
from datetime import datetime
from typing import Optional, Set
from google.cloud import firestore, storage
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME") or (f"{os.getenv('PROJECT_ID')}.firebasestorage.app" if os.getenv("PROJECT_ID") else None)
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "boardgames")
OUTPUT_FILENAME = "missing_boardgames.csv"


def load_master_list(path: Optional[str]) -> list:
    """Load master list from local CSV. Returns list of dicts with bgg_id, name, etc."""
    if not path or not os.path.exists(path):
        return []
    with open(path, mode="r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def download_master_list_from_gcs(filename: str) -> Optional[str]:
    """Download master list from GCS. Returns local path or None."""
    if not PROJECT_ID or not BUCKET_NAME:
        return None
    try:
        bucket = storage.Client(project=PROJECT_ID).bucket(BUCKET_NAME)
        blob = bucket.blob(filename)
        blob.download_to_filename(filename)
        return filename
    except Exception as e:
        print(f"  ⚠️ GCS download failed: {e}")
        return None


def get_firestore_ids(db) -> Set[str]:
    """Stream all document IDs from Firestore boardgames collection."""
    ids = set()
    for doc in db.collection(COLLECTION_NAME).stream():
        ids.add(doc.id)
    return ids


def main():
    parser = argparse.ArgumentParser(description="List board games in master list but not in Firestore")
    parser.add_argument(
        "master_list",
        nargs="?",
        default=None,
        help="Path to master list CSV (or use --date to download from GCS)",
    )
    parser.add_argument(
        "-d", "--date",
        default=os.getenv("CURR_DATE") or datetime.now().strftime("%Y-%m-%d"),
        help="Date for master list filename, e.g. 2026-03-10 (default: today or CURR_DATE)",
    )
    parser.add_argument(
        "-o", "--output",
        default=OUTPUT_FILENAME,
        help=f"Output CSV path (default: {OUTPUT_FILENAME})",
    )
    args = parser.parse_args()

    master_list_filename = f"bgg_master_list_{args.date}.csv"
    master_path = args.master_list
    if not master_path:
        master_path = master_list_filename
    if not os.path.exists(master_path):
        print(f"📥 Master list not found at {master_path}. Trying GCS...")
        downloaded = download_master_list_from_gcs(master_list_filename)
        if downloaded:
            master_path = downloaded
    if not os.path.exists(master_path):
        print(f"❌ Could not load master list. Provide path: python analyze_missing_boardgames.py <path_to_master.csv>")
        return 1

    print("=" * 60)
    print("Missing Board Games Analysis")
    print("=" * 60)
    print(f"  Master list: {master_path}")

    master = load_master_list(master_path)
    master_ids = {str(row["bgg_id"]) for row in master}
    print(f"  Master list rows: {len(master):,} | Unique IDs: {len(master_ids):,}")

    if not PROJECT_ID:
        print("❌ PROJECT_ID must be set in .env")
        return 1

    print("  Fetching Firestore document IDs...")
    db = firestore.Client(project=PROJECT_ID)
    firestore_ids = get_firestore_ids(db)
    print(f"  Firestore count: {len(firestore_ids):,}")

    missing_ids = master_ids - firestore_ids
    missing_rows = [r for r in master if str(r["bgg_id"]) in missing_ids]

    print(f"  Missing (in master, not in Firestore): {len(missing_ids):,}")
    print("-" * 60)

    with open(args.output, mode="w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["bgg_id", "name", "parent_id", "parent_name", "is_expansion"])
        writer.writeheader()
        writer.writerows(missing_rows)

    print(f"✅ Wrote {len(missing_rows):,} missing games to {args.output}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    exit(main())
