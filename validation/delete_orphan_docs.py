#!/usr/bin/env python3
"""
One-time script: delete Firestore docs that are NOT in the master list.
Run: python delete_orphan_docs.py [--dry-run] [--date 2026-03-10]
"""
import argparse
import csv
import os
import time
from datetime import datetime
from typing import Optional, Set

from google.cloud import firestore, storage
from google.cloud.firestore_v1.field_path import FieldPath
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME") or (
    f"{os.getenv('PROJECT_ID')}.firebasestorage.app" if os.getenv("PROJECT_ID") else None
)
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "boardgames")
PAGE_SIZE = 500
BATCH_SIZE = 500  # Firestore batch limit
MAX_RETRIES = 3


def load_master_list(path: str) -> Set[str]:
    if not path or not os.path.exists(path):
        return set()
    with open(path, mode="r", encoding="utf-8") as f:
        return {str(row["bgg_id"]) for row in csv.DictReader(f)}


def download_master_list_from_gcs(filename: str) -> Optional[str]:
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="No deletes, only report")
    parser.add_argument(
        "-d", "--date",
        default=os.getenv("CURR_DATE") or datetime.now().strftime("%Y-%m-%d"),
        help="Master list date, e.g. 2026-03-10",
    )
    parser.add_argument("master_list", nargs="?", default=None, help="Path to master list CSV")
    args = parser.parse_args()

    master_filename = f"bgg_master_list_{args.date}.csv"
    master_path = args.master_list or master_filename
    if not os.path.exists(master_path):
        print(f"📥 Master list not found at {master_path}. Trying GCS...")
        master_path = download_master_list_from_gcs(master_filename) or master_path
    if not os.path.exists(master_path):
        print(f"❌ Could not load master list.")
        return 1

    master_ids = load_master_list(master_path)
    print("=" * 60)
    print("Delete Orphan Docs (In DB, NOT in master)")
    print("=" * 60)
    print(f"  Master list: {master_path} ({len(master_ids):,} unique)")
    print(f"  Mode: {'DRY RUN (no deletes)' if args.dry_run else 'LIVE'}")
    print("-" * 60)

    if not PROJECT_ID:
        print("❌ PROJECT_ID must be set")
        return 1

    db = firestore.Client(project=PROJECT_ID)
    coll = db.collection(COLLECTION_NAME)

    firestore_ids: Set[str] = set()
    start_after_doc = None

    while True:
        query = coll.order_by(FieldPath.document_id()).limit(PAGE_SIZE)
        if start_after_doc:
            query = query.start_after(start_after_doc)

        docs = None
        for attempt in range(MAX_RETRIES):
            try:
                docs = list(query.stream())
                break
            except Exception as e:
                if "timed out" in str(e).lower() or "503" in str(e) or "UNAVAILABLE" in str(e):
                    time.sleep((attempt + 1) * 10)
                else:
                    raise

        if not docs:
            break

        for doc in docs:
            firestore_ids.add(doc.id)
        start_after_doc = docs[-1]
        if len(docs) < PAGE_SIZE:
            break
        if len(firestore_ids) % 10000 == 0:
            print(f"  Scanned {len(firestore_ids):,} docs...")

    to_delete = firestore_ids - master_ids
    print(f"  Orphan docs to delete: {len(to_delete):,}")

    if not to_delete:
        print("  Nothing to delete.")
        print("=" * 60)
        return 0

    if args.dry_run:
        print("  Sample (first 15):")
        for doc_id in sorted(to_delete)[:15]:
            print(f"    {doc_id}")
        print("=" * 60)
        return 0

    deleted = 0
    batch = db.batch()
    batch_count = 0
    for doc_id in sorted(to_delete):
        batch.delete(coll.document(doc_id))
        batch_count += 1
        if batch_count >= BATCH_SIZE:
            batch.commit()
            deleted += batch_count
            print(f"  Deleted {deleted:,} / {len(to_delete):,}")
            batch = db.batch()
            batch_count = 0

    if batch_count > 0:
        batch.commit()
        deleted += batch_count

    print("-" * 60)
    print(f"✅ Deleted {deleted:,} orphan docs")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    exit(main())
