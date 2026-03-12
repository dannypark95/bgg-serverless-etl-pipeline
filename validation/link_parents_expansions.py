#!/usr/bin/env python3
"""
One-time script: ensure each expansion has all its parents in the parents array.
Uses master list as source of truth for parent-expansion relationships.
Run: python link_parents_expansions.py [--dry-run] [--date 2026-03-10]
"""
import argparse
import csv
import os
import time
from collections import defaultdict
from datetime import datetime
from typing import Optional

from google.cloud import firestore, storage
from google.cloud.firestore_v1.field_path import FieldPath
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
BUCKET_NAME = os.getenv("BUCKET_NAME") or (
    f"{os.getenv('PROJECT_ID')}.firebasestorage.app" if os.getenv("PROJECT_ID") else None
)
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "boardgames")
BATCH_SIZE = 400
PAGE_SIZE = 500
MAX_RETRIES = 3


def load_master_list(path: str) -> list:
    """Load master list CSV."""
    if not path or not os.path.exists(path):
        return []
    with open(path, mode="r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def download_master_list_from_gcs(filename: str) -> Optional[str]:
    """Download master list from GCS."""
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


def build_expected_parents(games: list) -> dict:
    """
    From master list, build {bgg_id: [{parent_id, parent_name}, ...]}.
    Dedupes by parent_id (keeps longest name per parent).
    """
    by_id = defaultdict(dict)  # bid -> {pid: pname}
    for row in games:
        bid = str(row["bgg_id"])
        pid = str(row.get("parent_id", "")).strip()
        pname = str(row.get("parent_name", "")).strip()
        if pid and len(pname) > len(by_id[bid].get(pid, "")):
            by_id[bid][pid] = pname
    return {
        bid: [{"parent_id": p, "parent_name": n} for p, n in info.items()]
        for bid, info in by_id.items()
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="No writes, only report")
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
        print(f"❌ Could not load master list. Use: python link_parents_expansions.py --date 2026-03-10")
        return 1

    games = load_master_list(master_path)
    expected_parents = build_expected_parents(games)
    print("=" * 60)
    print("Link Parents & Expansions")
    print("=" * 60)
    print(f"  Master list: {master_path}")
    print(f"  Expansions with parents in master list: {len(expected_parents):,}")
    print(f"  Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("-" * 60)

    if not PROJECT_ID:
        print("❌ PROJECT_ID must be set")
        return 1

    db = firestore.Client(project=PROJECT_ID)
    coll = db.collection(COLLECTION_NAME)

    updated = 0
    skipped = 0
    batch = db.batch()
    batch_count = 0
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
                    wait = (attempt + 1) * 10
                    print(f"  ⚠️ Query timeout, retry {attempt + 1}/{MAX_RETRIES} in {wait}s...")
                    time.sleep(wait)
                else:
                    raise

        if not docs:
            break

        last_doc = None
        for doc in docs:
            last_doc = doc
            doc_id = doc.id
            d = doc.to_dict()

            expected = expected_parents.get(doc_id)
            if not expected:
                skipped += 1
                continue

            current = d.get("parents") or []
            if not isinstance(current, list):
                current = []

            current_by_id = {p.get("parent_id", ""): p.get("parent_name", "") for p in current if p.get("parent_id")}
            missing = [p for p in expected if p["parent_id"] not in current_by_id]

            if not missing:
                skipped += 1
                continue

            # Merge current + expected, dedupe by parent_id (prefer longer name)
            merged = dict(current_by_id)
            for p in expected:
                pid, pname = p["parent_id"], p["parent_name"]
                if len(pname) > len(merged.get(pid, "")):
                    merged[pid] = pname
            new_parents = [{"parent_id": p, "parent_name": n} for p, n in merged.items()]

            if not args.dry_run:
                batch.update(coll.document(doc_id), {"parents": new_parents})
                batch_count += 1
                if batch_count >= BATCH_SIZE:
                    batch.commit()
                    print(f"  Committed batch ({updated + batch_count:,} updated so far)")
                    batch = db.batch()
                    batch_count = 0

            updated += 1
            if updated <= 3:  # Sample
                print(f"    {doc_id}: +{len(missing)} parents (had {len(current)}, now {len(new_parents)})")

        start_after_doc = last_doc
        if len(docs) < PAGE_SIZE:
            break

    if not args.dry_run and batch_count > 0:
        batch.commit()

    print("-" * 60)
    print(f"✅ Updated: {updated:,} | Skipped (no change needed): {skipped:,}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    exit(main())
