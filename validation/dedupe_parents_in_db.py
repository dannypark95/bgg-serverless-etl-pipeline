#!/usr/bin/env python3
"""
One-time script: remove duplicate parent_id entries from expansions' parents arrays.
Same parent can appear twice with different names (e.g. "18NY" vs "18NY: The Formation of...").
Keeps one per parent_id, preferring the longer (full) name.
Run: python dedupe_parents_in_db.py [--dry-run]
"""
import argparse
import os
import time
from google.cloud import firestore
from google.cloud.firestore_v1.field_path import FieldPath
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "boardgames")
BATCH_SIZE = 400
PAGE_SIZE = 500
MAX_RETRIES = 3


def dedupe_parents(parents: list) -> list:
    """Keep one entry per parent_id, preferring longer parent_name."""
    by_id = {}
    for p in parents or []:
        pid = p.get("parent_id", "")
        pname = p.get("parent_name", "")
        if pid and len(pname) > len(by_id.get(pid, "")):
            by_id[pid] = pname
    return [{"parent_id": p, "parent_name": n} for p, n in by_id.items()]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="No writes")
    args = parser.parse_args()

    if not PROJECT_ID:
        print("❌ PROJECT_ID must be set")
        return 1

    db = firestore.Client(project=PROJECT_ID)
    coll = db.collection(COLLECTION_NAME)

    print("=" * 60)
    print("Dedupe Parents in Expansions")
    print("=" * 60)
    print(f"  Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("-" * 60)

    updated = 0
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

        batch = db.batch()
        batch_count = 0
        last_doc = None

        for doc in docs:
            last_doc = doc
            d = doc.to_dict()
            current = d.get("parents") or []
            if not isinstance(current, list):
                continue

            deduped = dedupe_parents(current)
            if len(deduped) < len(current):
                if not args.dry_run:
                    batch.update(coll.document(doc.id), {"parents": deduped})
                    batch_count += 1
                    if batch_count >= BATCH_SIZE:
                        batch.commit()
                        batch = db.batch()
                        batch_count = 0
                updated += 1
                if updated <= 5:
                    print(f"    {doc.id}: {len(current)} -> {len(deduped)} parents")

        if not args.dry_run and batch_count > 0:
            batch.commit()
        start_after_doc = last_doc
        if len(docs) < PAGE_SIZE:
            break

    print("-" * 60)
    print(f"✅ Deduped: {updated:,} expansions")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    exit(main())
