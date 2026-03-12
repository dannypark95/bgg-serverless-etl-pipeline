#!/usr/bin/env python3
"""
Clone boardgames collection from one Firestore project to another.
Source: boardgame-catalog-app / boardgames
Dest:   test2025042-firebase / danny_boardgames

Run: python validation/clone_to_firestore.py [--dry-run]
Requires: gcloud auth and access to both projects.
"""
import argparse
import time
from google.cloud import firestore
from google.cloud.firestore_v1.field_path import FieldPath
from dotenv import load_dotenv

load_dotenv()

SOURCE_PROJECT = "boardgame-catalog-app"
SOURCE_COLLECTION = "boardgames"
DEST_PROJECT = "test2025042-firebase"
DEST_COLLECTION = "danny_boardgames"
PAGE_SIZE = 500
BATCH_SIZE = 400  # Firestore batch limit is 500
MAX_RETRIES = 3


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="No writes, only report count")
    args = parser.parse_args()

    print("=" * 60)
    print("Clone Firestore Collection")
    print("=" * 60)
    print(f"  Source: {SOURCE_PROJECT} / {SOURCE_COLLECTION}")
    print(f"  Dest:   {DEST_PROJECT} / {DEST_COLLECTION}")
    print(f"  Mode:   {'DRY RUN' if args.dry_run else 'LIVE'}")
    print("-" * 60)

    source_db = firestore.Client(project=SOURCE_PROJECT)
    dest_db = firestore.Client(project=DEST_PROJECT)
    source_coll = source_db.collection(SOURCE_COLLECTION)
    dest_coll = dest_db.collection(DEST_COLLECTION)

    copied = 0
    start_after_doc = None

    while True:
        query = source_coll.order_by(FieldPath.document_id()).limit(PAGE_SIZE)
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

        if args.dry_run:
            copied += len(docs)
            if copied % 5000 == 0:
                print(f"  Would copy {copied:,} docs...")
        else:
            for i in range(0, len(docs), BATCH_SIZE):
                chunk = docs[i : i + BATCH_SIZE]
                batch = dest_db.batch()
                for doc in chunk:
                    data = doc.to_dict()
                    batch.set(dest_coll.document(doc.id), data)
                batch.commit()
                copied += len(chunk)
                if copied % 5000 == 0:
                    print(f"  Copied {copied:,} docs...")

        start_after_doc = docs[-1]
        if len(docs) < PAGE_SIZE:
            break

    print("-" * 60)
    print(f"✅ {'Would copy' if args.dry_run else 'Copied'} {copied:,} documents")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    exit(main())
