#!/usr/bin/env python3
"""
Export board games from Firestore into JSON batches suitable for manual translation
with Gemini (web UI).

Each batch file is a JSON array of objects:
[
  {
    "id": "224517",
    "title_en": "...",
    "summary_en": "...",
    "description_en": "..."
  },
  ...
]

Run examples:
  python validation/export_for_gemini_manual.py               # default: 50 per batch
  python validation/export_for_gemini_manual.py --batch-size 20 --max-games 500
"""
import argparse
import json
import os
import time
from datetime import datetime

from google.cloud import firestore
from google.cloud.firestore_v1.field_path import FieldPath
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "boardgames")
PAGE_SIZE = 500
MAX_RETRIES = 3


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Number of games per output file (default: 50)",
    )
    parser.add_argument(
        "--max-games",
        type=int,
        default=None,
        help="Optional maximum number of games to export",
    )
    parser.add_argument(
        "--output-prefix",
        default=f"gemini_export_{datetime.now().strftime('%Y%m%d')}",
        help="Prefix for output files (default: gemini_export_YYYYMMDD)",
    )
    args = parser.parse_args()

    if not PROJECT_ID:
        raise ValueError("PROJECT_ID must be set in .env")

    db = firestore.Client(project=PROJECT_ID)
    coll = db.collection(COLLECTION_NAME)

    print("=" * 60)
    print("Export for Gemini (manual translation)")
    print("=" * 60)
    print(f"  Project:    {PROJECT_ID}")
    print(f"  Collection: {COLLECTION_NAME}")
    print(f"  Batch size: {args.batch_size}")
    if args.max_games:
        print(f"  Max games:  {args.max_games}")
    print(f"  Output prefix: {args.output_prefix}_NNNN.json")
    print("-" * 60)

    # Each batch is a dict keyed by BGG ID, matching tests/output_translations.json shape
    # {
    #   "224517": {
    #     "title": { "en": "..." },
    #     "description": { "en": "..." },
    #     "summary": { "en": "..." }
    #   },
    #   ...
    # }
    batch = {}
    batch_index = 0
    exported = 0
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

        for doc in docs:
            d = doc.to_dict()

            title_map = d.get("title") or {}
            desc_map = d.get("description") or {}
            summary_map = d.get("summary_description") or d.get("summary") or {}

            batch[doc.id] = {
                "title": {"en": (title_map.get("en") or "").strip()},
                "description": {"en": (desc_map.get("en") or "").strip()},
                "summary": {"en": (summary_map.get("en") or "").strip()},
            }
            exported += 1

            if len(batch) >= args.batch_size:
                batch_index += 1
                filename = f"{args.output_prefix}_{batch_index:04d}.json"
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(batch, f, ensure_ascii=False, indent=2)
                print(f"  ✅ Wrote batch {batch_index} ({len(batch)} games) -> {filename}")
                batch = {}

            if args.max_games and exported >= args.max_games:
                break

        start_after_doc = docs[-1]
        if args.max_games and exported >= args.max_games:
            break
        if len(docs) < PAGE_SIZE:
            break

    # Flush final partial batch
    if batch:
        batch_index += 1
        filename = f"{args.output_prefix}_{batch_index:04d}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(batch, f, ensure_ascii=False, indent=2)
        print(f"  ✅ Wrote batch {batch_index} ({len(batch)} games) -> {filename}")

    print("-" * 60)
    print(f"✅ Exported {exported:,} games in {batch_index} batch file(s).")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    exit(main())

