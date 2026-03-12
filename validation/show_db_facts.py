#!/usr/bin/env python3
"""
Show facts/stats from the Firestore boardgames collection.
Run: python validation/show_db_facts.py
"""
import os
import time
from collections import Counter, defaultdict
from google.cloud import firestore
from google.cloud.firestore_v1.field_path import FieldPath
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "boardgames")
PAGE_SIZE = 500
MAX_RETRIES = 3
TARGET_LANGS = ["ko", "de", "es", "fr", "ja", "ru", "zh"]


def main():
    if not PROJECT_ID:
        print("❌ PROJECT_ID must be set in .env")
        return 1

    db = firestore.Client(project=PROJECT_ID)
    coll = db.collection(COLLECTION_NAME)

    print("=" * 60)
    print("Board Game Database – Facts")
    print("=" * 60)
    print(f"  Collection: {COLLECTION_NAME}")
    print("-" * 60)

    base_games = 0
    expansions = 0
    with_parents = 0
    multi_parent = 0
    year_counts = Counter()
    rating_sum = 0.0
    rating_count = 0
    weight_sum = 0.0
    weight_count = 0
    has_image = 0
    translation_counts = defaultdict(int)  # lang -> count with non-empty title
    total = 0

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
            d = doc.to_dict()
            total += 1

            if d.get("is_expansion"):
                expansions += 1
            else:
                base_games += 1

            parents = d.get("parents") or []
            if parents:
                with_parents += 1
                if len(parents) > 1:
                    multi_parent += 1

            y = d.get("year_published") or 0
            if y and y > 1900:
                year_counts[y] += 1

            r = d.get("rating")
            if r is not None and r > 0:
                rating_sum += float(r)
                rating_count += 1

            w = d.get("weight")
            if w is not None and w > 0:
                weight_sum += float(w)
                weight_count += 1

            if (d.get("image_url") or "").strip():
                has_image += 1

            title = d.get("title") or {}
            for lang in TARGET_LANGS:
                if (title.get(lang) or "").strip():
                    translation_counts[lang] += 1

        start_after_doc = docs[-1]
        if len(docs) < PAGE_SIZE:
            break
        if total % 10000 == 0:
            print(f"  Scanned {total:,} docs...")

    # Output
    print()
    print("📊 Counts")
    print("-" * 60)
    print(f"  Total games:        {total:,}")
    print(f"  Base games:         {base_games:,}")
    print(f"  Expansions:         {expansions:,}")
    print(f"  With parent(s):     {with_parents:,}")
    print(f"  Multi-parent:       {multi_parent:,} (expansion linked to 2+ base games)")
    print()

    print("📊 Metadata")
    print("-" * 60)
    print(f"  With image:         {has_image:,} ({100*has_image/total:.1f}%)" if total else "  With image: 0")
    if rating_count:
        print(f"  Avg rating:         {rating_sum/rating_count:.2f} ({rating_count:,} rated)")
    if weight_count:
        print(f"  Avg weight:         {weight_sum/weight_count:.2f} ({weight_count:,} with weight)")
    print()

    if year_counts:
        print("📊 Top 5 years (by games published)")
        print("-" * 60)
        for year, count in year_counts.most_common(5):
            print(f"  {year}: {count:,}")
        print()

    print("📊 Translation coverage (title)")
    print("-" * 60)
    for lang in TARGET_LANGS:
        n = translation_counts[lang]
        pct = 100 * n / total if total else 0
        print(f"  {lang}: {n:,} ({pct:.1f}%)")
    print()

    print("=" * 60)
    return 0


if __name__ == "__main__":
    exit(main())
