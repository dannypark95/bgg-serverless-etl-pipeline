#!/usr/bin/env python3
"""
Board game database analysis script.
Reports translation coverage, gaps, and other stats.
"""
import os
from collections import defaultdict
from google.cloud import firestore
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "boardgames")
TARGET_LANGS = ["ko", "de", "es", "fr", "ja", "ru", "zh"]
FIELDS = ("title", "description", "summary_description")
QUERY_LIMIT = int(os.getenv("TRANSLATION_QUERY_LIMIT", "10000"))


def _get_count(query, label=""):
    """Run count aggregation and return the value."""
    try:
        result = query.count().get()
        return result[0][0].value
    except Exception as e:
        print(f"  ⚠️ {label}: {e}")
        return None


def run_analysis():
    if not PROJECT_ID:
        raise ValueError("PROJECT_ID must be set in .env")

    db = firestore.Client(project=PROJECT_ID)
    coll = db.collection(COLLECTION_NAME)

    print("=" * 60)
    print("Board Game Database Analysis")
    print("=" * 60)
    print(f"  Collection: {COLLECTION_NAME}")
    print(f"  Target languages: {', '.join(TARGET_LANGS)}")
    print("-" * 60)

    # Total count
    total = _get_count(coll, "total")
    if total is None:
        # Fallback: stream and count (slower)
        print("  Counting total via stream...")
        total = sum(1 for _ in coll.stream())
    print(f"  Total board games: {total:,}")
    print()

    # Per (field, lang): how many have empty
    print("📊 Games missing translations (empty field per language)")
    print("-" * 60)

    missing = defaultdict(dict)
    for field in FIELDS:
        for lang in TARGET_LANGS:
            try:
                q = coll.where(f"{field}.{lang}", "==", "")
                count = _get_count(q, f"{field}.{lang}")
                missing[field][lang] = count

            except Exception as e:
                print(f"  ⚠️ {field}.{lang}: {e}")
                missing[field][lang] = None

    # Print table
    print(f"  {'Field':<22} " + " ".join(f"{lang:>6}" for lang in TARGET_LANGS))
    print("  " + "-" * (22 + 7 * len(TARGET_LANGS)))
    for field in FIELDS:
        row = "  " + f"{field:<22}"
        for lang in TARGET_LANGS:
            v = missing[field][lang]
            row += f" {v:>6,}" if v is not None else "     ?"
        print(row)

    print()
    print("📊 Summary: games needing translation")
    print("-" * 60)

    # Unique games needing translation: use same logic as gemini_translator
    # (union of docs with any empty field/lang)
    seen = {}
    for lang in TARGET_LANGS:
        for field in FIELDS:
            try:
                q = coll.where(f"{field}.{lang}", "==", "")
                for doc in q.limit(QUERY_LIMIT).stream():
                    seen[doc.id] = True
            except Exception as e:
                print(f"  ⚠️ Query {field}.{lang} failed: {e}")

    needs_translation = len(seen)
    max_missing = max(
        (v for row in missing.values() for v in row.values() if v is not None),
        default=0,
    )

    print(f"  Unique games needing translation (sampled): {needs_translation:,}")
    print(f"  Max missing per field/lang: {max_missing:,}")
    if total and max_missing:
        pct = 100 * max_missing / total
        print(f"  Translation gap: ~{pct:.1f}% of games missing at least one translation")
    print()
    print("=" * 60)


if __name__ == "__main__":
    run_analysis()
