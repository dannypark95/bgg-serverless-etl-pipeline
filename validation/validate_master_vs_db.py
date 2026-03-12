#!/usr/bin/env python3
"""
Validate master list (75,206) matches Firestore database.
Checks: coverage (all master games in DB), parent relationships for expansions.
Run: python validate_master_vs_db.py [--date 2026-03-10]
"""
import argparse
import csv
import os
import time
from collections import defaultdict
from datetime import datetime
from typing import Dict, Optional, Set, Tuple

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
MAX_RETRIES = 3


def load_master_list(path: str) -> list:
    if not path or not os.path.exists(path):
        return []
    with open(path, mode="r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


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


def build_master_data(games: list) -> Tuple[Set[str], Dict]:
    """
    Returns (master_ids, expected_parents).
    expected_parents: {bgg_id: [{parent_id, parent_name}, ...]}, deduped by parent_id.
    """
    master_ids = set()
    expected_parents = defaultdict(dict)  # bid -> {pid: pname}
    for row in games:
        bid = str(row["bgg_id"])
        master_ids.add(bid)
        pid = str(row.get("parent_id", "")).strip()
        pname = str(row.get("parent_name", "")).strip()
        if pid and len(pname) > len(expected_parents[bid].get(pid, "")):
            expected_parents[bid][pid] = pname
    return master_ids, {
        bid: [{"parent_id": p, "parent_name": n} for p, n in info.items()]
        for bid, info in expected_parents.items()
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d", "--date",
        default=os.getenv("CURR_DATE") or datetime.now().strftime("%Y-%m-%d"),
        help="Master list date, e.g. 2026-03-10",
    )
    parser.add_argument("master_list", nargs="?", default=None, help="Path to master list CSV")
    parser.add_argument("-o", "--output", help="Write validation report to file")
    args = parser.parse_args()

    master_filename = f"bgg_master_list_{args.date}.csv"
    master_path = args.master_list or master_filename
    if not os.path.exists(master_path):
        print(f"📥 Master list not found at {master_path}. Trying GCS...")
        master_path = download_master_list_from_gcs(master_filename) or master_path
    if not os.path.exists(master_path):
        print(f"❌ Could not load master list.")
        return 1

    games = load_master_list(master_path)
    master_ids, expected_parents = build_master_data(games)

    print("=" * 60)
    print("Master List vs Database Validation")
    print("=" * 60)
    print(f"  Master list: {master_path}")
    print(f"  Master unique games: {len(master_ids):,}")
    print(f"  Expansions (with parents): {len(expected_parents):,}")
    print("-" * 60)

    if not PROJECT_ID:
        print("❌ PROJECT_ID must be set")
        return 1

    db = firestore.Client(project=PROJECT_ID)
    coll = db.collection(COLLECTION_NAME)

    firestore_ids: Set[str] = set()
    missing_parents = []  # (doc_id, expected_count, actual_count, missing_list)
    extra_parents = []  # (doc_id, has unexpected parents)
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
            firestore_ids.add(doc_id)
            d = doc.to_dict()

            expected = expected_parents.get(doc_id)
            if not expected:
                continue

            current = d.get("parents") or []
            if not isinstance(current, list):
                current = []

            current_ids = {p.get("parent_id", "") for p in current if p.get("parent_id")}
            expected_ids = {p["parent_id"] for p in expected}

            missing = expected_ids - current_ids
            extra = current_ids - expected_ids

            if missing:
                missing_parents.append(
                    (doc_id, len(expected), len(current), list(missing)[:5])
                )
            if extra:
                extra_parents.append((doc_id, list(extra)[:5]))

        start_after_doc = last_doc
        if len(docs) < PAGE_SIZE:
            break

        if len(firestore_ids) % 10000 == 0 and len(firestore_ids) > 0:
            print(f"  Scanned {len(firestore_ids):,} Firestore docs...")

    # Results
    missing_from_db = master_ids - firestore_ids
    extra_in_db = firestore_ids - master_ids

    print()
    print("📊 Validation Results")
    print("-" * 60)
    print(f"  Master list:     {len(master_ids):,} unique games")
    print(f"  Firestore:       {len(firestore_ids):,} docs")
    print()
    print(f"  ❌ In master, NOT in DB:  {len(missing_from_db):,}")
    print(f"  ⚠️ In DB, NOT in master: {len(extra_in_db):,}")
    print(f"  ❌ Expansions missing parents: {len(missing_parents):,}")
    print(f"  ⚠️ Expansions with extra parents: {len(extra_parents):,} (parent in DB not in master)")
    in_both = master_ids & firestore_ids
    expansions_ok = len(in_both) - len({m[0] for m in missing_parents})
    print(f"  ✅ In both, parents OK: {expansions_ok:,}")
    print()

    report_lines = []

    if missing_from_db:
        print("  Sample missing from DB (first 10):")
        sample = sorted(missing_from_db)[:10]
        for sid in sample:
            print(f"    {sid}")
        report_lines.append(f"\nMissing from DB ({len(missing_from_db)} total):")
        report_lines.extend(f"  {x}" for x in sorted(missing_from_db)[:100])

    if missing_parents:
        print("\n  Sample expansions missing parents (first 5):")
        for doc_id, exp, act, miss in missing_parents[:5]:
            print(f"    {doc_id}: expected {exp}, has {act} | missing: {miss}")
        report_lines.append(f"\nExpansions missing parents ({len(missing_parents)} total):")
        for doc_id, exp, act, miss in missing_parents[:20]:
            report_lines.append(f"  {doc_id}: expected {exp}, has {act} | missing: {miss}")

    if extra_in_db:
        print("\n  In DB, NOT in master (sample of 15):")
        for eid in sorted(extra_in_db)[:15]:
            print(f"    {eid}")
        if len(extra_in_db) > 15:
            print(f"    ... and {len(extra_in_db) - 15:,} more")

    if extra_parents:
        print("\n  Expansions with extra parents (has parent(s) in DB that master list doesn't list):")
        for doc_id, extra_list in extra_parents:
            print(f"    {doc_id}: extra parents = {extra_list}")

    print()
    print("=" * 60)

    if args.output:
        report_lines.append(f"\nIn DB, NOT in master ({len(extra_in_db)} total):")
        report_lines.extend(f"  {x}" for x in sorted(extra_in_db)[:200])
        report_lines.append(f"\nExpansions with extra parents ({len(extra_parents)} total):")
        for doc_id, extra_list in extra_parents:
            report_lines.append(f"  {doc_id}: {extra_list}")
        with open(args.output, "w", encoding="utf-8") as f:
            f.write("\n".join(report_lines))
        print(f"  Report written to {args.output}")

    return 0 if not missing_from_db and not missing_parents else 1


if __name__ == "__main__":
    exit(main())
