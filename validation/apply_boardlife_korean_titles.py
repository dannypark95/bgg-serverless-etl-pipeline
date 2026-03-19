import csv
import os
import time
from datetime import datetime
from typing import Dict, Optional, Set

from dotenv import load_dotenv
from google.cloud import firestore
from google.cloud.firestore_v1.field_path import FieldPath


load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
if not PROJECT_ID:
    raise ValueError("PROJECT_ID env var must be set")

BOARDGAMES_COLLECTION = os.getenv("COLLECTION_NAME", "boardgames")
TRANSLATION_COLLECTION = os.getenv("TRANSLATION_COLLECTION", "translation")

PAGE_SIZE = int(os.getenv("PAGE_SIZE", "500"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "400"))

OUTPUT_DIR = os.getenv("OUTPUT_DIR") or os.path.dirname(__file__)


def _safe_str(x) -> str:
    return (x or "").strip()


def _list_collection_ids(db: firestore.Client, collection: str) -> Set[str]:
    coll = db.collection(collection)
    ids: Set[str] = set()
    start_after_doc = None
    scanned = 0

    while True:
        q = coll.order_by(FieldPath.document_id()).limit(PAGE_SIZE)
        if start_after_doc is not None:
            q = q.start_after(start_after_doc)
        docs = list(q.stream())
        if not docs:
            break

        for d in docs:
            ids.add(d.id)
        scanned += len(docs)
        start_after_doc = docs[-1]

        if scanned % 5000 == 0:
            print(f"  Scanned {scanned:,} docs from `{collection}`...")

        if len(docs) < PAGE_SIZE:
            break

    print(f"  `{collection}`: {len(ids):,} ids loaded")
    return ids


def apply_korean_titles(db: firestore.Client) -> int:
    print("=" * 60)
    print("Apply BoardLife Korean titles to boardgames")
    print("=" * 60)
    print(f"Project: {PROJECT_ID}")
    print(f"Boardgames: `{BOARDGAMES_COLLECTION}` | Translation: `{TRANSLATION_COLLECTION}`")

    print("\nLoading boardgames ids (for existence check)...")
    boardgame_ids = _list_collection_ids(db, BOARDGAMES_COLLECTION)

    tcoll = db.collection(TRANSLATION_COLLECTION)
    bcoll = db.collection(BOARDGAMES_COLLECTION)

    updated = 0
    skipped_missing_boardgame = 0
    skipped_empty_ko = 0
    batch = db.batch()
    batch_count = 0

    start_after_doc = None
    scanned = 0

    print("\nApplying translations...")
    while True:
        q = tcoll.order_by(FieldPath.document_id()).limit(PAGE_SIZE)
        if start_after_doc is not None:
            q = q.start_after(start_after_doc)
        docs = list(q.stream())
        if not docs:
            break

        for doc in docs:
            scanned += 1
            bgg_id = doc.id
            if bgg_id not in boardgame_ids:
                skipped_missing_boardgame += 1
                continue

            d = doc.to_dict() or {}
            title_ko = _safe_str(d.get("title_ko"))
            if not title_ko:
                skipped_empty_ko += 1
                continue

            batch.update(
                bcoll.document(bgg_id),
                {"title.ko": title_ko, "updated_at": firestore.SERVER_TIMESTAMP},
            )
            batch_count += 1
            updated += 1

            if batch_count >= BATCH_SIZE:
                batch.commit()
                batch = db.batch()
                batch_count = 0

        start_after_doc = docs[-1]
        if scanned % 5000 == 0:
            print(f"  Scanned {scanned:,} translation docs... updated {updated:,}")

        if len(docs) < PAGE_SIZE:
            break

    if batch_count:
        batch.commit()

    print("\nDone applying.")
    print(f"  Updated boardgames: {updated:,}")
    print(f"  Skipped (boardgame missing): {skipped_missing_boardgame:,}")
    print(f"  Skipped (empty title_ko): {skipped_empty_ko:,}")
    return updated


def export_missing_korean_titles_csv(db: firestore.Client) -> str:
    print("\n" + "=" * 60)
    print("Export boardgames missing title.ko")
    print("=" * 60)

    coll = db.collection(BOARDGAMES_COLLECTION)
    start_after_doc = None
    rows: list[Dict[str, str]] = []
    scanned = 0

    while True:
        q = coll.order_by(FieldPath.document_id()).limit(PAGE_SIZE)
        if start_after_doc is not None:
            q = q.start_after(start_after_doc)
        docs = list(q.stream())
        if not docs:
            break

        for doc in docs:
            scanned += 1
            d = doc.to_dict() or {}
            title = d.get("title") or {}
            ko = _safe_str(title.get("ko"))
            if ko:
                continue

            en = _safe_str((title.get("en") if isinstance(title, dict) else "") or "")
            rows.append({"bgg_id": doc.id, "title_en": en})

        start_after_doc = docs[-1]
        if scanned % 5000 == 0:
            print(f"  Scanned {scanned:,} boardgames... missing ko so far {len(rows):,}")

        if len(docs) < PAGE_SIZE:
            break

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(OUTPUT_DIR, f"boardgames_missing_korean_title_{ts}.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["bgg_id", "title_en"])
        w.writeheader()
        w.writerows(rows)

    print(f"\nWrote {len(rows):,} rows to {out_path}")
    return out_path


def main() -> int:
    started = time.time()
    db = firestore.Client(project=PROJECT_ID)

    apply_korean_titles(db)
    export_missing_korean_titles_csv(db)

    print(f"\nAll done in {time.time() - started:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

