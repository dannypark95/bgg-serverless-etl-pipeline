#!/usr/bin/env python3
"""
Targeted Korean title translator.

Reads boardgames_missing_korean_title_*.csv (bgg_id, title_en), asks Gemini
for the official Korean retail title (boardlife.co.kr priority, phonetic
transliteration fallback), then writes title.ko back to Firestore.

Usage:
  python validation/translate_missing_korean_titles.py \\
      --csv validation/boardgames_missing_korean_title_20260317_005235.csv

Options:
  --csv         Path to the missing-Korean-titles CSV  (required)
  --batch-size  Games per Gemini request               (default: 20)
  --max-games   Stop after N games                     (default: all)
  --dry-run     Print translations without writing to Firestore
"""

import argparse
import csv
import json
import os
import time

from dotenv import load_dotenv
from google.cloud import firestore

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if not PROJECT_ID or not GEMINI_API_KEY:
    raise ValueError("PROJECT_ID and GEMINI_API_KEY env vars must be set")

MODEL_ID = "gemini-2.0-flash"
FIRESTORE_BATCH_SIZE = 400

# ---------------------------------------------------------------------------
# System instruction: Korean-title-only, strict Official Name Rule
# ---------------------------------------------------------------------------
SYSTEM_INSTRUCTION = (
    "You are a professional Board Game Localizer and BGG (BoardGameGeek) expert "
    "specialising in the Korean market.\n\n"

    "### THE 'OFFICIAL NAME' RULE (CRITICAL):\n"
    "1. Use the OFFICIAL RETAIL TITLE published in Korea (boardlife.co.kr is the "
    "authoritative source). Examples: 'Scythe' -> '사이드', 'Brass' -> '브라스', "
    "'Terraforming Mars' -> '테라포밍 마스', 'Wingspan' -> '윙스팬'.\n"
    "2. If no official Korean retail title exists, use PHONETIC TRANSLITERATION "
    "(hangul romanisation). NEVER translate the literal meaning of the title. "
    "Example: 'Zoom Zoom' -> '줌 줌', NOT '붕붕'.\n\n"

    "### OUTPUT FORMAT (MANDATORY):\n"
    "- Return a single JSON object where each key is the BGG ID (string).\n"
    "- Value is the Korean title string only.\n"
    "- Example: {\"224517\": \"테라포밍 마스\", \"13\": \"카탄\"}\n"
    "- Return raw JSON only — no markdown fences, no extra keys, no explanation.\n"
    "- Use valid JSON escapes only."
)

# ---------------------------------------------------------------------------
# Gemini client (google-genai SDK)
# ---------------------------------------------------------------------------
try:
    from google import genai
    from google.genai import types as genai_types

    _client = genai.Client(api_key=GEMINI_API_KEY)

    def _call_gemini(prompt: str) -> str:
        r = _client.models.generate_content(
            model=MODEL_ID,
            contents=prompt,
            config=genai_types.GenerateContentConfig(
                system_instruction=SYSTEM_INSTRUCTION,
                response_mime_type="application/json",
            ),
        )
        return r.text or ""

except ImportError:
    import google.generativeai as genai  # type: ignore

    genai.configure(api_key=GEMINI_API_KEY)
    _legacy_model = genai.GenerativeModel(
        model_name=MODEL_ID,
        generation_config={"response_mime_type": "application/json"},
        system_instruction=SYSTEM_INSTRUCTION,
    )

    def _call_gemini(prompt: str) -> str:
        return _legacy_model.generate_content(prompt).text or ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _repair_json_escapes(text: str) -> str:
    """Fix invalid JSON escape sequences Gemini sometimes returns."""
    result = []
    i = 0
    while i < len(text):
        if text[i] == "\\" and i + 1 < len(text):
            c = text[i + 1]
            if c in '"\\/bfnrt':
                result.append(text[i: i + 2])
                i += 2
            elif c == "u" and i + 5 <= len(text):
                hex_part = text[i + 2: i + 6]
                if len(hex_part) == 4 and all(h in "0123456789abcdefABCDEF" for h in hex_part):
                    result.append(text[i: i + 6])
                    i += 6
                else:
                    result.append("\\\\")
                    result.append(c)
                    i += 2
            else:
                result.append("\\\\")
                result.append(c)
                i += 2
        else:
            result.append(text[i])
            i += 1
    return "".join(result)


def _parse_json(text: str) -> dict:
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        if "Invalid \\escape" not in str(e):
            raise
        return json.loads(_repair_json_escapes(text))


def _read_csv(path: str) -> list[dict]:
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append({"bgg_id": row["bgg_id"].strip(), "title_en": row["title_en"].strip()})
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Translate missing Korean board game titles via Gemini.")
    parser.add_argument("--csv", required=True, help="Path to missing-Korean-titles CSV")
    parser.add_argument("--batch-size", type=int, default=20, help="Games per Gemini call (default: 20)")
    parser.add_argument("--max-games", type=int, default=None, help="Stop after N games (default: all)")
    parser.add_argument("--dry-run", action="store_true", help="Print translations but do not write to Firestore")
    args = parser.parse_args()

    games = _read_csv(args.csv)
    if args.max_games:
        games = games[: args.max_games]

    total = len(games)
    print("=" * 60)
    print("Targeted Korean Title Translator")
    print("=" * 60)
    print(f"  CSV:        {args.csv}")
    print(f"  Games:      {total:,}")
    print(f"  Batch size: {args.batch_size}")
    print(f"  Model:      {MODEL_ID}")
    print(f"  Dry run:    {args.dry_run}")
    print("-" * 60)

    db = None if args.dry_run else firestore.Client(project=PROJECT_ID)

    num_batches = (total + args.batch_size - 1) // args.batch_size
    total_updated = 0
    total_skipped = 0
    start_time = time.time()

    # Accumulate Firestore writes and commit in large batches for efficiency
    fs_batch = None if args.dry_run else db.batch()
    fs_batch_count = 0

    for batch_num, i in enumerate(range(0, total, args.batch_size), start=1):
        chunk = games[i: i + args.batch_size]
        batch_start = time.time()
        print(f"  Batch {batch_num}/{num_batches} ({len(chunk)} games)...", end=" ", flush=True)

        # Build prompt: list of {id, title_en} objects
        prompt = (
            f"Return Korean retail titles for these {len(chunk)} board games. "
            f"Use boardlife.co.kr official names where they exist; otherwise use "
            f"phonetic transliteration. Input: {json.dumps([{'id': g['bgg_id'], 'title_en': g['title_en']} for g in chunk])}"
        )

        try:
            text = _call_gemini(prompt)
            try:
                results: dict = _parse_json(text)
            except (json.JSONDecodeError, Exception):
                # One retry with a fresh call
                time.sleep(3)
                text = _call_gemini(prompt)
                results = _parse_json(text)

            batch_updated = 0
            for game in chunk:
                bgg_id = game["bgg_id"]
                title_ko = (results.get(bgg_id) or results.get(str(bgg_id)) or "").strip()
                if not title_ko:
                    total_skipped += 1
                    continue

                if args.dry_run:
                    print(f"\n      {bgg_id}: {game['title_en']} -> {title_ko}", end="")
                else:
                    doc_ref = db.collection("boardgames").document(bgg_id)
                    fs_batch.update(doc_ref, {
                        "title.ko": title_ko,
                        "updated_at": firestore.SERVER_TIMESTAMP,
                    })
                    fs_batch_count += 1

                batch_updated += 1
                total_updated += 1

            # Commit Firestore batch when it reaches the size limit
            if not args.dry_run and fs_batch_count >= FIRESTORE_BATCH_SIZE:
                fs_batch.commit()
                fs_batch = db.batch()
                fs_batch_count = 0

            elapsed = time.time() - batch_start
            rate = total_updated / max(0.001, time.time() - start_time) * 60
            print(f"✅ {batch_updated} translated ({elapsed:.1f}s) | {rate:.1f} games/min")

        except Exception as e:
            print(f"❌ FAILED: {e}")
            import traceback
            traceback.print_exc()

        time.sleep(1)  # Rate-limit buffer

    # Final Firestore commit
    if not args.dry_run and fs_batch_count > 0:
        fs_batch.commit()

    elapsed_total = time.time() - start_time
    rate_avg = total_updated / max(0.001, elapsed_total) * 60
    print("-" * 60)
    print(f"✅ Done.")
    print(f"   Translated: {total_updated:,}")
    print(f"   Skipped:    {total_skipped:,}")
    print(f"   Elapsed:    {elapsed_total:.0f}s")
    print(f"   Avg rate:   {rate_avg:.1f} games/min")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
