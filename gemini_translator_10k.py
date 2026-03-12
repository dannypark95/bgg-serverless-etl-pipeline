#!/usr/bin/env python3
"""
Limited Gemini translator: translate at most N games in one run.

Use this when you want to do a smaller batch locally (e.g. 10k games)
without touching the main gemini_translator.py pipeline.

Run examples:
  python gemini_translator_10k.py                # default: 10,000 games
  python gemini_translator_10k.py --max-games 5000 --batch-size 10
"""
import os
import json
import time
import argparse

from google.cloud import firestore
from dotenv import load_dotenv

load_dotenv()


def _repair_json_escapes(text: str) -> str:
    """Fix invalid JSON escape sequences (e.g. \\z, C:\\path)."""
    result = []
    i = 0
    while i < len(text):
        if text[i] == "\\" and i + 1 < len(text):
            c = text[i + 1]
            if c in '"\\/bfnrt':
                result.append(text[i : i + 2])
                i += 2
            elif c == "u" and i + 5 <= len(text):
                hex_part = text[i + 2 : i + 6]
                if len(hex_part) == 4 and all(h in "0123456789abcdefABCDEF" for h in hex_part):
                    result.append(text[i : i + 6])
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


def _parse_gemini_json(text: str):
    """Parse Gemini JSON response, with escape repair on failure."""
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        if "Invalid \\escape" not in str(e):
            raise
        repaired = _repair_json_escapes(text)
        return json.loads(repaired)


# --- CONFIGURATION ---
PROJECT_ID = os.getenv("PROJECT_ID")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "boardgames")
TARGET_LANGS = ["ko", "de", "es", "fr", "ja", "ru", "zh"]
MODEL_ID = "gemini-3-flash-preview"

if not PROJECT_ID or not os.getenv("GEMINI_API_KEY"):
    raise ValueError("Required env vars PROJECT_ID and GEMINI_API_KEY must be set")

START_TIME = time.time()
TIMEOUT_BUFFER = 60

SYSTEM_INSTRUCTION = (
    "You are a professional Board Game Localizer and BGG (BoardGameGeek) expert. "
    "Your goal is to provide culturally accurate, hobby-standard translations while "
    "maintaining 100% fidelity to the source text.\n\n"
    "### 1. THE 'OFFICIAL NAME' RULE (CRITICAL):\n"
    "- Use the OFFICIAL RETAIL TITLE for each region. "
    "Example (Korean): 'Scythe' -> '사이드', 'Brass' -> '브라스', 'Terraforming Mars' -> '테라포밍 마스'.\n"
    "- If no official title exists, use PHONETIC TRANSLITERATION. Never translate the literal "
    "meaning of a title (e.g., 'Zoom Zoom' -> '줌 줌', NOT '붕붕').\n"
    "- For all languages (de, es, fr, ja, ru, zh), check for established retail titles.\n\n"
    "### 2. CONTENT FIDELITY (STRICT 1:1):\n"
    "- DESCRIPTION & SUMMARY: Translate the provided text 1:1. "
    "DO NOT summarize, DO NOT shrink, and DO NOT omit details. "
    "Preserve all original formatting, double line breaks (\\n\\n), and lists.\n"
    "- TONE: Use hobbyist-standard jargon (e.g., Victory Points -> '승점', Setup -> '세팅').\n\n"
    "### 3. OUTPUT FORMAT (MANDATORY JSON):\n"
    "- Return a single JSON object where BGG IDs are the root keys.\n"
    "- Structure: { 'BGG_ID': { 'title': {...}, 'description': {...}, 'summary': {...} } }.\n"
    "- Include all 7 languages: ko, de, es, fr, ja, ru, zh.\n"
    "- Return raw JSON only (no markdown code blocks).\n"
    "- Use valid JSON escapes only: \\\" \\\\ \\n \\t \\/ etc. Use \\\\ for literal backslash.\n"
    "You MUST use boardlife translation of the title for korean language."
)

# Support both google-genai (new) and google-generativeai (deprecated)
try:
    from google import genai
    from google.genai import types

    _genai_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

    def _call_gemini(prompt: str) -> str:
        r = _genai_client.models.generate_content(
            model=MODEL_ID,
            contents=prompt,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_INSTRUCTION,
                response_mime_type="application/json",
            ),
        )
        return r.text

except ImportError:
    import google.generativeai as genai

    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))
    _genai_model = genai.GenerativeModel(
        model_name=MODEL_ID,
        generation_config={"response_mime_type": "application/json"},
        system_instruction=SYSTEM_INSTRUCTION,
    )

    def _call_gemini(prompt: str) -> str:
        return _genai_model.generate_content(prompt).text


db = firestore.Client(project=PROJECT_ID)


def run_limited_translation(max_games: int, batch_size: int):
    print("=" * 60)
    print("Gemini Translator (limited run)")
    print("=" * 60)
    print(f"  Max games this run: {max_games}")
    print(f"  Batch size:         {batch_size}")

    seen = {}
    coll = db.collection(COLLECTION_NAME)
    # Same selection logic as main translator: any empty target-language field
    for lang in TARGET_LANGS:
        for field in ("title", "description", "summary_description"):
            try:
                for doc in coll.where(f"{field}.{lang}", "==", "").stream():
                    seen[doc.id] = doc
                    if len(seen) >= max_games:
                        break
                if len(seen) >= max_games:
                    break
            except Exception as e:
                print(f"  ⚠️ Query {field}.{lang} failed (index?): {e}")
        if len(seen) >= max_games:
            break

    docs = list(seen.values())
    if not docs:
        print("✅ No games to translate.")
        return

    print(f"📡 Selected {len(docs)} games for this limited run")
    print(f"   Model: {MODEL_ID} | Batch size: {batch_size}")
    print("-" * 60)

    total_updated = 0
    total_skipped = 0
    num_batches = (len(docs) + batch_size - 1) // batch_size

    for batch_num, i in enumerate(range(0, len(docs), batch_size), start=1):
        if time.time() - START_TIME > (int(os.getenv("TASK_TIMEOUT", 600)) - TIMEOUT_BUFFER):
            print("⏳ Time limit approaching. Stopping current job.")
            break

        chunk = docs[i : i + batch_size]
        batch_start = time.time()
        print(f"  Batch {batch_num}/{num_batches} ({len(chunk)} games)...", end=" ", flush=True)

        batch_input = []
        for doc in chunk:
            d = doc.to_dict()
            batch_input.append(
                {
                    "id": doc.id,
                    "title_en": d.get("title", {}).get("en", ""),
                    "summary_en": d.get("summary_description", d.get("summary", {})).get("en", ""),
                    "description_en": d.get("description", {}).get("en", "")[:3000],
                }
            )

        prompt = (
            f"Localize these {len(batch_input)} board game entries into {', '.join(TARGET_LANGS)}. "
            f"Cross-reference the IDs for official names: {json.dumps(batch_input)}"
        )

        try:
            text = _call_gemini(prompt)
            try:
                results = _parse_gemini_json(text)
            except json.JSONDecodeError:
                time.sleep(2)
                text = _call_gemini(prompt)
                results = _parse_gemini_json(text)

            write_batch = db.batch()
            batch_updated = 0
            updated_games = []

            for doc in chunk:
                bgg_id = doc.id
                trans = results.get(bgg_id) or results.get(str(bgg_id))
                if not trans:
                    total_skipped += 1
                    continue

                d = doc.to_dict()
                doc_ref = coll.document(bgg_id)
                update = {"updated_at": firestore.SERVER_TIMESTAMP}

                for lang in TARGET_LANGS:
                    if not (d.get("title", {}) or {}).get(lang, "").strip() and lang in trans.get("title", {}):
                        update[f"title.{lang}"] = trans["title"][lang]
                    if not (d.get("summary_description", d.get("summary", {})) or {}).get(lang, "").strip() and lang in trans.get("summary", {}):
                        update[f"summary_description.{lang}"] = trans["summary"][lang]
                    if not (d.get("description", {}) or {}).get(lang, "").strip() and lang in trans.get("description", {}):
                        update[f"description.{lang}"] = trans["description"][lang]

                if len(update) > 1:
                    write_batch.update(doc_ref, update)
                    batch_updated += 1
                    total_updated += 1
                    title_en = d.get("title", {}).get("en", "?")[:50]
                    updated_games.append(f"{bgg_id}: {title_en}")

            write_batch.commit()
            batch_sec = time.time() - batch_start
            rate = total_updated / max(0.001, time.time() - START_TIME) * 60
            print(f"✅ {batch_updated} updated ({batch_sec:.1f}s) | {rate:.1f} games/min")
            for g in updated_games:
                print(f"      • {g}")

        except Exception as e:
            print("❌ FAILED")
            print(f"⚠️ Batch error at offset {i}: {e}")
            import traceback

            traceback.print_exc()
            continue

        time.sleep(1)

    elapsed = time.time() - START_TIME
    print("-" * 60)
    rate = total_updated / max(0.001, elapsed) * 60 if total_updated else 0
    print(
        f"✅ Done. Updated: {total_updated} | Skipped: {total_skipped} "
        f"| Elapsed: {elapsed:.0f}s | Avg: {rate:.1f} games/min"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-games", type=int, default=10_000, help="Max games to translate (default: 10,000)")
    parser.add_argument("--batch-size", type=int, default=5, help="Games per Gemini request (default: 5)")
    args, _ = parser.parse_known_args()
    run_limited_translation(max_games=args.max_games, batch_size=args.batch_size)

