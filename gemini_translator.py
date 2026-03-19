import os
import json
import time
from google.cloud import firestore
from google.cloud.firestore_v1.base_query import FieldFilter
from dotenv import load_dotenv

load_dotenv()


def _repair_json_escapes(text):
    """Fix invalid JSON escape sequences (e.g. \\z, C:\\path) that Gemini sometimes returns."""
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


def _parse_gemini_json(text):
    """Parse Gemini JSON response, with retry and escape repair on failure."""
    # Sometimes the client library can return a response with `text=None`
    # (e.g. empty response, transport error). In that case we treat it as an
    # empty result so the current batch is simply skipped instead of failing.
    if text is None:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        if "Invalid \\escape" not in str(e):
            raise
        repaired = _repair_json_escapes(text)
        return json.loads(repaired)

# --- CONFIGURATION ---
PROJECT_ID = os.getenv("PROJECT_ID")

# Sample mode: when workflow_sample passes SAMPLE_MAX_GAMES, point translations
# at the test collection by default so they don't mutate production docs.
SAMPLE_MODE = bool(os.getenv("SAMPLE_MAX_GAMES"))
_default_collection = "test_boardgames" if SAMPLE_MODE else "boardgames"

COLLECTION_NAME = os.getenv("COLLECTION_NAME", _default_collection)
TARGET_LANGS = ["ko", "de", "es", "fr", "ja", "ru", "zh"]
# Use Gemini 2.5 Flash for the best balance of speed and hobby-specific knowledge
MODEL_ID = "gemini-2.5-flash-preview"

if not PROJECT_ID or not os.getenv("GEMINI_API_KEY"):
    raise ValueError("Required env vars PROJECT_ID and GEMINI_API_KEY must be set")

BATCH_SIZE = 5
QUERY_LIMIT = int(os.getenv("TRANSLATION_QUERY_LIMIT", "5000"))  # Per (field, lang) query; 500 was too low
START_TIME = time.time()

# --- THE "BOARD GAME EXPERT" INSTRUCTIONS ---
SYSTEM_INSTRUCTION = (
    "You are a professional Board Game Localizer and BGG (BoardGameGeek) expert. "
    "Your goal is to provide culturally accurate, hobby-standard translations while "
    "maintaining 100% fidelity to the source text.\n\n"

    "### 1. THE 'OFFICIAL NAME' RULE (CRITICAL):\n"
    "- Use the OFFICIAL RETAIL TITLE for each region. "
    "Example (Korean): 'Scythe' -> '사이드', 'Brass' -> '브라스', 'Terraforming Mars' -> '테라포밍 마스'.\n"
    "For Korean (ko): if title.ko is requested (missing), use PHONETIC TRANSLITERATION from the English title. "
    "Do not create Korean titles when title.ko is not requested.\n"
    "- If no official title exists in other languages, use PHONETIC TRANSLITERATION. Never translate the literal "
    "meaning of a title (e.g., 'Zoom Zoom' -> '줌 줌', NOT '붕붕').\n"
    "- For all languages (de, es, fr, ja, ru, zh), check for established retail titles.\n\n"

    "### 2. CONTENT FIDELITY (STRICT 1:1):\n"
    "- DESCRIPTION: Translate the provided text 1:1. "
    "DO NOT summarize, DO NOT shrink, and DO NOT omit details. "
    "Preserve all original formatting, double line breaks (\\n\\n), and lists.\n"
    "- TONE: Use hobbyist-standard jargon (e.g., Victory Points -> '승점', Setup -> '세팅').\n\n"

    "### 3. OUTPUT FORMAT (MANDATORY JSON ONLY):\n"
    "- Return a single JSON object where BGG IDs are the root keys.\n"
    "- Structure: { 'BGG_ID': { 'title': {...}, 'description': {...} } }.\n"
    "- Only include fields and languages explicitly requested in the prompt.\n"
    "- Return only JSON, no commentary, no explanation, no markdown.\n"
    "- Use valid JSON escapes only: \\\" \\\\ \\n \\t \\/ etc. Use \\\\ for literal backslash."
)

# Support both google-genai (new) and google-generativeai (deprecated)
try:
    from google import genai
    from google.genai import types
    _genai_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

    def _call_gemini(prompt):
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

    def _call_gemini(prompt):
        return _genai_model.generate_content(prompt).text

db = firestore.Client(project=PROJECT_ID)


def _safe_get(mapping, key):
    if not isinstance(mapping, dict):
        return ""
    return (mapping.get(key) or "").strip()

def run_localized_translation():
    print("=" * 60)
    print("Gemini Translator")
    print("=" * 60)

    # Fetch games where ANY target language field is empty (title or description).
    seen = {}
    coll = db.collection(COLLECTION_NAME)
    for lang in TARGET_LANGS:
        for field in ("title", "description"):
            try:
                for doc in coll.where(filter=FieldFilter(f"{field}.{lang}", "==", "")).limit(QUERY_LIMIT).stream():
                    seen[doc.id] = doc
            except Exception as e:
                print(f"  ⚠️ Query {field}.{lang} failed (index?): {e}")
    docs = list(seen.values())

    # Sample run: limit to small set (e.g. workflow passes SAMPLE_MAX_GAMES=20)
    sample_max = os.getenv("SAMPLE_MAX_GAMES")
    if sample_max:
        n = int(sample_max)
        docs = docs[:n]
        print(f"⚠️ SAMPLE MODE: limiting to first {len(docs)} games (SAMPLE_MAX_GAMES={sample_max})")

    if not docs:
        print("✅ No new games to translate.")
        return

    print(f"📡 Found {len(docs)} games needing translation (query limit: {QUERY_LIMIT} per field/lang)")
    print(f"   Model: {MODEL_ID} | Batch size: {BATCH_SIZE}")
    print("-" * 60)

    total_updated = 0
    total_skipped = 0
    batch_num = 0
    num_batches = (len(docs) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(docs), BATCH_SIZE):
        batch_num += 1
        chunk = docs[i : i + BATCH_SIZE]
        batch_start = time.time()

        # Build per-game instructions that only request missing fields/langs.
        request_games = []
        for doc in chunk:
            d = doc.to_dict() or {}
            title_map = d.get("title") or {}
            desc_map = d.get("description") or {}

            need_title_langs = []
            need_desc_langs = []
            for lang in TARGET_LANGS:
                if not _safe_get(title_map, lang):
                    need_title_langs.append(lang)
                if not _safe_get(desc_map, lang):
                    need_desc_langs.append(lang)

            if not need_title_langs and not need_desc_langs:
                continue

            request_games.append(
                {
                    "id": doc.id,
                    "source": {
                        "title_en": _safe_get(title_map, "en"),
                        "description_en": _safe_get(desc_map, "en")[:3000],
                    },
                    "need": {
                        "title": need_title_langs,
                        "description": need_desc_langs,
                    },
                }
            )

        if not request_games:
            print(f"  Batch {batch_num}/{num_batches} (0 games needing work)... skipping Gemini call.")
            continue

        print(f"  Batch {batch_num}/{num_batches} ({len(request_games)} games to translate)...", end=" ", flush=True)

        prompt = (
            "You will receive a list of board games with their English title and description, "
            "and, for each game, a list of which fields and languages still need translation.\n"
            "For each game, translate ONLY the requested fields and languages.\n"
            "If Korean title (ko) is requested, translate it using PHONETIC TRANSLITERATION from the English title.\n"
            "Return a single JSON object of the form:\n"
            "{\n"
            '  \"BGG_ID\": {\n'
            '    \"title\": { \"LANG\": \"...\" },\n'
            '    \"description\": { \"LANG\": \"...\" }\n'
            "  },\n"
            "  ...\n"
            "}\n"
            "Only include languages and fields that were requested in the input. "
            "Return only JSON, no commentary, no explanation.\n\n"
            f"REQUEST:\n{json.dumps(request_games, ensure_ascii=False)}"
        )

        try:
            text = _call_gemini(prompt)
            try:
                results = _parse_gemini_json(text)
            except json.JSONDecodeError:
                # Retry once - fresh response may be valid
                time.sleep(2)
                text = _call_gemini(prompt)
                results = _parse_gemini_json(text)

            write_batch = db.batch()
            batch_updated = 0
            updated_games = []
            for idx, doc in enumerate(chunk):
                bgg_id = doc.id
                trans = results.get(bgg_id) or results.get(str(bgg_id))
                if not trans:
                    total_skipped += 1
                    continue
                d = doc.to_dict()
                doc_ref = db.collection(COLLECTION_NAME).document(bgg_id)
                update = {"updated_at": firestore.SERVER_TIMESTAMP}

                for lang in TARGET_LANGS:
                    # Only fill empty fields - don't overwrite existing translations.
                    if (
                        not (d.get("title", {}) or {}).get(lang, "").strip()
                        and lang in (trans.get("title") or {})
                    ):
                        update[f"title.{lang}"] = trans["title"][lang]
                    if (
                        not (d.get("description", {}) or {}).get(lang, "").strip()
                        and lang in (trans.get("description") or {})
                    ):
                        update[f"description.{lang}"] = trans["description"][lang]

                if len(update) > 1:  # more than just updated_at
                    write_batch.update(doc_ref, update)
                    batch_updated += 1
                    total_updated += 1
                    title_en = d.get("title", {}).get("en", "?")[:50]
                    updated_games.append(f"{bgg_id}: {title_en}")

            write_batch.commit()
            batch_sec = time.time() - batch_start
            rate = total_updated / max(0.001, time.time() - START_TIME) * 60  # games/min
            print(f"✅ {batch_updated} updated ({batch_sec:.1f}s) | {rate:.1f} games/min")
            for g in updated_games:
                print(f"      • {g}")

        except Exception as e:
            print(f"❌ FAILED")
            print(f"⚠️ Batch error at index {i}: {e}")
            import traceback
            traceback.print_exc()
            continue

        time.sleep(1)  # Gemini 3 Flash is fast; 1s is enough to stay safe

    elapsed = time.time() - START_TIME
    print("-" * 60)
    rate = total_updated / max(0.001, elapsed) * 60 if total_updated else 0
    print(f"✅ Done. Updated: {total_updated} | Skipped: {total_skipped} | Elapsed: {elapsed:.0f}s | Avg: {rate:.1f} games/min")


if __name__ == "__main__":
    run_localized_translation()