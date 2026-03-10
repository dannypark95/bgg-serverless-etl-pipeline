import os
import json
import time
from google import genai
from google.genai import types
from google.cloud import firestore
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
PROJECT_ID = os.getenv("PROJECT_ID")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "boardgames")
TARGET_LANGS = ["ko", "de", "es", "fr", "ja", "ru", "zh"]
# Use Gemini 3 Flash for the best balance of speed and hobby-specific knowledge
MODEL_ID = "gemini-3-flash-preview"

if not PROJECT_ID or not os.getenv("GEMINI_API_KEY"):
    raise ValueError("Required env vars PROJECT_ID and GEMINI_API_KEY must be set")

BATCH_SIZE = 5
START_TIME = time.time()
TIMEOUT_BUFFER = 60

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# --- THE "BOARD GAME EXPERT" INSTRUCTIONS ---
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
    "- Return raw JSON only (no markdown code blocks)."
)

db = firestore.Client(project=PROJECT_ID)

def run_localized_translation():
    # Fetch games where ANY target language field is empty (title, description, or summary)
    seen = {}
    coll = db.collection(COLLECTION_NAME)
    for lang in TARGET_LANGS:
        for field in ("title", "description", "summary_description"):
            try:
                for doc in coll.where(f"{field}.{lang}", "==", "").limit(500).stream():
                    seen[doc.id] = doc
            except Exception:
                # Firestore may require composite index for description/summary_description
                pass
    docs = list(seen.values())

    if not docs:
        print("✅ No new games to translate.")
        return

    print(f"📡 Found {len(docs)} games needing translation. Starting localization with {MODEL_ID}...")

    for i in range(0, len(docs), BATCH_SIZE):
        # Safety check for Cloud Run timeout
        if time.time() - START_TIME > (int(os.getenv("TASK_TIMEOUT", 600)) - TIMEOUT_BUFFER):
            print("⏳ Time limit approaching. Stopping current job.")
            break

        chunk = docs[i : i + BATCH_SIZE]
        batch_input = []
        
        for doc in chunk:
            d = doc.to_dict()
            batch_input.append({
                "id": doc.id,
                "title_en": d.get('title', {}).get('en', ''),
                "summary_en": d.get('summary_description', d.get('summary', {})).get('en', ''),
                "description_en": d.get('description', {}).get('en', '')[:3000]
            })

        prompt = (
            f"Localize these {len(batch_input)} board game entries into {', '.join(TARGET_LANGS)}. "
            f"Cross-reference the IDs for official names: {json.dumps(batch_input)}"
        )
        
        try:
            response = client.models.generate_content(
                model=MODEL_ID,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_INSTRUCTION,
                    response_mime_type="application/json",
                ),
            )
            results = json.loads(response.text)
            
            write_batch = db.batch()
            for idx, doc in enumerate(chunk):
                bgg_id = doc.id
                trans = results.get(bgg_id) or results.get(str(bgg_id))
                if not trans:
                    continue
                d = doc.to_dict()
                doc_ref = db.collection(COLLECTION_NAME).document(bgg_id)
                update = {"updated_at": firestore.SERVER_TIMESTAMP}

                for lang in TARGET_LANGS:
                    # Only fill empty fields - don't overwrite existing translations
                    if not (d.get("title", {}) or {}).get(lang, "").strip() and lang in trans.get("title", {}):
                        update[f"title.{lang}"] = trans["title"][lang]
                    if not (d.get("summary_description", d.get("summary", {})) or {}).get(lang, "").strip() and lang in trans.get("summary", {}):
                        update[f"summary_description.{lang}"] = trans["summary"][lang]
                    if not (d.get("description", {}) or {}).get(lang, "").strip() and lang in trans.get("description", {}):
                        update[f"description.{lang}"] = trans["description"][lang]

                if len(update) > 1:  # more than just updated_at
                    write_batch.update(doc_ref, update)
                    print(f"   ↳ ID {bgg_id} -> filled empty fields")
            
            write_batch.commit()
            print(f"✅ Batch ({i+BATCH_SIZE}/{len(docs)}) committed.")
            
        except Exception as e:
            print(f"⚠️ Batch Error at index {i}: {e}")
            continue

        time.sleep(1) # Gemini 3 Flash is fast; 1s is enough to stay safe

if __name__ == "__main__":
    run_localized_translation()