import os
import json
import time
import google.generativeai as genai
from google.cloud import firestore
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
PROJECT_ID = os.getenv("PROJECT_ID")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "boardgames")
TARGET_LANGS = ["ko", "de", "es", "fr", "ja", "ru", "zh"]
BATCH_SIZE = 5  # Number of games to translate in ONE API call (Cost Optimization)

genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

# Use system_instruction to save tokens on repeated instructions
model = genai.GenerativeModel(
    model_name='gemini-1.5-flash',
    system_instruction=(
        "You are a board game expert. Translate game titles and descriptions. "
        "Return a JSON object where the keys are the BGG IDs provided. "
        "Summary must be exactly 2 sentences."
    )
)

db = firestore.Client(project=PROJECT_ID)

def translate_batch(games):
    """Translates a list of games in a single prompt to save tokens."""
    # Build a compact input for the AI
    input_data = []
    for g in games:
        input_data.append({
            "id": g['id'],
            "t": g['title'],
            "d": g['desc']
        })

    prompt = f"Translate into {', '.join(TARGET_LANGS)}: {json.dumps(input_data)}"
    
    try:
        response = model.generate_content(prompt)
        return json.loads(response.text.replace('```json', '').replace('```', '').strip())
    except Exception as e:
        print(f"⚠️ Batch Error: {e}")
        return None

def run_optimized_translation():
    # Query for untranslated games
    docs = list(db.collection(COLLECTION_NAME).where("title.ko", "==", "").limit(100).stream())
    
    if not docs:
        print("✅ No new games to translate.")
        return

    # Group into batches of 5
    for i in range(0, len(docs), BATCH_SIZE):
        chunk = docs[i : i + BATCH_SIZE]
        batch_input = []
        
        for doc in chunk:
            d = doc.to_dict()
            batch_input.append({
                "id": doc.id,
                "title": d.get('title', {}).get('en', ''),
                "desc": d.get('description', {}).get('en', '')[:1500] # Limit chars to save tokens
            })

        results = translate_batch(batch_input)
        
        if results:
            # Write results to Firestore
            write_batch = db.batch()
            for bgg_id, trans in results.items():
                update = {}
                for lang in TARGET_LANGS:
                    update[f"title.{lang}"] = trans['title'].get(lang, "")
                    update[f"summary_description.{lang}"] = trans['summary'].get(lang, "")
                    update[f"description.{lang}"] = trans['description'].get(lang, "")
                
                update["updated_at"] = firestore.SERVER_TIMESTAMP
                write_batch.update(db.collection(COLLECTION_NAME).document(bgg_id), update)
            
            write_batch.commit()
            print(f"Successfully translated batch of {len(chunk)}")
            time.sleep(4) # Respect rate limits

if __name__ == "__main__":
    run_optimized_translation()