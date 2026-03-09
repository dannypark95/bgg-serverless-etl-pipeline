import os
import json
import time
import google.generativeai as genai
from google.cloud import firestore
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "boardgames")
TARGET_LANGS = ["ko", "de", "es", "fr", "ja", "ru", "zh"]
BATCH_SIZE = 5 
# Start time to monitor for Cloud Run 600s/3600s limits
START_TIME = time.time()
TIMEOUT_BUFFER = 60 # Stop 1 minute early to allow for cleanup

genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

# Strict JSON Schema to ensure the AI doesn't hallucinate key names
generation_config = {
    "response_mime_type": "application/json",
}

model = genai.GenerativeModel(
    model_name='gemini-1.5-flash',
    generation_config=generation_config,
    system_instruction=(
        "You are a board game expert. Translate titles and descriptions. "
        "Return a JSON object where BGG IDs are the root keys. "
        "Each game object must contain: 'title' (object with lang keys), "
        "'description' (full translation), and 'summary' (exactly 2 sentences). "
        "Language keys must be: ko, de, es, fr, ja, ru, zh."
    )
)

db = firestore.Client(project=PROJECT_ID)

def run_optimized_translation():
    # Increase limit to 500; the script will loop through them 5 at a time
    docs = list(db.collection(COLLECTION_NAME).where("title.ko", "==", "").limit(500).stream())
    
    if not docs:
        print("✅ No new games to translate.")
        return

    print(f"📡 Found {len(docs)} games for translation...")

    for i in range(0, len(docs), BATCH_SIZE):
        # ⏱️ SAFETY CHECK: Exit before timeout
        # This allows the Workflow Loop to pick up where we left off
        if time.time() - START_TIME > (int(os.getenv("TASK_TIMEOUT", 600)) - TIMEOUT_BUFFER):
            print("⏳ Time limit approaching. Stopping current job to allow next loop iteration.")
            break

        chunk = docs[i : i + BATCH_SIZE]
        batch_input = []
        
        for doc in chunk:
            d = doc.to_dict()
            batch_input.append({
                "id": doc.id,
                "title": d.get('title', {}).get('en', ''),
                "desc": d.get('description', {}).get('en', '')[:1000] 
            })

        prompt = f"Translate these games into {', '.join(TARGET_LANGS)}: {json.dumps(batch_input)}"
        
        try:
            response = model.generate_content(prompt)
            # No need for manual string stripping with response_mime_type: "application/json"
            results = json.loads(response.text)
            
            write_batch = db.batch()
            for bgg_id, trans in results.items():
                update = {}
                for lang in TARGET_LANGS:
                    # Defensive programming: check if key exists in AI response
                    if lang in trans.get('title', {}):
                        update[f"title.{lang}"] = trans['title'][lang]
                    if lang in trans.get('summary', {}):
                        update[f"summary_description.{lang}"] = trans['summary'][lang]
                    if lang in trans.get('description', {}):
                        update[f"description.{lang}"] = trans['description'][lang]
                
                update["updated_at"] = firestore.SERVER_TIMESTAMP
                write_batch.update(db.collection(COLLECTION_NAME).document(bgg_id), update)
            
            write_batch.commit()
            print(f"✅ Translated batch ({i+BATCH_SIZE}/{len(docs)})")
            
        except Exception as e:
            print(f"⚠️ Batch Error at index {i}: {e}")
            # We don't save progress for this batch, so it stays "" and gets picked up next time
            continue

        time.sleep(2) # Modest sleep to stay under rate limits

if __name__ == "__main__":
    run_optimized_translation()