#!/usr/bin/env python3
"""
Test gemini_translator logic locally.
Run: python tests/test_gemini_translator.py [--limit N]

Uses tests/output_extractor.json (run test_bgg_extractor.py first).
Outputs to tests/output_translations.json.
Uses GEMINI_API_KEY from .env.
"""
import os
import sys
import json
import time

# Add project root and load .env
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

_env_path = os.path.join(project_root, ".env")
if os.path.exists(_env_path):
    with open(_env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

# --- Test config ---
TEST_EXTRACTOR_JSON = os.path.join(project_root, "tests", "output_extractor.json")
TEST_OUTPUT_JSON = os.path.join(project_root, "tests", "output_translations.json")
TARGET_LANGS = ["ko", "de", "es", "fr", "ja", "ru", "zh"]
BATCH_SIZE = 5
DEFAULT_LIMIT = 5  # Number of games to translate (to limit API usage)


def run_test(limit=DEFAULT_LIMIT):
    print("=" * 60)
    print("Gemini Translator Test")
    print("=" * 60)

    if not os.getenv("GEMINI_API_KEY"):
        print("\n❌ GEMINI_API_KEY required. Set it in .env")
        sys.exit(1)

    if not os.path.exists(TEST_EXTRACTOR_JSON):
        print(f"\n❌ Extractor output not found: {TEST_EXTRACTOR_JSON}")
        print("   Run test_bgg_extractor.py first (with --json for full output).")
        sys.exit(1)

    import google.generativeai as genai

    genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

    generation_config = {"response_mime_type": "application/json"}

    model = genai.GenerativeModel(
        model_name="gemini-2.5-flash",
        generation_config=generation_config,
        system_instruction=(
            "You are a board game expert. Translate titles, descriptions, and summaries into all target languages. "
            "Return a JSON object where BGG IDs are the root keys. "
            "Each game object must contain three objects, each with lang keys ko, de, es, fr, ja, ru, zh: "
            "'title' (translated title per language), "
            "'description' (full translation of the description per language), "
            "'summary' (exactly 2 sentences per language). "
            "Every field must be an object with keys: ko, de, es, fr, ja, ru, zh."
        ),
    )

    # Load extractor output
    with open(TEST_EXTRACTOR_JSON, encoding="utf-8") as f:
        docs = json.load(f)

    # Limit to first N games
    docs = docs[:limit]
    print(f"\n📥 Translating {len(docs)} games...")

    all_translations = {}

    for i in range(0, len(docs), BATCH_SIZE):
        chunk = docs[i : i + BATCH_SIZE]
        batch_input = []

        for doc in chunk:
            batch_input.append(
                {
                    "id": doc["bgg_id"],
                    "title": doc.get("title", {}).get("en", ""),
                    "desc": doc.get("description", {}).get("en", "")[:2500],
                }
            )

        prompt = f"Translate these games into {', '.join(TARGET_LANGS)}: {json.dumps(batch_input)}"

        try:
            response = model.generate_content(prompt)
            results = json.loads(response.text)
            all_translations.update(results)
            print(f"   ✅ Batch {i // BATCH_SIZE + 1}: {len(results)} games translated")
        except Exception as e:
            print(f"   ❌ Batch error: {e}")
            raise

        time.sleep(2)

    # Write output
    os.makedirs(os.path.dirname(TEST_OUTPUT_JSON), exist_ok=True)
    with open(TEST_OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(all_translations, f, indent=2, ensure_ascii=False)

    print(f"\n✍️  Output: {TEST_OUTPUT_JSON}")
    print(f"\n📊 Summary: {len(all_translations)} games translated")
    print("\n🎉 Translator test complete.")


if __name__ == "__main__":
    limit = DEFAULT_LIMIT
    args = sys.argv[1:]
    for i, arg in enumerate(args):
        if arg.startswith("--limit="):
            limit = int(arg.split("=")[1])
        elif arg in ("-n", "--limit") and i + 1 < len(args):
            limit = int(args[i + 1])
    run_test(limit=limit)
