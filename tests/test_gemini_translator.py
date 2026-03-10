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

    from google import genai
    from google.genai import types

    client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

    system_instruction=(
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
        "- Structure: { 'BGG_ID': { 'title': {…}, 'description': {…}, 'summary': {…} } }.\n"
        "- Include all 7 languages: ko, de, es, fr, ja, ru, zh.\n"
        "- Return raw JSON only (no markdown code blocks)."
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
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=system_instruction,
                    response_mime_type="application/json",
                ),
            )
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
