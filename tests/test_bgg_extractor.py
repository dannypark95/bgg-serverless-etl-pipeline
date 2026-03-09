#!/usr/bin/env python3
"""
Test bgg_extractor logic locally.
Run: python tests/test_bgg_extractor.py [--json]

Uses tests/output_master_list.csv (run test_bgg_csv.py first).
Outputs to tests/output_extractor.csv (or .json with --json).
"""
import os
import sys
import csv
import time
import json
import urllib.request
import xml.etree.ElementTree as ET

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
TEST_MASTER_LIST = os.path.join(project_root, "tests", "output_master_list.csv")
TEST_OUTPUT_CSV = os.path.join(project_root, "tests", "output_extractor.csv")
TEST_OUTPUT_JSON = os.path.join(project_root, "tests", "output_extractor.json")
CHUNK_SIZE = 20
SLEEP_SECONDS = 5
LANGUAGES = ["en", "ko", "de", "es", "fr", "ja", "ru", "zh"]


def generate_localized_dict(content, default_lang="en"):
    ldict = {lang: "" for lang in LANGUAGES}
    ldict[default_lang] = content if content else ""
    return ldict


def fetch_and_parse_chunk(chunk):
    """Fetch BGG API and parse into doc_data. Same logic as bgg_extractor."""
    bgg_ids = [game["bgg_id"] for game in chunk]
    chunk_dict = {game["bgg_id"]: game for game in chunk}
    url = f"https://boardgamegeek.com/xmlapi2/thing?id={','.join(bgg_ids)}&stats=1"

    headers = {
        "User-Agent": "BoardGameCatalog/1.0 (https://boardgamegeek.com/applications)",
    }
    bgg_token = os.getenv("BGG_TOKEN")
    if bgg_token:
        headers["Authorization"] = f"Bearer {bgg_token}"

    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as response:
        root = ET.fromstring(response.read())

    docs = []
    for item in root.findall("item"):
        bgg_id = item.get("id")
        csv_row = chunk_dict.get(bgg_id, {})

        title_elem = item.find("name[@type='primary']")
        title_en = title_elem.get("value") if title_elem is not None else "Unknown"

        desc_elem = item.find("description")
        desc_en = desc_elem.text if desc_elem is not None else ""
        summary_en = desc_en[:250] + "..." if len(desc_en) > 250 else desc_en

        stats = item.find("statistics/ratings")
        avg_elem = stats.find("average") if stats is not None else None
        weight_elem = stats.find("averageweight") if stats is not None else None
        rating = (
            round(float(avg_elem.get("value")), 2)
            if avg_elem is not None and avg_elem.get("value")
            else 0.0
        )
        weight = (
            round(float(weight_elem.get("value")), 2)
            if weight_elem is not None and weight_elem.get("value")
            else 0.0
        )

        doc_data = {
            "bgg_id": bgg_id,
            "title": generate_localized_dict(title_en),
            "description": generate_localized_dict(desc_en),
            "summary_description": generate_localized_dict(summary_en),
            "year_published": (
                int(item.find("yearpublished").get("value"))
                if item.find("yearpublished") is not None
                else 0
            ),
            "min_players": (
                int(item.find("minplayers").get("value"))
                if item.find("minplayers") is not None
                else 0
            ),
            "max_players": (
                int(item.find("maxplayers").get("value"))
                if item.find("maxplayers") is not None
                else 0
            ),
            "rating": rating,
            "weight": weight,
            "image_url": (
                item.find("thumbnail").text if item.find("thumbnail") is not None else ""
            ),
            "is_expansion": csv_row.get("is_expansion") == "True",
        }
        docs.append(doc_data)

    return docs


def doc_to_csv_row(doc):
    """Flatten doc for CSV output."""
    return {
        "bgg_id": doc["bgg_id"],
        "title": doc["title"]["en"],
        "description": doc["description"]["en"][:200] + "..." if len(doc["description"]["en"]) > 200 else doc["description"]["en"],
        "year_published": doc["year_published"],
        "min_players": doc["min_players"],
        "max_players": doc["max_players"],
        "rating": doc["rating"],
        "weight": doc["weight"],
        "image_url": doc["image_url"][:80] + "..." if len(doc["image_url"]) > 80 else doc["image_url"],
        "is_expansion": doc["is_expansion"],
    }


def run_test(output_json=False):
    print("=" * 60)
    print("BGG Extractor Test")
    print("=" * 60)

    if not os.path.exists(TEST_MASTER_LIST):
        print(f"\n❌ Master list not found: {TEST_MASTER_LIST}")
        print("   Run test_bgg_csv.py first to generate it.")
        sys.exit(1)

    # Read master list
    games = []
    with open(TEST_MASTER_LIST, encoding="utf-8") as f:
        games = list(csv.DictReader(f))

    print(f"\n📥 Loaded {len(games)} games from master list")

    chunks = [games[i : i + CHUNK_SIZE] for i in range(0, len(games), CHUNK_SIZE)]
    print(f"   {len(chunks)} chunks to process\n")

    all_docs = []

    for i, chunk in enumerate(chunks):
        print(f"🔄 Chunk {i + 1}/{len(chunks)}: {len(chunk)} games...")

        try:
            docs = fetch_and_parse_chunk(chunk)
            all_docs.extend(docs)
            time.sleep(SLEEP_SECONDS)
        except Exception as e:
            print(f"   ❌ {e}")
            raise

    # Write output
    os.makedirs(os.path.dirname(TEST_OUTPUT_CSV), exist_ok=True)

    if output_json:
        with open(TEST_OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(all_docs, f, indent=2, ensure_ascii=False)
        print(f"\n✍️  Output: {TEST_OUTPUT_JSON}")
    else:
        with open(TEST_OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            if all_docs:
                fieldnames = list(doc_to_csv_row(all_docs[0]).keys())
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for doc in all_docs:
                    writer.writerow(doc_to_csv_row(doc))
        print(f"\n✍️  Output: {TEST_OUTPUT_CSV}")

    print(f"\n📊 Summary: {len(all_docs)} games extracted")
    print("\n🎉 Extractor test complete.")


if __name__ == "__main__":
    output_json = "--json" in sys.argv or "-j" in sys.argv
    run_test(output_json=output_json)
