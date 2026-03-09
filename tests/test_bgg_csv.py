#!/usr/bin/env python3
"""
Test bgg_csv logic locally with a small fixture (10 games).
Run: python tests/test_bgg_csv.py

Uses: Gloomhaven, Ark Nova, Brass: Birmingham, Pandemic Legacy + 6 less popular games.
Calls real BGG API to fetch expansion links.
Loads BGG_TOKEN from .env if present.
"""
import os
import sys

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

# Load .env from project root
_env_path = os.path.join(project_root, ".env")
if os.path.exists(_env_path):
    with open(_env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))

import pandas as pd
import time
import urllib.request
import xml.etree.ElementTree as ET

# --- Test config ---
TEST_RAW_PATH = os.path.join(
    os.path.dirname(__file__), "fixtures", "bg_ranks_raw_test.csv"
)
TEST_OUTPUT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "tests", "output_master_list.csv"
)
RATING_THRESHOLD = 73
BGG_CHUNK_SIZE = 20
BGG_SLEEP_SECONDS = 5


def fetch_expansions_for_base_games(base_games):
    """Query BGG API for expansion links. Same logic as bgg_csv.py."""
    headers = {
        "User-Agent": "BoardGameCatalog/1.0 (https://boardgamegeek.com/applications)",
    }
    bgg_token = os.getenv("BGG_TOKEN")
    if bgg_token:
        headers["Authorization"] = f"Bearer {bgg_token}"
    else:
        print("⚠️  No BGG_TOKEN - API may return 401. Set it for full test.")

    master_list = []
    base_id_to_name = {str(g["bgg_id"]): g["name"] for g in base_games}

    for i in range(0, len(base_games), BGG_CHUNK_SIZE):
        chunk = base_games[i : i + BGG_CHUNK_SIZE]
        bgg_ids = [str(g["bgg_id"]) for g in chunk]
        url = f"https://boardgamegeek.com/xmlapi2/thing?id={','.join(bgg_ids)}&stats=1"

        for attempt in range(3):
            try:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req) as response:
                    root = ET.fromstring(response.read())

                for item in root.findall("item"):
                    base_id = item.get("id")
                    base_name_elem = item.find("name[@type='primary']")
                    base_name = (
                        base_name_elem.get("value")
                        if base_name_elem is not None
                        else base_id_to_name.get(base_id, "Unknown")
                    )

                    master_list.append(
                        {
                            "bgg_id": base_id,
                            "name": base_name,
                            "parent_id": "",
                            "parent_name": "",
                            "is_expansion": "False",
                        }
                    )

                    for link in item.findall("link"):
                        if link.get("type") == "boardgameexpansion":
                            exp_id = link.get("id")
                            exp_name = link.get("value", "Unknown")
                            if exp_id:
                                master_list.append(
                                    {
                                        "bgg_id": exp_id,
                                        "name": exp_name,
                                        "parent_id": base_id,
                                        "parent_name": base_name,
                                        "is_expansion": "True",
                                    }
                                )

                break
            except Exception as e:
                print(f"⚠️ BGG API attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    time.sleep(10)
                else:
                    raise

        time.sleep(BGG_SLEEP_SECONDS)
        done = min(i + BGG_CHUNK_SIZE, len(base_games))
        print(f"  ✅ Fetched expansions for {done}/{len(base_games)} base games...")

    return master_list


def run_test():
    print("=" * 60)
    print("BGG CSV Test - 10 games (Gloomhaven, Ark Nova, + 8 others)")
    print("=" * 60)

    print("\n🚀 Step 1: Filtering base games from CSV...")
    df = pd.read_csv(TEST_RAW_PATH)

    mask = (df["is_expansion"] == 0) & (
        (df["rank"] > 0) | (df["usersrated"] >= RATING_THRESHOLD)
    )
    base_games = df.loc[mask, ["id", "name"]].copy()
    base_games.columns = ["bgg_id", "name"]
    base_games = base_games.to_dict("records")

    print(f"  Found {len(base_games)} base games meeting threshold.")
    for g in base_games:
        print(f"    - {g['name']} (id={g['bgg_id']})")

    # Step 2: Fetch expansions (requires BGG_TOKEN for real API)
    if not os.getenv("BGG_TOKEN"):
        print("\n⚠️  Skipping Step 2 (BGG API) - no BGG_TOKEN set.")
        print("   To test expansion fetch: BGG_TOKEN=your_token python tests/test_bgg_csv.py")
        master_list = [
            {**g, "parent_id": "", "parent_name": "", "is_expansion": "False"}
            for g in base_games
        ]
        print(f"   Filter-only result: {len(master_list)} base games (no expansions)")
    else:
        print("\n🚀 Step 2: Fetching expansion links from BGG API...")
        master_list = fetch_expansions_for_base_games(base_games)
        print(f"\n  Master list: {len(master_list)} items (base + expansions)")

    # Write output
    os.makedirs(os.path.dirname(TEST_OUTPUT_PATH), exist_ok=True)
    pd.DataFrame(master_list).to_csv(TEST_OUTPUT_PATH, index=False)
    print(f"\n✍️  Output written to: {TEST_OUTPUT_PATH}")

    # Summary
    base_count = sum(1 for r in master_list if r["is_expansion"] == "False")
    exp_count = sum(1 for r in master_list if r["is_expansion"] == "True")
    print(f"\n📊 Summary: {base_count} base games, {exp_count} expansions")
    print("\n🎉 Test complete.")


if __name__ == "__main__":
    run_test()
