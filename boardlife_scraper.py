import os
import time
import requests
from bs4 import BeautifulSoup
from google.cloud import firestore
from dotenv import load_dotenv
from tqdm import tqdm

# Load environment variables
load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "boardgames")

# BoardLife asks for 2-3 seconds between requests to avoid IP bans
SLEEP_TIME = 2.5 

def get_games_missing_ko_names(db):
    """Fetches games from Firestore where we don't have a Korean translation yet."""
    print(f"🔍 Scanning '{COLLECTION_NAME}' collection for missing translations...")
    games_to_update = []
    
    # Get all games (Firestore doesn't natively support querying where Field A == Field B)
    docs = db.collection(COLLECTION_NAME).stream()
    
    for doc in docs:
        data = doc.to_dict()
        eng_title = data.get('title', '')
        ko_title = data.get('title_ko', '')
        
        # If the Korean title is missing, or exactly the same as the English one
        if not ko_title or eng_title.lower() == ko_title.lower():
            games_to_update.append({
                'id': doc.id,
                'title': eng_title
            })
            
    return games_to_update

def scrape_boardlife(english_title):
    """Hits BoardLife's search engine and parses the HTML for the Korean title."""
    url = f"https://boardlife.co.kr/bbs_list.php?tb=board_game&search_mode=ok&b_search_target=info_eng_title&b_search={requests.utils.quote(english_title)}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            return None
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # BoardLife results are usually in a table where the title is an <a> tag
        result_links = soup.select("table tbody tr td a")
        
        for link in result_links:
            text = link.text.strip()
            # If the link has text, and it's not a generic button or the english name itself
            if text and len(text) > 1 and english_title.lower() not in text.lower():
                return text
                
        return None
        
    except Exception:
        # We fail silently here so we don't clutter the progress bar
        return None

def main():
    if not PROJECT_ID:
        print("❌ Error: PROJECT_ID is missing from your .env file.")
        return

    # --- TERMINAL UX: CLEAR PROJECT HEADER ---
    print("=" * 55)
    print(" 🚀 STARTING BOARDLIFE LOCALIZATION SCRAPER")
    print(f" 🔌 Target Database : {PROJECT_ID}")
    print("=" * 55 + "\n")

    # Initialize Firestore
    db = firestore.Client(project=PROJECT_ID)

    target_games = get_games_missing_ko_names(db)
    print(f"📊 Found {len(target_games)} games that need checking on BoardLife.\n")
    
    if not target_games:
        print("✅ Database is already fully localized!")
        return

    updates_made = 0
    not_found_count = 0
    batch = db.batch()
    batch_count = 0

    for game in tqdm(target_games, desc="Scraping BoardLife", unit="game"):
        eng_title = game['title']
        doc_id = game['id']
        
        korean_name = scrape_boardlife(eng_title)
        
        if korean_name:
            # Stage the update for Firestore
            doc_ref = db.collection(COLLECTION_NAME).document(doc_id)
            
            # Using set with merge=True is safer than update()
            batch.set(doc_ref, {"title_ko": korean_name}, merge=True)
            batch_count += 1
            updates_made += 1
            
            # Print success without breaking the tqdm progress bar
            tqdm.write(f"  ✅ Found: '{eng_title}' -> '{korean_name}'")
            
            # Commit batch every 400 updates to stay under Firestore's 500 limit
            if batch_count >= 400:
                batch.commit()
                batch = db.batch()
                batch_count = 0
        else:
            not_found_count += 1
        
        # Be polite to BoardLife's servers
        time.sleep(SLEEP_TIME)

    # Commit any remaining updates
    if batch_count > 0:
        batch.commit()

    # --- TERMINAL UX: EXECUTIVE SUMMARY ---
    print("\n" + "=" * 55)
    print(f" 🎉 LOCALIZATION COMPLETE FOR {PROJECT_ID.upper()}")
    print("=" * 55)
    print(f"  🟢 Successfully Translated : {updates_made} games")
    print(f"  🟡 No BoardLife Data Found : {not_found_count} games")
    print("=" * 55 + "\n")


if __name__ == "__main__":
    main()