import re
import csv
import os
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def find_last_page(rank_url):
    end_of_page_list = False
    last_page = 1
    while not end_of_page_list:
        page_url = rank_url.format(last_page)
        response = requests.get(page_url)
        soup = BeautifulSoup(response.text, "html.parser")
        game_rank_div_wrapper = soup.find("div", attrs={"class": "main-wrapper-box"})

        pattern = re.compile(r"rank-row")
        game_id = game_rank_div_wrapper.find_all("div", attrs={"id": pattern})
        if len(game_id) == 0:
            end_of_page_list = True
            break
        else:
            last_page += 1
    print(f">> Last Page: {last_page-1}")
    return last_page


def get_game_info(game_page_soup, href, game_page_url):
    try:
        game_name_kr_str = game_page_soup.find("a", attrs={"id": "boardgame-title"}).text
    except Exception:
        game_name_kr_str = ""

    game_bl_id_str = href.split("/")[-1]
    try:
        game_bgg_id_str = game_page_soup.find("a", attrs={"class": "guide ms-2"})["href"].split("/boardgame/")[-1].split("/")[0]
    except Exception:
        game_bgg_id_str = ""

    game_row = {
        "title_ko": game_name_kr_str,
        "bl_id": game_bl_id_str,
        "bgg_id": game_bgg_id_str,
    }
    return game_row


def get_game_hrefs_from_rank_page(soup):
    """Extract all game detail links from a rank page."""
    hrefs = []
    for a in soup.find_all("a", href=re.compile(r"^/game/\d+$")):
        href = a.get("href")
        if href and href not in hrefs:
            hrefs.append(href)
    return hrefs


def main():
    main_url = "https://boardlife.co.kr"
    rank_url = "https://www.boardlife.co.kr/rank/{}"
    start_page = 1
    # Use a generous upper bound and stop when a page has no games.
    # This avoids relying on fragile HTML structure for last-page detection.
    last_page_plus_one = 1000

    # Stream directly to CSV so progress is always saved, even on Ctrl+C.
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_name = f"boardlife_korean_titles_{timestamp}.csv"
    out_path = os.path.join(os.path.dirname(__file__), out_name)

    total_written = 0
    try:
        with open(out_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["bgg_id", "bl_id", "title_ko"])
            writer.writeheader()

            for page in tqdm(range(start_page, last_page_plus_one), colour="blue", desc="[ Page # ]"):
                page_url = rank_url.format(page)
                try:
                    response = requests.get(page_url, timeout=20)
                except requests.exceptions.ConnectionError as err:
                    print(f"Connection failed for [page_url response]: {err}")
                    continue

                soup = BeautifulSoup(response.text, "html.parser")

                game_href_list = get_game_hrefs_from_rank_page(soup)
                if len(game_href_list) == 0:
                    break

                for href in tqdm(game_href_list, colour="green", desc="[ Game # ]"):
                    game_page_url = main_url + href
                    try:
                        game_page_response = requests.get(game_page_url, timeout=20)
                    except requests.exceptions.ConnectionError as err:
                        print(f"Connection failed for [game_page_response]: {err}")
                        continue
                    game_page_soup = BeautifulSoup(game_page_response.text, "html.parser")

                    game_row = get_game_info(game_page_soup, href, game_page_url)
                    if len(game_row["bgg_id"]) == 0:
                        print(f"No bgg_id for {game_row['title_ko']}")
                        continue

                    writer.writerow(game_row)
                    total_written += 1

                    # Flush periodically so progress is on disk even if interrupted.
                    if total_written % 100 == 0:
                        f.flush()

        print(f">> COMPLETE: wrote {total_written} rows to {out_path}")

    except KeyboardInterrupt:
        # Graceful early exit with partial CSV left on disk.
        print(f"\n>> INTERRUPTED: wrote {total_written} rows to {out_path}")
    except Exception as e:
        print(f">> ERROR: {e}")
        print(f">> ERROR: partial CSV may be in {out_path} with {total_written} rows")
    return


if __name__ == "__main__":
    main()
