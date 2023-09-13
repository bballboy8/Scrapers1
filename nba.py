from threading import Lock
import time
import requests
from bs4 import BeautifulSoup
from datetime import date, timedelta
import concurrent.futures
from tqdm import tqdm
from random import uniform
import psycopg2


# Initialize a global request counter
request_counter = 0
counter_lock = Lock()


def save_to_postgresql(data_list, table_name):
    try:
        conn = psycopg2.connect(
            host="datapipelinedatabaseproto.ckdigrzob04s.eu-west-1.rds.amazonaws.com",
            database="stats_data",
            user="dataPipelineDbUsername",
            password="wNxS7*GLqNls8cr31XAV",
            port="5432",
        )

        cursor = conn.cursor()

        for data in data_list:
            columns = ", ".join(data.keys())
            values = ", ".join([f"'{v}'" for v in data.values()])

            query = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"

            cursor.execute(query, list(data.values()))

        conn.commit()

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"An error occurred while saving to the database: {e}")
        conn.rollback()


def increment_request_counter():
    global request_counter
    request_counter += 1  # Increment the counter
    if request_counter >= 10:  # Check if 24 requests have been made
        print("Waiting 61 seconds...")
        time.sleep(61)  # Wait for 61 seconds
        request_counter = 0  # Reset the counter


def get_team_game_stats(soup, team_name):
    game_stats = []
    h2_tag = soup.find("h2", string=f"{team_name} Basic and Advanced Stats")
    if not h2_tag:
        return None

    table_tag = h2_tag.find_next("table")
    if not table_tag:
        return None

    type_tag = table_tag.find("th", {"class": "over_header", "colspan": True})
    if type_tag:
        stats_type = type_tag.text.strip()
    else:
        stats_type = "Unknown"

    header_tags = table_tag.find_all("th", {"scope": "col"})
    headers = [tag["data-stat"] for tag in header_tags if "data-stat" in tag.attrs]

    row_tags = table_tag.find_all("tr")
    for row_tag in row_tags[1:]:  # Skip the header row
        row_data = {"type": stats_type}
        cell_tags = row_tag.find_all(["th", "td"])
        for header, cell_tag in zip(headers, cell_tags):
            row_data[header] = cell_tag.text.strip() if cell_tag.text else None
        game_stats.append(row_data)

    return game_stats


def get_attendance(soup):
    # Find the div containing 'Attendance'
    attendance_div = soup.find(
        "div", string=lambda text: "Attendance:" in text if text else False
    )

    if attendance_div:
        attendance_str = attendance_div.text
        # Extract just the numerical part of the attendance
        attendance_num = int(
            attendance_str.split("Attendance: ")[1].replace(",", "").strip()
        )
        return attendance_num
    else:
        return None


def get_teams_from_links(soup):
    # Find the specific 'div' containing the desired links
    scorebox_div = soup.find("div", {"class": "scorebox"})
    print(scorebox_div)

    if scorebox_div is not None:
        # Only look for links within the specific 'div'
        away_team_link = scorebox_div.find_all(
            "a", href=lambda href: href and "/teams/" in href and "/2023.html" in href
        )[0]
        home_team_link = scorebox_div.find_all(
            "a", href=lambda href: href and "/teams/" in href and "/2023.html" in href
        )[1]

        if away_team_link and home_team_link:
            away_team = away_team_link.text
            home_team = home_team_link.text
        else:
            away_team = "N/A"
            home_team = "N/A"
    else:
        away_team = "N/A"
        home_team = "N/A"

    return away_team, home_team


def get_team_records(soup):
    # Find divs that contain team records. The actual path may vary based on the page's HTML structure.
    records_divs = soup.select("#content > div:nth-child(2) > div > div:nth-child(3)")
    if len(records_divs) >= 2:
        # Assume the format is 'W-L'
        away_record = records_divs[0].text.split("-")
        home_record = records_divs[1].text.split("-")

        # Initialize as None in case of an unexpected format
        away_team_wins, away_team_losses, home_team_wins, home_team_losses = (
            None,
            None,
            None,
            None,
        )

        if len(away_record) == 2:
            away_team_wins = int(away_record[0])
            away_team_losses = int(away_record[1])

        if len(home_record) == 2:
            home_team_wins = int(home_record[0])
            home_team_losses = int(home_record[1])

        return away_team_wins, away_team_losses, home_team_wins, home_team_losses

    else:
        return None, None, None, None


def get_team_scores(soup):
    scores = soup.find_all("td", class_="score")
    if len(scores) >= 2:
        away_score = int(scores[0].text)
        home_score = int(scores[1].text)
        return away_score, home_score
    return None, None


def get_game_location(soup):
    # The actual path may vary based on the page's HTML structure.
    location_div = soup.select_one(
        "#content > div:nth-child(2) > div:nth-child(3) > div:nth-child(2)"
    )

    if location_div:
        return location_div.text
    else:
        return None


def get_play_by_play(play_by_play_url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    response = requests.get(play_by_play_url, headers=headers)
    increment_request_counter()
    if response.status_code != 200:
        print(f"Failed to get play-by-play data from {play_by_play_url}")
        return None

    soup = BeautifulSoup(response.content, "html.parser")

    play_by_play_data = []
    period = 0

    # Assuming each row in the table represents a play-by-play event.
    for row in soup.select("tr"):
        if row.get("class") == ["thead"] and row.get("id"):
            period_text = row.find("th").text
            period = int(
                period_text.replace("st Q", "")
                .replace("nd Q", "")
                .replace("rd Q", "")
                .replace("th Q", "")
            )
            continue

        if row.get("class") != ["thead"]:
            cells = row.find_all("td")
            if len(cells) == 6:
                play = {
                    "period": period,
                    "Time": cells[0].text.strip(),
                    "Atlanta": cells[1].text.strip(),
                    "Score": cells[3].text.strip(),
                    "Chicago": cells[5].text.strip(),
                }
                play_by_play_data.append(play)

    return play_by_play_data


def populate_fields(soup):
    scraped_data = {}

    # Get sport information
    scraped_data["sport"] = "NBA"

    # Get away_team and home_team
    away_team, home_team = get_teams_from_links(soup)
    scraped_data["away_team"] = away_team
    scraped_data["home_team"] = home_team

    # Get wins and losses
    (
        away_team_wins,
        away_team_losses,
        home_team_wins,
        home_team_losses,
    ) = get_team_records(soup)
    scraped_data["away_team_wins"] = away_team_wins
    scraped_data["away_team_losses"] = away_team_losses
    scraped_data["home_team_wins"] = home_team_wins
    scraped_data["home_team_losses"] = home_team_losses

    # Get scores
    away_score, home_score = get_team_scores(soup)
    scraped_data["away_score"] = away_score
    scraped_data["home_score"] = home_score
    location = get_game_location(soup)
    scraped_data["location"] = location
    play_by_play_link_element = soup.find("a", href=lambda href: href and "pbp" in href)
    if play_by_play_link_element:
        full_url = (
            f"https://www.basketball-reference.com{play_by_play_link_element['href']}"
        )
        play_by_play_data = get_play_by_play(full_url)
        scraped_data["play_by_play"] = play_by_play_data

    attendance = get_attendance(soup)
    scraped_data["attendance"] = attendance

    home_team_game_stats = get_team_game_stats(soup, home_team)
    away_team_game_stats = get_team_game_stats(soup, away_team)

    scraped_data["home_team_game_stats"] = home_team_game_stats
    scraped_data["away_team_game_stats"] = away_team_game_stats

    return scraped_data


def fetch_and_parse_game_links(date_url, max_retries=3):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    game_links = []
    game_data = []  # To store scraped data for each game
    retries = 0

    while retries <= max_retries:
        try:
            response = requests.get(date_url, headers=headers)
            increment_request_counter()
            if response.status_code == 200:
                print(f"Fetched {date_url}")
                soup = BeautifulSoup(response.content, "html.parser")

                for a_tag in soup.find_all("a", href=True):
                    href = a_tag["href"]
                    if (
                        "/boxscores/" in href
                        and ".html" in href
                        and len(href) > len("/boxscores/")
                    ):
                        full_url = f"https://www.basketball-reference.com{href}"
                        game_response = requests.get(full_url, headers=headers)
                        game_soup = BeautifulSoup(game_response.content, "html.parser")
                        game_links.append(full_url)
                        game_info = populate_fields(game_soup)
                        game_data.append(game_info)

                # Successfully fetched and parsed the data, so break out of the loop
                break

            elif response.status_code == 429:
                print(f"Rate-limited. Waiting for 1 hour before retrying.")
                time.sleep(3600)  # Wait for an hour

            retries += 1  # Increment the retries count
            if retries > max_retries:
                print(f"Max retries reached for URL {date_url}.")
                break

        except Exception as e:
            print(f"Exception while fetching {date_url}: {e}")
            retries += 1
            if retries > max_retries:
                print(f"Max retries reached for URL {date_url}.")
                break

        time.sleep(uniform(3, 4))  # Wait 3 to 4 seconds between requests

    return game_links, game_data


def scrape_data(start_date=date(2023, 6, 12), end_date=date.today()):
    base_url = "https://www.basketball-reference.com/boxscores/index.fcgi?"
    all_game_links = []
    all_game_data = []  # To store scraped data for all games

    date_urls = []
    current_date = start_date
    while current_date <= end_date:
        date_url = (
            base_url
            + f"month={current_date.month}&day={current_date.day}&year={current_date.year}"
        )
        date_urls.append(date_url)
        current_date += timedelta(days=1)

    for date_url in date_urls:  # Replacing threading with a for loop
        print(f"Fetching and parsing game links for {date_url}")
        game_links, game_data = fetch_and_parse_game_links(date_url)
        save_to_postgresql(game_data, "game")
        all_game_links.extend(game_links)
        all_game_data.extend(game_data)
        print(f"Completed for {date_url}")

    return all_game_links, all_game_data


if __name__ == "__main__":
    scrape_data()
