import requests
import time
from datetime import date, timedelta, datetime
import pytz
from tqdm import tqdm
from bs4 import BeautifulSoup
from random import uniform
import json
import psycopg2


# Initialize a global request counter
request_counter = 0


def increment_request_counter():
    global request_counter
    request_counter += 1  # Increment the counter
    if request_counter >= 20:  # Check if 24 requests have been made
        print("Waiting 61 seconds...")
        time.sleep(61)  # Wait for 61 seconds
        request_counter = 0  # Reset the counter


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
            placeholders = ", ".join(["%s"] * len(data))

            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            # Convert data values to a list while handling JSON serialization
            values = [
                json.dumps(v) if isinstance(v, (dict, list)) else v
                for v in data.values()
            ]

            try:
                cursor.execute(query, values)
            # if the data is already in the database, skip it
            except ValueError:
                print("Data already in database, skipping...")
                conn.rollback()
                continue

        conn.commit()

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"An error occurred while saving to the database: {e}")
        conn.rollback()
        # raise Exception


def get_teams_from_links(soup: BeautifulSoup):
    # Find the specific 'div' containing the desired links
    scorebox_div = soup.find("div", {"class": "scorebox"})

    # Default values
    away_team = "N/A"
    home_team = "N/A"

    if scorebox_div:
        # Find all links within the 'div' that match the criteria
        team_links = scorebox_div.find_all(
            "a", href=lambda href: href and "/cbb/schools/" in href and ".html" in href
        )

        if len(team_links) >= 2:
            away_team = team_links[0].text
            home_team = team_links[1].text

    return away_team, home_team


def get_team_records(soup):
    # Find divs that contain team records. The actual path may vary based on the page's HTML structure.
    scorebox_div = soup.find("div", {"class": "scorebox"})
    records_divs = scorebox_div.find_all(
        "div", string=lambda text: text and "-" in text
    )
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

        try:
            return away_team_wins, away_team_losses, home_team_wins, home_team_losses
        except:
            return None, None, None, None

    else:
        return None, None, None, None


def get_team_scores(soup):
    scores = soup.find_all("div", class_="score")
    if len(scores) >= 2:
        away_score = int(scores[0].text)
        home_score = int(scores[1].text)
        try:
            return away_score, home_score
        except:
            return None, None
    return None, None


def get_game_location(soup):
    # The actual path may vary based on the page's HTML structure.
    scorebox_div = soup.find("div", {"class": "scorebox_meta"})
    # get the 2nd number of div under scorebox_meta
    location_div = scorebox_div.find_all("div", string=lambda text: text)[1]

    if location_div:
        try:
            return location_div.text
        except:
            return None
    else:
        return None


def get_game_attendance(soup):
    # The actual path may vary based on the page's HTML structure.
    scorebox_div = soup.find("div", {"class": "scorebox_meta"})
    # get the 2 number of div under scorebox_meta
    location_div = scorebox_div.find_all("div")[2]
    if location_div:
        try:
            return int(location_div.text.split(":")[1].strip().replace(",", ""))
        except:
            return None
    else:
        return None


def get_game_arena(soup):
    # The actual path may vary based on the page's HTML structure.
    scorebox_div = soup.find("div", {"class": "scorebox_meta"})
    # get the 3 number of div under scorebox_meta
    location_div = scorebox_div.find_all("div")[1]
    if location_div:
        try:
            return location_div.text.strip()
        except:
            return None
    else:
        return None


def get_game_duration(soup):
    # The actual path may vary based on the page's HTML structure.
    scorebox_div = soup.find("div", {"class": "scorebox_meta"})
    # get the 4 number of div under scorebox_meta
    location_div = scorebox_div.find_all("div")[4]
    if location_div:
        return location_div.text.split(":")[1].strip()
    else:
        return None


def get_game_time(soup):
    # The actual path may vary based on the page's HTML structure.
    scorebox_div = soup.find("div", {"class": "scorebox_meta"})
    time_div = scorebox_div.find_all("div", string=lambda text: text and "," in text)[0]

    if time_div:
        # convert above date_format to
        #    "%I:%M %p, %B %d, %Y"
        # Convert to datetime object
        try:
            date_format = "%B %d, %Y, %I:%M %p"
            local_datetime = datetime.strptime(time_div.text, date_format)
        except:
            date_format = "%B %d, %Y"
            local_datetime = datetime.strptime(time_div.text, date_format)
        # Assume the original time is in 'America/New_York' timezone
        local_timezone = pytz.timezone("America/New_York")

        # Localize the datetime object to the given timezone
        local_datetime = local_timezone.localize(local_datetime)

        # Convert to UTC
        utc_datetime = local_datetime.astimezone(pytz.UTC)
        return utc_datetime
    else:
        return None


def get_team_game_stats(soup: BeautifulSoup, team_name: str, score):
    try:
        all_game_stats = []
        # get all h2_tags that have score in their text
        h2_tags = soup.find_all("h2", string=lambda text: text and score in text)
        if not h2_tags:
            return None

        for h2_tag in h2_tags:
            # Find the next two tables after this h2 tag
            next_two_tables = h2_tag.find_all_next("table", limit=2)

            for table_tag in next_two_tables:
                # <th aria-label="" data-stat="header_tmp" colspan="22" class=" over_header center">Basic Box Score Stats</th>
                table_type = table_tag.find("th", {"class": "over_header"}).text.strip()
                if not table_tag:
                    continue

                header_tags = table_tag.find_all("th", {"scope": "col"})
                headers = [
                    tag["data-stat"] for tag in header_tags if "data-stat" in tag.attrs
                ]
                row_tags = table_tag.find_all("tr", class_=lambda x: x != "thead")
                for row_tag in row_tags[2:]:  # Skip the header row
                    row_data = {
                        "type": table_type,
                        "caption": f"{team_name} Basic and Advanced Stats Table",
                    }
                    cell_tags = row_tag.find_all(["th", "td"])
                    for header, cell_tag in zip(headers, cell_tags):
                        row_data[header] = (
                            cell_tag.text.strip() if cell_tag.text else None
                        )
                    all_game_stats.append(row_data)
        return all_game_stats
    except Exception as e:
        return []


def populate_fields(soup):
    scraped_data = {}

    # Get sport information
    scraped_data["sport"] = "NCAAB"

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

    game_arena = get_game_arena(soup)
    scraped_data["location"] = game_arena

    game_time = get_game_time(soup)
    scraped_data["game_time"] = game_time

    # get all game stats
    away_team_basic_stats = get_team_game_stats(
        soup, away_team, f"({away_team_wins}-{away_team_losses})"
    )
    home_team_basic_stats = get_team_game_stats(
        soup, home_team, f"({home_team_wins}-{home_team_losses})"
    )

    scraped_data["home_team_game_stats"] = home_team_basic_stats
    scraped_data["away_team_game_stats"] = away_team_basic_stats
    return scraped_data


def fetch_and_parse_game_links(date_url, max_retries=3):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    game_links = []
    game_data = []  # To store scraped data for each game
    retries = 0
    while retries <= max_retries:
        if retries == max_retries:
            print(f"Failed to fetch {date_url}. Skipping...")
            return game_links, game_data
        try:
            response = requests.get(date_url, headers=headers)
            increment_request_counter()
            if response.status_code == 200:
                print(f"Fetched {date_url}")
                soup = BeautifulSoup(response.content, "html.parser")
                links = soup.find_all("td", {"class": "right gamelink"})
                links = [
                    link
                    for link in links
                    if link.find("a")
                    and link.find("a").get("href")
                    and "_w" not in link.find("a")["href"]
                ]

                for link in links:
                    try:
                        if link is not None:
                            tries = 0
                            while True:
                                full_url = f"https://www.sports-reference.com{link.find('a')['href']}"
                                print(full_url)
                                game_response = requests.get(full_url, headers=headers)
                                increment_request_counter()
                                if game_response.status_code == 200:
                                    game_links.append(full_url)
                                    game_info = populate_fields(
                                        BeautifulSoup(
                                            game_response.content, "html.parser"
                                        )
                                    )
                                    game_data.append(game_info)
                                    break
                                else:
                                    print(f"Failed to fetch {full_url}. Retrying...")
                                    tries += 1
                                    if tries > max_retries:
                                        print(
                                            f"Max retries reached for URL {full_url}."
                                        )
                                        break
                    except Exception as e:
                        print(e)
                        continue

                break

            elif response.status_code == 429:
                print(f"Rate-limited. Waiting for 1 hour before retrying.")
                time.sleep(3600)  # Wait for an hour

                retries += 1  # Increment the retries count
                if retries > max_retries:
                    print(f"Max retries reached for URL {date_url}.")
                    break
        except Exception as e:
            import traceback

            print(traceback.format_exc())
            print(f"Failed to fetch {date_url}. Retrying...")
            retries += 1
            time.sleep(5)
        time.sleep(uniform(3, 4))  # Wait 3 to 4 seconds between requests

    return game_links, game_data


def scrape_data(start_date=date(1975, 10, 23), end_date=date(2023, 3, 11)):
    time.sleep(60)
    base_url = "https://www.sports-reference.com/cbb/boxscores/index.cgi?"
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
    date_urls.reverse()
    for date_url in tqdm(date_urls):  # Replacing threading with a for loop
        print(f"Fetching and parsing game links for {date_url}")
        game_links, game_data = fetch_and_parse_game_links(date_url)
        for data in game_data:
            save_to_postgresql([data], "game")
        save_to_postgresql(game_data, "game")
        all_game_links.extend(game_links)
        all_game_data.extend(game_data)
        print(f"Completed for {date_url}")

    return all_game_links, all_game_data


if __name__ == "__main__":
    scrape_data()
