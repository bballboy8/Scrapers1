import json
import psycopg2
import pytz
import requests
import time
from datetime import date, datetime, timedelta
from bs4 import BeautifulSoup, Comment
from datetime import datetime
from random import uniform
from tqdm import tqdm


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
        # from database get 50 rows of the table where sport='NHL' also convert the game_time to UTC
        # if table doesn't exist, create it
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
            except:
                print("Data already in database, skipping...")
                conn.rollback()
                continue

        conn.commit()

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"An error occurred while saving to the database: {e}")
        conn.rollback()
        raise Exception


def get_teams_from_links(soup):
    # Find the specific 'div' containing the desired links
    scorebox_div = soup.find("div", {"class": "scorebox"})

    # Default values
    away_team = "N/A"
    home_team = "N/A"

    if scorebox_div:
        # Find all links within the 'div' that match the criteria
        team_links = scorebox_div.find_all(
            "a", href=lambda href: href and "/cfb/schools/" in href and ".html" in href
        )

        if len(team_links) >= 2:
            away_team = team_links[0].text
            home_team = team_links[1].text

    return away_team, home_team


def parse_record(record_text):
    parts = record_text.split("-")
    if len(parts) == 2:
        return int(parts[0]), int(parts[1])
    return None, None


def get_team_records(soup):
    scorebox_div = soup.find("div", {"class": "scorebox"})
    records_divs = scorebox_div.find_all(
        "div", string=lambda text: text and "-" in text
    )

    if len(records_divs) >= 2:
        away_team_wins, away_team_losses = parse_record(records_divs[0].text)
        home_team_wins, home_team_losses = parse_record(records_divs[1].text)
        return away_team_wins, away_team_losses, home_team_wins, home_team_losses

    return None, None, None, None


def get_team_scores(soup):
    scores = [
        int(score.text) if score.text else 0
        for score in soup.find_all("div", class_="score")
    ]
    return tuple(scores) if len(scores) >= 2 else (None, None)


def get_game_attendance(soup):
    scorebox_div = soup.find("div", class_="scorebox_meta")
    if scorebox_div:
        for div in scorebox_div.find_all("div"):
            text = div.get_text(strip=True)
            if text.startswith("Attendance"):
                game_attendance = text.split(":", 1)[1].strip()
                return int(game_attendance.replace(",", "")) if game_attendance else 0
    return 0


def get_game_time(soup):
    # The actual path may vary based on the page's HTML structure.
    scorebox_divs = soup.find("div", class_="scorebox_meta")
    # if the first element contains a link, then start from the second element
    try:
        game_date = scorebox_divs.find(
            "div", string=lambda text: text and "," in text
        ).text.strip()  # Monday Jan 10, 2022
        try:
            game_time = scorebox_divs.find(
                "div", string=lambda text: text and "ET" in text
            ).text.strip()  # 8:15 PM ET
        except:
            game_time = ""
        game_date_time = game_date + " " + game_time  # Monday Jan 10, 2022 8:15 PM ET
        if game_date_time:
            # The format string that matches the date_string
            if game_time == "":
                date_format = "%A %b %d, %Y "
            else:
                date_format = "%A %b %d, %Y %I:%M %p ET"
            # convert above date_format to
            local_datetime = datetime.strptime(game_date_time, date_format)

            # Assume the original time is in 'America/New_York' timezone
            local_timezone = pytz.timezone("America/New_York")

            # Localize the datetime object to the given timezone
            local_datetime = local_timezone.localize(local_datetime)

            # Convert to UTC
            utc_datetime = local_datetime.astimezone(pytz.UTC)

            # Define the desired output format
            output_format_string = "%B %d, %Y, %I:%M %p"

            # Convert the datetime object to the desired format
            formatted_datetime = utc_datetime.strftime(output_format_string)

            return formatted_datetime

        else:
            return None
    except:
        return None


def get_game_location(soup):
    try:
        scorebox_divs = soup.find("div", class_="scorebox_meta").find_all("div")
        for tag in scorebox_divs:
            # check if tag.text contains ET and if it is the last second element
            if "ET" in tag.text and tag == scorebox_divs[-2]:
                return None
            elif "ET" in tag.text and tag != scorebox_divs[-2]:
                return scorebox_divs[scorebox_divs.index(tag) + 1].text.strip()
    except:
        return None


def get_team_game_stats(soup: BeautifulSoup, table_type, team_name, t_type):
    # try finding all tables in comments
    try:
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))
        for comment in comments:
            all_game_stats = []
            if table_type in comment:
                table_tag = BeautifulSoup(comment, "html.parser")
                caption = table_tag.find("caption").text
                header_tags = table_tag.find_all("th", {"scope": "col"})
                headers = [
                    tag["data-stat"] for tag in header_tags if "data-stat" in tag.attrs
                ]
                row_tags = table_tag.find_all("tr", class_=lambda x: x != "thead")
                for row_tag in row_tags[2:]:  # Skip the header row
                    row_data = {"type": t_type, "caption": caption}
                    cell_tags = row_tag.find_all(["th", "td"])
                    for header, cell_tag in zip(headers, cell_tags):
                        row_data[header] = (
                            cell_tag.text.strip() if cell_tag.text else None
                        )
                    all_game_stats.append(row_data)
                final_stats = []
                for stat in all_game_stats:
                    if stat["school_name"] == team_name:
                        final_stats.append(stat)
                return final_stats
    except Exception as e:
        print(e)
        return []


def populate_fields(soup):
    scraped_data = {}

    # Get sport information
    scraped_data["sport"] = "NCAAF"

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

    # Get game location
    game_location = get_game_location(soup)
    scraped_data["location"] = game_location
    # Get game time
    game_time = get_game_time(soup)
    scraped_data["game_time"] = game_time

    home_team_stats_result = []
    all_stats = [
        "Rushing &amp; Receiving",
        "Passing",
        "Defense &amp; Fumbles",
        "Kick &amp; Punt Returns",
        "Kicking &amp; Punting",
        "Scoring",
    ]
    for stat in all_stats:
        try:
            home_team_stats_result.extend(
                get_team_game_stats(soup, stat, home_team, f"{home_team} {stat} Stats")
            )
        except:
            continue

    away_team_stats_result = []
    for stat in all_stats:
        try:
            away_team_stats_result.extend(
                get_team_game_stats(soup, stat, away_team, f"{away_team} {stat} Stats")
            )
        except:
            continue

    scraped_data["home_team_game_stats"] = home_team_stats_result
    scraped_data["away_team_game_stats"] = away_team_stats_result
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
                # find class = game_summaries
                summary = soup.find("div", {"class": "game_summaries"})
                if (
                    summary != None
                    and summary.find("h2").text != "Other Games This Week"
                ):
                    links = []
                    try:
                        links.extend(
                            summary.find_all("td", {"class": "right gamelink"})
                        )
                    except:
                        break
                    if links:
                        for link in links:
                            if link is None:
                                continue
                            full_url = f"https://www.sports-reference.com{link.find('a')['href']}"
                            print(full_url)
                            game_response = requests.get(full_url, headers=headers)
                            increment_request_counter()
                            game_links.append(full_url)
                            game_info = populate_fields(
                                BeautifulSoup(game_response.content, "html.parser")
                            )
                            game_data.append(game_info)
                    break
                else:
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


def scrape_data(start_date=date(1975, 10, 23), end_date=date(2019, 11, 23)):
    base_url = "https://www.sports-reference.com/cfb/boxscores/index.cgi?"
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
        save_to_postgresql(game_data, "game")
        all_game_links.extend(game_links)
        all_game_data.extend(game_data)
        print(f"Completed for {date_url}")

    return all_game_links, all_game_data


if __name__ == "__main__":
    scrape_data()
