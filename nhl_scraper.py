import requests
import time
from datetime import date, timedelta, datetime
import pytz
from tqdm import tqdm
from bs4 import BeautifulSoup
from random import uniform
import json
import psycopg2
from psycopg2 import errors


# Initialize a global request counter
request_counter = 0

def increment_request_counter():
    global request_counter
    request_counter += 1  # Increment the counter
    if request_counter >= 10:  # Check if 24 requests have been made
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
            except errors.UniqueViolation:
                print("Data already in database, skipping...")
                conn.rollback()
                continue

        conn.commit()

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"An error occurred while saving to the database: {e}")
        conn.rollback()
        cursor.close()




def get_teams_from_links(soup: BeautifulSoup):
    # Find the specific 'div' containing the desired links
    scorebox_div = soup.find("div", {"class": "scorebox"})

    if scorebox_div is not None:
        # Only look for links within the specific 'div'
        away_team_link = scorebox_div.find_all(
            "a", href=lambda href: href and "/teams/" in href and ".html" in href
        )[0]
        home_team_link = scorebox_div.find_all(
            "a", href=lambda href: href and "/teams/" in href and ".html" in href
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

        return away_team_wins, away_team_losses, home_team_wins, home_team_losses

    else:
        return None, None, None, None


def get_team_scores(soup):
    scores = soup.find_all("div", class_="score")
    if len(scores) >= 2:
        away_score = int(scores[0].text)
        home_score = int(scores[1].text)
        return away_score, home_score
    return None, None


def get_game_location(soup):
    # The actual path may vary based on the page's HTML structure.
    scorebox_div = soup.find("div", {"class": "scorebox_meta"})
    # get the 2nd number of div under scorebox_meta
    location_div = scorebox_div.find_all(
        "div", string=lambda text: text
    )[1]

    if location_div:
        return location_div.text
    else:
        return None

def get_game_attendance(soup):
    # The actual path may vary based on the page's HTML structure.
    scorebox_div = soup.find("div", {"class": "scorebox_meta"})
    # get the 2 number of div under scorebox_meta
    location_div = scorebox_div.find_all("div")[2]
    if location_div:
        return location_div.text.split(":")[1].strip()
    else:
        return None


def get_game_arena(soup):
    # The actual path may vary based on the page's HTML structure.
    scorebox_div = soup.find("div", {"class": "scorebox_meta"})
    # get the 3 number of div under scorebox_meta
    location_div = scorebox_div.find_all("div")[3]
    if location_div:
        return location_div.text.split(":")[1].strip()
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
        date_format = (
            "%B %d, %Y, %I:%M %p"  # The format string that matches the date_string
        )
        # convert above date_format to 
        #    "%I:%M %p, %B %d, %Y"  
        # Convert to datetime object
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


def get_team_game_stats(soup:BeautifulSoup, team_name:str, stats_type, goalies=0):
    all_game_stats = []
    h2_tags = soup.find_all("h2", string=f"{team_name}")

    if not h2_tags:
        return None

    for h2_tag in h2_tags:
        # Find the next two tables after this h2 tag
        next_two_tables = h2_tag.find_all_next("table", limit=8)
        if goalies == 0:
            pass
        elif goalies == 1:
            tables = []
            for table in next_two_tables:
                if "Goalies" in table.find("caption").text:
                    tables.append(table)
            next_two_tables = [tables[0]]
        elif goalies == 2:
            tables = []
            for table in next_two_tables:
                if "Goalies" in table.find("caption").text:
                    tables.append(table)
            next_two_tables = [tables[1]]
 
        for table_tag in next_two_tables:
            caption = table_tag.find("caption").text
            if not table_tag:
                continue


            header_tags = table_tag.find_all("th", {"scope": "col"})
            headers = [
                tag["data-stat"] for tag in header_tags if "data-stat" in tag.attrs
            ]
            row_tags = table_tag.find_all("tr", class_=lambda x: x != "thead")
            for row_tag in row_tags[2:]:  # Skip the header row
                row_data = {"type": stats_type, "caption": caption}
                cell_tags = row_tag.find_all(["th", "td"])
                for header, cell_tag in zip(headers, cell_tags):
                    row_data[header] = cell_tag.text.strip() if cell_tag.text else None                  
                all_game_stats.append(row_data)
                try:
                    if row_data['player'] == 'TOTAL':
                        final_stats = []
                        for stat in all_game_stats:
                            if stat['caption'] == f"{team_name} Table" or stat['caption'] == f"{team_name} Advanced Table":
                                final_stats.append(stat)
                        return final_stats
                except:
                    pass
        if goalies != 0:
            break
    final_stats = []
    for stat in all_game_stats:
        if stat['caption'] == f"{team_name} Table" or stat['caption'] == f"{team_name} Advanced Table":
            final_stats.append(stat)
    return final_stats

def populate_fields(soup):
    scraped_data = {}

    # Get sport information
    scraped_data["sport"] = "NHL"

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
    
    game_attendance = get_game_attendance(soup)
    scraped_data["attendance"] = int(game_attendance.replace(",", ""))

    # get all game stats
    away_team_basic_stats = get_team_game_stats(soup, away_team, "Basic Box Score Stats")
    home_team_basic_stats = get_team_game_stats(soup, home_team, "Basic Box Score Stats")
    away_team_advance_stats = get_team_game_stats(soup, away_team+ " Advanced", "Advanced Box Score Stats")
    home_team_advance_stats = get_team_game_stats(soup, home_team+ " Advanced", "Advanced Box Score Stats")
    away_team_goalie_stats = get_team_game_stats(soup, "Goalies", "Goalies Box Score Stats", 1)
    home_team_goalie_stats = get_team_game_stats(soup, "Goalies", "Goalies Box Score Stats", 2)

    scraped_data["home_team_game_stats"] = home_team_basic_stats + home_team_advance_stats + home_team_goalie_stats
    scraped_data["away_team_game_stats"] = away_team_basic_stats + away_team_advance_stats + away_team_goalie_stats

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
                for link in links:
                    full_url = f"https://www.hockey-reference.com{link.find('a')['href']}"
                    print(full_url)
                    game_response = requests.get(full_url, headers=headers)
                    game_links.append(full_url)
                    game_info = populate_fields(BeautifulSoup(game_response.content, "html.parser"))
                    game_data.append(game_info)

                break

            elif response.status_code == 429:
                print(f"Rate-limited. Waiting for 1 hour before retrying.")
                time.sleep(3600)  # Wait for an hour

                retries += 1  # Increment the retries count
                if retries > max_retries:
                    print(f"Max retries reached for URL {date_url}.")
                    break
        except Exception as e:
           print(e)
           print(f"Failed to fetch {date_url}. Retrying...")
           retries += 1
           time.sleep(5)
        time.sleep(uniform(3, 4))  # Wait 3 to 4 seconds between requests
    
    return game_links, game_data


def scrape_data(start_date=date(1975, 10, 23), end_date=date(2022, 6, 16)):
    base_url = "https://www.hockey-reference.com/boxscores/index.fcgi?"
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
