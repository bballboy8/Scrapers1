import json
import psycopg2
import pytz
import requests
import time
import traceback
import os
from bs4 import BeautifulSoup, Comment
from datetime import datetime
from random import uniform
from tqdm import tqdm


def save_to_postgresql(data_list, table_name):
    try:
        host = os.getenv("DB_HOST")
        database = os.getenv("DB_NAME")
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        port = os.getenv("DB_PORT")

        # Establish the connection
        conn = psycopg2.connect(
            host=host,
            database=database,
            user=user,
            password=password,
            port=port,
        )

        cursor = conn.cursor()
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

            cursor.execute(query, values)

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
        team_links = scorebox_div.find_all("a", href=lambda href: href and "/teams/" in href and ".htm" in href)

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
    records_divs = scorebox_div.find_all("div", string=lambda text: text and "-" in text)

    if len(records_divs) >= 2:
        away_team_wins, away_team_losses = parse_record(records_divs[0].text)
        home_team_wins, home_team_losses = parse_record(records_divs[1].text)
        return away_team_wins, away_team_losses, home_team_wins, home_team_losses

    return None, None, None, None


def get_team_scores(soup):
    scores = [int(score.text) if score.text else 0 for score in soup.find_all("div", class_="score")]
    return tuple(scores) if len(scores) >= 2 else (None, None)


def get_game_attendance(soup):
    scorebox_div = soup.find('div', class_='scorebox_meta')
    if scorebox_div:
        for div in scorebox_div.find_all('div'):
            text = div.get_text(strip=True)
            if text.startswith("Attendance"):
                game_attendance = text.split(":", 1)[1].strip()
                return int(game_attendance.replace(",", "")) if game_attendance else 0
    return 0


def get_game_time(soup):
    # The actual path may vary based on the page's HTML structure.
    scorebox_divs = soup.find('div', class_='scorebox_meta').find_all('div')

    # Initialize variables to store date and start time
    game_date = None
    start_time = None

    # Loop through the div elements and extract information
    for i, div in enumerate(scorebox_divs):
        if i == 0:
            game_date = div.get_text(strip=True)
        else:
            text = div.get_text(strip=True)
            if text.startswith("Start Time"):
                start_time = text.split(":", 1)[1].strip()
                break
    game_date_time = game_date if game_date else ""
    game_date_time += f" {start_time}" if start_time else ""

    if game_date_time:
        # The format string that matches the date_string
        date_format = "%A %b %d, %Y %I:%M%p" if start_time else "%A %b %d, %Y"

        # convert above date_format to
        #    "%I:%M %p, %B %d, %Y"
        # Convert to datetime object
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


def get_play_by_play(soup):
    # Find the table element by its ID
    table = soup.find('table', id='pbp')

    if not table:
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))

        for comment in comments:
            if 'id=\"pbp\"' in comment:
                soup_temp = BeautifulSoup(comment, "html.parser")
                table = soup_temp.find('table', id='pbp')
                break

    # Initialize a list to store the dictionaries
    data_list = []

    # Check if the table is found
    if table:
        # Find all rows in the table's tbody
        rows = table.tbody.find_all('tr')

        # Iterate through the rows
        for row in rows:
            # Initialize an empty dictionary for each row
            row_data = {}

            # Find all cells in the row
            cells = row.find_all(['th', 'td'])

            if len(cells) != 10:
                continue

            # Iterate through the cells and extract data
            for cell in cells:
                # Get the column header (data-stat attribute)
                header = cell.get('data-stat')

                # Add data to the dictionary
                row_data[header] = cell.get_text(strip=True)

            # Append the dictionary to the list
            data_list.append(row_data)

    return data_list


def scrap_stat_table(table):
    all_game_stats = []
    if table:
        caption = table.find("caption").text
        type_tag = table.find("th", {"class": "over_header", "colspan": True})
        stats_type = type_tag.get_text(strip=True) if type_tag else "Unknown"

        header_tags = table.find_all("th", {"scope": "col"})
        headers = [
            tag["data-stat"] for tag in header_tags if "data-stat" in tag.attrs
        ]

        # Find the tbody element within the table
        tbody = table.find('tbody')

        if tbody:
            # Extract all rows from the tbody
            row_tags = tbody.find_all("tr", class_=lambda x: x != "thead")

            last_value = {}

            filtered_rows = [row for row in row_tags if not row.has_attr('class') or 'thead' not in row['class']]

            for row_tag in filtered_rows:  # Skip the header row
                row_data = {"type": stats_type, "caption": caption}
                cell_tags = row_tag.find_all(["th", "td"])
                for header, cell_tag in zip(headers, cell_tags):
                    row_data[header] = cell_tag.get_text(strip=True)
                    if not row_data[header] and header == "quarter" and header in last_value:
                        row_data[header] = last_value[header]
                    else:
                        last_value[header] = row_data[header]
                all_game_stats.append(row_data)

        else:
            # Initialize a list to store column names
            column_names = []

            # Find the column names from the first row (skipping the header row)
            header_row = table.find('tr', class_='thead onecell')
            if header_row:
                column_names = [th.get_text(strip=True) for th in table.find_all('th')]

            # Iterate through table rows (excluding the header row)
            for header, row in zip(column_names, table.find_all('tr')[1:]):
                row_data = {"type": stats_type, "caption": caption}
                cell_tag = row.find("td")
                # Extract values for each column
                row_data[header] = cell_tag.get_text(strip=True)
                all_game_stats.append(row_data)
    return all_game_stats


def populate_fields(soup):
    scraped_data = {}

    # Get sport information
    scraped_data["sport"] = "NFL"

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

    game_time = get_game_time(soup)
    scraped_data["game_time"] = game_time

    game_attendance = get_game_attendance(soup)
    scraped_data["attendance"] = game_attendance

    play_by_play_data = get_play_by_play(soup)
    scraped_data["play_by_play"] = play_by_play_data

    away_team_alternative, home_team_alternative = get_team_name_alternatives(away_team, home_team, soup)

    all_tables = get_all_tables_from_html(soup)

    away_team_stats_result, home_team_stats_result, stats_result = get_stat_results(all_tables, away_team,
                                                                                    away_team_alternative, home_team,
                                                                                    home_team_alternative)

    # scraped_data["game_stats"] = stats_result
    scraped_data["home_team_game_stats"] = home_team_stats_result
    scraped_data["away_team_game_stats"] = away_team_stats_result

    return scraped_data


def get_all_tables_from_html(soup):
    all_tables = [table for table in soup.findAll('table') if table.caption]
    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        if 'stats_table' in comment and 'id=\"pbp\"' not in comment:
            soup_temp = BeautifulSoup(comment, "html.parser")
            table = soup_temp.find('table')
            if table.caption:
                all_tables.append(table)
    return all_tables


def is_team_in_caption(caption, team_name, team_alternative):
    caption_text = caption.text.strip().lower()
    return team_name and team_name.strip().lower() in caption_text or (
            team_alternative and team_alternative.lower() in caption_text.split()
    )


def get_stat_results(all_tables, away_team, away_team_alternative, home_team, home_team_alternative):
    away_team_stats_result = []
    home_team_stats_result = []
    stats_result = []

    for table in all_tables:
        result = scrap_stat_table(table)
        if result:
            caption = table.find("caption")

            if is_team_in_caption(caption, away_team, away_team_alternative):
                away_team_stats_result.extend(result)
            elif is_team_in_caption(caption, home_team, home_team_alternative):
                home_team_stats_result.extend(result)
            else:
                stats_result.extend(result)

    return away_team_stats_result, home_team_stats_result, stats_result


def get_team_name_alternatives(away_team, home_team, soup):
    table = soup.find('table', id='scoring')
    team_name_alternatives = set()
    if not table:
        comments = soup.find_all(string=lambda text: isinstance(text, Comment))
        for comment in comments:
            if 'id=\"scoring\"' in comment:
                soup_temp = BeautifulSoup(comment, "html.parser")
                table = soup_temp.find('table')
                break
    if table:
        # Find all 'td' elements within the 'Tm' column
        tm_cells = table.select('td[data-stat="team"]')

        # Extract unique values and add them to the set
        for cell in tm_cells:
            team_name_alternatives.add(cell.get_text(strip=True))

    team_name_alternatives = list(team_name_alternatives)
    away_team_alternative = None
    home_team_alternative = None
    for name in team_name_alternatives:
        if away_team and name in away_team.split():
            away_team_alternative = name
        elif home_team and name in home_team.split():
            home_team_alternative = name
    return away_team_alternative, home_team_alternative


def request_url_data(url, max_retries=3):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    retries = 0
    response = None
    while retries <= max_retries:
        if retries == max_retries:
            break
        else:
            print(f"Requesting url: {url}", end=" ")
            response = requests.get(url, headers=headers)
            if response.status_code == 404:
                print("Page not available!!")
                response = None
                break
            elif response.status_code == 200:
                print("Url Fetched!")
                break
            elif response.status_code == 429:
                print(f"Rate-limited. Waiting for {int(response.headers['Retry-After'])} before retrying.")
                time.sleep(int(response.headers["Retry-After"]))

                retries += 1  # Increment the retries count
                if retries > max_retries:
                    print(f"Max retries reached for URL {url}.")
                    response = None
                    break
    return response


def fetch_and_parse_game_links(week_url, base_url):
    game_links = []
    game_data = []  # To store scraped data for each game

    try:
        response = request_url_data(week_url)
        if response:
            soup = BeautifulSoup(response.content, "html.parser")
            links = soup.find_all("td", {"class": "right gamelink"})
            for link in links:
                game_url = ""
                try:
                    game_url = f"{base_url}{link.find('a')['href']}"
                    game_response = request_url_data(game_url)
                    if game_response:
                        game_links.append(game_url)
                        game_info = populate_fields(BeautifulSoup(game_response.content, "html.parser"))
                        game_data.append(game_info)
                    time.sleep(10)
                except Exception as e:
                    print(f"Failed to fetch {game_url}.")
                    print(traceback.format_exc())

    except Exception as e:
        print(f"Failed to fetch {week_url}.")
        print(traceback.format_exc())

    time.sleep(uniform(3, 4))  # Wait 3 to 4 seconds between requests

    return game_links, game_data


def scrape_data(start_year=2017, end_year=2022):
    base_url = "https://www.pro-football-reference.com"
    all_game_links = []
    all_game_data = []  # To store scraped data for all games

    year_urls = [f"{base_url}/years/{year}/" for year in range(start_year, end_year + 1)]
    year_urls.reverse()

    for year_url in tqdm(year_urls):  # Replacing threading with a for loop
        print(f"Fetching and parsing week links for {year_url}")

        # Max 21 weeks in a year
        for i in range(1, 21):
            week_url = f"{year_url}week_{i}.htm"
            game_links, game_data = fetch_and_parse_game_links(week_url, base_url)
            save_to_postgresql(game_data, "game")
            all_game_links.extend(game_links)
            all_game_data.extend(game_data)
            print(f"Completed for:{week_url}")

    return all_game_links, all_game_data


if __name__ == "__main__":
    scrape_data()