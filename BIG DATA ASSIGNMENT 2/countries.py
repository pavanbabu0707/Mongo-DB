import csv
import pymongo
from datetime import datetime

# MongoDB connection setup
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
fifa_db = mongo_client['FIFA_DB']

def clean_value(value):
    """Remove extra quotes and whitespace from CSV data."""
    return value.strip("'").strip('"').strip()

def process_country_data(country_path, player_path, disciplinary_path, goal_path, winner_path):
    country_collection = fifa_db.countries
    player_data = prepare_player_data(player_path, disciplinary_path, goal_path)

    with open(country_path, 'r') as country_file:
        reader = csv.DictReader(country_file)
        for row in reader:
            cname = clean_value(row.get("country_name", "Unknown"))

            country_doc = {
                "Name": cname,
                "Capital": clean_value(row.get("capital", "")),
                "Population": float(row.get("population", 0)),
                "Manager": clean_value(row.get("manager_name", "")),
                "Players": player_data.get(cname, []),
                "WorldCupHistory": fetch_world_cup_history(winner_path, cname)
            }

            country_collection.insert_one(country_doc)
    print("Country data processed and inserted successfully!")

def prepare_player_data(player_path, disciplinary_path, goal_path):
    players_by_country = {}

    with open(player_path, 'r') as player_file:
        reader = csv.DictReader(player_file)
        for row in reader:
            country = clean_value(row.get("country", "Unknown"))
            player_id = clean_value(row["player_id"])

            dob = clean_value(row["dob"])
            try:
                parsed_dob = datetime.strptime(dob, "%Y-%m-%d")
            except ValueError:
                print(f"Skipping player {player_id} due to invalid date: {dob}")
                continue

            player = {
                "PlayerID": player_id,
                "FullName": clean_value(row["full_name"]),
                "FirstName": clean_value(row["fname"]),
                "LastName": clean_value(row["lname"]),
                "Height": float(row.get("height", 0)),
                "DateOfBirth": parsed_dob,
                "IsCaptain": row["is_captain"].strip().lower() == 'true',
                "Position": clean_value(row["position"]),
                "DisciplinaryRecord": get_disciplinary_record(disciplinary_path, player_id),
                "PerformanceStats": get_performance_stats(goal_path, player_id)
            }

            if country not in players_by_country:
                players_by_country[country] = []
            players_by_country[country].append(player)

    return players_by_country

def get_disciplinary_record(disciplinary_path, player_id):
    with open(disciplinary_path, 'r') as disc_file:
        reader = csv.DictReader(disc_file)
        for row in reader:
            if clean_value(row["player_id"]) == player_id:
                return {
                    "YellowCards": int(row.get("no_of_yellow_cards", 0)),
                    "RedCards": int(row.get("no_of_red_cards", 0))
                }
    return {"YellowCards": 0, "RedCards": 0}

def get_performance_stats(goal_path, player_id):
    with open(goal_path, 'r') as goal_file:
        reader = csv.DictReader(goal_file)
        for row in reader:
            if clean_value(row["player_id"]) == player_id:
                return {
                    "Goals": int(row.get("goals", 0)),
                    "Assists": int(row.get("assists", 0)),
                    "MinutesPlayed": int(row.get("minutes_played", 0))
                }
    return {"Goals": 0, "Assists": 0, "MinutesPlayed": 0}

def fetch_world_cup_history(winner_path, country_name):
    history = []
    with open(winner_path, 'r') as winner_file:
        reader = csv.DictReader(winner_file)
        for row in reader:
            if clean_value(row["Winner"]) == country_name:
                history.append({
                    "Year": int(row["Year"]),
                    "Host": clean_value(row["Host"])
                })
    return history

# Example usage
process_country_data(
    r"E:\BIG DATA ASSIGNMENT 2\COUNTRY.csv",
    r"E:\BIG DATA ASSIGNMENT 2\PLAYER.csv",
    r"E:\BIG DATA ASSIGNMENT 2\DISCIPLINARY_RECORD.csv",
    r"E:\BIG DATA ASSIGNMENT 2\GOAL_SCORER.csv",
    r"E:\BIG DATA ASSIGNMENT 2\WORLD_CUP_WINNER.csv"
)
