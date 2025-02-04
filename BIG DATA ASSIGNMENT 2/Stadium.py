import csv
import pymongo
from datetime import datetime

# MongoDB connection setup
mongo_client = pymongo.MongoClient("mongodb://localhost:27017/")
fifa_db = mongo_client['FIFA_DB']

def clean_value(value):
    """Remove extra quotes and whitespace from CSV data."""
    return value.strip("'").strip('"').strip()

def process_matches_as_individual_documents(match_path):
    match_collection = fifa_db.matches
    # Clear the collection to avoid duplicates
    match_collection.delete_many({})

    all_matches = []

    with open(match_path, 'r') as match_file:
        match_reader = csv.DictReader(match_file)

        for match_row in match_reader:
            # Create a document for each match
            match_doc = {
                "MatchID": int(match_row["match_id"]),
                "Date": parse_date(match_row["date"]),
                "StartTime": clean_value(match_row["start_time"]),
                "Team1": clean_value(match_row["team1"]),
                "Team2": clean_value(match_row["team2 "]),
                "Team1Score": int(match_row["team1_score"]),
                "Team2Score": int(match_row["team2_score"]),
                "Stadium": clean_value(match_row["stadium"]),
                "HostCity": clean_value(match_row["host_city"])
            }
            all_matches.append(match_doc)

    # Insert all matches into MongoDB
    match_collection.insert_many(all_matches)
    print(f"{len(all_matches)} matches inserted successfully into MongoDB!")

def parse_date(date_str):
    """Parse date from string, handling errors."""
    date_str = clean_value(date_str)
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        print(f"Invalid date format: {date_str}")
        return None

# Example usage with the corrected match path
process_matches_as_individual_documents(
    r"E:\BIG DATA ASSIGNMENT 2\MATCH.csv"
)
