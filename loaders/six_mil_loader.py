"""
6mil_loader.py - Configuration for loading 6mil CSV files

Simple configuration using reusable SQL templates.
"""

SOURCE_NAME = "6mil"

CSV_PATHS = {
    "artists": "csvs/6mil/artists.csv",
}

# Define which columns are present in 6mil CSV files
CSV_COLUMNS = {
    "artists": ["name", "genres", "spotify_uri"],  # 6mil format includes genres
}

# Conflict resolution policy for 6mil data
POLICY = {
    "artists": {
        "name": "prefer_incoming",
        "spotify_uri": "prefer_non_null",
        "genres": "extend",
    },
}