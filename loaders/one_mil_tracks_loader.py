"""
one_mil_tracks_loader.py - Configuration for loading 1mil CSV files

Simple configuration using reusable SQL templates.
"""

SOURCE_URL = "https://www.kaggle.com/datasets/amitanshjoshi/spotify-1million-tracks"

SOURCE_NAME = "1mil_tracks"

CSV_PATHS = {
    "artists": "csvs/1mil_tracks/artists.csv",
    "tracks": "csvs/1mil_tracks/tracks.csv",
}

# Define which columns are present in 
CSV_COLUMNS = {
    "artists": ["spotify_uri", "genres"], 
    "tracks": ["spotify_uri", "name", "duration_ms", "artist_spotify_uris"],
}

# Conflict resolution policy
POLICY = {
    "artists": {
        "spotify_uri": "prefer_non_null",
        "genres": "extend",
    },
    "tracks": {
        "spotify_uri": "prefer_non_null",
        "name": "prefer_incoming",
        "duration_ms": "prefer_incoming",
        "artists": "prefer_non_null",
    },
}