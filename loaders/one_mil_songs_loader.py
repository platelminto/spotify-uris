"""
one_mil_songs_loader.py - Configuration for loading 1mil songs CSV files

Simple configuration using reusable SQL templates.
"""

SOURCE_URL = "https://www.kaggle.com/datasets/rodolfofigueroa/spotify-12m-songs"

SOURCE_NAME = "1mil_songs"

CSV_PATHS = {
    "artists": "csvs/1mil_songs/artists.csv",
    "albums": "csvs/1mil_songs/albums.csv", 
    "tracks": "csvs/1mil_songs/tracks.csv",
}

# Define which columns are present in 1mil songs CSV files
CSV_COLUMNS = {
    "artists": ["spotify_uri", "name"],
    "albums": ["spotify_uri", "name", "spotify_release_date", "release_date_precision"],
    "tracks": [
        "spotify_uri",
        "name", 
        "duration_ms",
        "explicit",
        "disc_number",
        "track_number",
        "album_spotify_uri",
        "artist_spotify_uris",
    ],
}

# Conflict resolution policy for 1mil songs data
POLICY = {
    "artists": {
        "name": "prefer_non_null",
        "spotify_uri": "prefer_non_null",
    },
    "albums": {
        "name": "prefer_incoming",
        "spotify_uri": "prefer_non_null",
        "spotify_release_date": "prefer_non_null",
        "release_date_precision": "prefer_non_null",
    },
    "tracks": {
        "name": "prefer_incoming",
        "spotify_uri": "prefer_incoming",
        "duration_ms": "prefer_incoming",
        "explicit": "prefer_non_null",
        "disc_number": "prefer_non_null", 
        "track_number": "prefer_non_null",
        "album_spotify_uri": "prefer_non_null",
        "artists": "prefer_incoming",
    },
}