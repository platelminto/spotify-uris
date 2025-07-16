"""
ten_mil_beatport_loader.py - Configuration for loading 10M Beatport CSV files

Simple configuration using reusable SQL templates.
"""

SOURCE_URL = "https://www.kaggle.com/datasets/mcfurland/10-m-beatport-tracks-spotify-audio-features"

SOURCE_NAME = "10m_beatport"

CSV_PATHS = {
    "artists": "csvs/10m_beatport/artists.csv",
    "albums": "csvs/10m_beatport/albums.csv", 
    "tracks": "csvs/10m_beatport/tracks.csv",
}

# Define which columns are present in 10M Beatport CSV files
CSV_COLUMNS = {
    "artists": ["spotify_uri", "name"],
    "albums": [
        "spotify_uri", 
        "name", 
        "spotify_release_date", 
        "release_date_precision",
        "n_tracks",
        "album_type",
        "artist_spotify_uris",
    ],
    "tracks": [
        "spotify_uri",
        "name", 
        "duration_ms",
        "explicit",
        "disc_number",
        "track_number",
        "album_spotify_uri",
        "artist_spotify_uris",
        "isrc",
    ],
}

# Conflict resolution policy for 10M Beatport data
POLICY = {
    "artists": {
        "name": "prefer_incoming",
        "spotify_uri": "prefer_non_null",
    },
    "albums": {
        "name": "prefer_incoming",
        "spotify_uri": "prefer_non_null",
        "spotify_release_date": "prefer_incoming",
        "release_date_precision": "prefer_incoming",
        "n_tracks": "prefer_non_null",
        "album_type": "prefer_non_null",
        "artists": "prefer_non_null",
    },
    "tracks": {
        "name": "prefer_incoming",
        "spotify_uri": "prefer_non_null",
        "duration_ms": "prefer_incoming",
        "explicit": "prefer_incoming",
        "disc_number": "prefer_non_null", 
        "track_number": "prefer_non_null",
        "album_spotify_uri": "prefer_non_null",
        "artists": "prefer_non_null",
        "isrc": "prefer_non_null",
    },
}