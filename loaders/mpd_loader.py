"""
mpd_loader.py - Configuration for loading MPD (Million Playlist Dataset) CSV files

Simple configuration using reusable SQL templates.
"""

SOURCE_URL = "https://www.kaggle.com/datasets/himanshuwagh/spotify-million"

SOURCE_NAME = "MPD"

CSV_PATHS = {
    "artists": "csvs/mpd/artists.csv",
    "albums": "csvs/mpd/albums.csv",
    "tracks": "csvs/mpd/tracks.csv",
}

# Define which columns are present in MPD CSV files
CSV_COLUMNS = {
    "artists": ["spotify_uri", "name"],  # MPD format (no mbid, no genres)
    "albums": ["spotify_uri", "name"],
    "tracks": [
        "spotify_uri",
        "name",
        "duration_ms",
        "album_spotify_uri",
        "artist_spotify_uris",
    ],
}

# Conflict resolution policy for MPD data
POLICY = {
    "artists": {
        "name": "prefer_non_null",
        "spotify_uri": "prefer_non_null",
    },
    "albums": {
        "name": "prefer_incoming",
        "spotify_uri": "prefer_non_null",
    },
    "tracks": {
        "name": "prefer_incoming",
        "spotify_uri": "prefer_non_null",
        "duration_ms": "prefer_non_null",
        "artists": "prefer_non_null",
    },
}
