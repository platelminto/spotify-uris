from collections import defaultdict
import pandas as pd
import os
from db_utils import get_db_connection, get_artist_uris_batch, convert_json_array_to_postgres_array


def process_spotify_data():
    """Read spotify_data.csv from 1million-tracks, extract required columns, save to csvs/1million/tracks.csv and artists.csv"""

    input_file = "data/1million-tracks/spotify_data.csv"
    tracks_output_file = "csvs/1mil_tracks/tracks.csv"
    artists_output_file = "csvs/1mil_tracks/artists.csv"

    os.makedirs("csvs/1mil_tracks", exist_ok=True)

    print(f"Reading {input_file}...")

    chunk_size = 1_000_000
    first_chunk = True

    artists = set()
    artist_genres = defaultdict(set)

    for chunk in pd.read_csv(input_file, chunksize=chunk_size):
        # Extract required columns
        selected_columns = chunk[['artist_name', 'track_name', 'track_id', 'genre', 'duration_ms']]
        # Filter out NaN values before adding to set
        valid_artists = selected_columns['artist_name'].dropna()
        artists.update(valid_artists)
        
        # Collect genres per artist
        for _, row in selected_columns.iterrows():
            if pd.notna(row['artist_name']) and pd.notna(row['genre']):
                artist_genres[row['artist_name']].add(row['genre'])
    
    artist_uris = {}

    print(f"Found {len(artists)} unique artists. Fetching URIs from database...")
    # Get artist URIs from the database
    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            artist_uris_batch = get_artist_uris_batch(list(artists), cursor)
            
            artist_uris.update(artist_uris_batch)

    name_counts = defaultdict(int)
    for name in artist_uris.values():
        name_counts[name] += 1
    
    # Filter out artists with non-unique names (we can't determine which it is)
    unique_artist_uris = {v: k for k, v in artist_uris.items() if name_counts[v] == 1}

    # Process tracks CSV
    for chunk in pd.read_csv(input_file, chunksize=chunk_size):
        selected_columns = chunk[['artist_name', 'track_name', 'track_id', 'duration_ms']].copy()
        
        # Add artist_uri and transform track_id to track_uri
        selected_columns['artist_spotify_uris'] = selected_columns['artist_name'].map(unique_artist_uris)
        selected_columns['spotify_uri'] = 'spotify:track:' + selected_columns['track_id'].astype(str)
        selected_columns['name'] = selected_columns['track_name']

        # Reorder columns for final output
        tracks_df = selected_columns[['spotify_uri', 'name', 'duration_ms', 'artist_spotify_uris']]
        
        if first_chunk:
            tracks_df.to_csv(tracks_output_file, index=False, mode="w")
            first_chunk = False
        else:
            tracks_df.to_csv(tracks_output_file, index=False, mode="a", header=False)

    # Create artists CSV
    print("Creating artists CSV...")
    artists_data = []
    for artist_name in artists:
        if artist_name in unique_artist_uris:
            artist_uri = unique_artist_uris[artist_name]
            genres_list = list(artist_genres[artist_name]) if artist_name in artist_genres else []
            postgres_genres = convert_json_array_to_postgres_array(str(genres_list))
            artists_data.append({
                'spotify_uri': artist_uri,
                'genres': postgres_genres
            })
    
    artists_df = pd.DataFrame(artists_data)
    artists_df.to_csv(artists_output_file, index=False)

    print(f"Tracks file saved to {tracks_output_file}")
    print(f"Artists file saved to {artists_output_file}")

    total_rows = sum(1 for _ in pd.read_csv(tracks_output_file, chunksize=chunk_size))
    print(f"Total rows processed: {total_rows * chunk_size}")


if __name__ == "__main__":
    process_spotify_data()