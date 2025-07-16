import pandas as pd
import os
import ast
import json
import re
from collections import defaultdict
from tqdm import tqdm
from db_utils import convert_json_array_to_postgres_array, parse_release_date



def process_spotify_data():
    """Read tracks_features.csv from 1.2m-songs, extract required columns, save to csvs/1mil_songs/tracks.csv, artists.csv, and albums.csv"""

    input_file = "data/1.2m-songs/tracks_features.csv"
    tracks_output_file = "csvs/1mil_songs/tracks.csv"
    artists_output_file = "csvs/1mil_songs/artists.csv"
    albums_output_file = "csvs/1mil_songs/albums.csv"

    os.makedirs("csvs/1mil_songs", exist_ok=True)

    print(f"Reading {input_file}...")
    df = pd.read_csv(input_file)

    tracks_data = []
    artists_data = []
    seen_artists = set()
    
    # Track album info: album_id -> {name, release_dates, artist_uris, track_count}
    # Have this weird code cos first we were gonna filter if release_date was only on one song but nah it looks good
    album_info = defaultdict(lambda: {'name': None, 'release_dates': set(), 'track_count': 0})

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing tracks"):
        if pd.notna(row['artists']) and pd.notna(row['artist_ids']):
            try:
                artist_list = ast.literal_eval(row['artists'])
                artist_id_list = ast.literal_eval(row['artist_ids'])
                
                if artist_list and artist_id_list and len(artist_list) == len(artist_id_list):
                    artist_uris = []
                    
                    # Process all artists
                    for artist_name, artist_id in zip(artist_list, artist_id_list):
                        artist_uri = f"spotify:artist:{artist_id}"
                        artist_uris.append(artist_uri)
                        
                        # Add to artists data if not seen
                        if artist_uri not in seen_artists:
                            seen_artists.add(artist_uri)
                            artists_data.append({
                                'spotify_uri': artist_uri,
                                'name': artist_name
                            })

                    if pd.notna(row['album_id']):
                        album_id = row['album_id']
                        album_info[album_id]['name'] = row['album']
                        album_info[album_id]['track_count'] += 1
                        
                    if pd.notna(row['release_date']):
                        parsed_date, precision = parse_release_date(row['release_date'])
                        if parsed_date:
                            album_info[album_id]['release_dates'].add((row['release_date'], parsed_date, precision))

                    # Add track data
                    tracks_data.append({
                        'spotify_uri': f"spotify:track:{row['id']}",
                        'name': row['name'],
                        'duration_ms': int(row['duration_ms']) if pd.notna(row['duration_ms']) else None,
                        'explicit': bool(row['explicit']) if pd.notna(row['explicit']) else None,
                        'disc_number': int(row['disc_number']) if pd.notna(row['disc_number']) else None,
                        'track_number': int(row['track_number']) if pd.notna(row['track_number']) else None,
                        'album_spotify_uri': f"spotify:album:{row['album_id']}" if pd.notna(row['album_id']) else None,
                        'artist_spotify_uris': json.dumps(artist_uris)
                    })
            except (ValueError, SyntaxError):
                continue

    # Create albums CSV
    print("Creating albums CSV...")
    albums_list = []
    for album_id, info in album_info.items():
        release_data = None
        precision = None
        if info['release_dates']:
            # Take the first release date info (original, parsed, precision)
            release_info = list(info['release_dates'])[0]
            release_data = release_info[1]  # parsed date
            precision = release_info[2]     # precision
        
        albums_list.append({
            'spotify_uri': f"spotify:album:{album_id}",
            'name': info['name'],
            'spotify_release_date': release_data,
            'release_date_precision': precision
        })

    tracks_df = pd.DataFrame(tracks_data)
    tracks_df['artist_spotify_uris'] = tracks_df['artist_spotify_uris'].apply(convert_json_array_to_postgres_array)
    tracks_df.to_csv(tracks_output_file, index=False)

    artists_df = pd.DataFrame(artists_data)
    artists_df.to_csv(artists_output_file, index=False)

    albums_df = pd.DataFrame(albums_list)
    albums_df.to_csv(albums_output_file, index=False)

    print(f"Tracks file saved to {tracks_output_file}")
    print(f"Artists file saved to {artists_output_file}")
    print(f"Albums file saved to {albums_output_file}")
    print(f"Total tracks processed: {len(tracks_df)}")
    print(f"Total unique artists: {len(artists_df)}")
    print(f"Total unique albums: {len(albums_df)}")


if __name__ == "__main__":
    process_spotify_data()