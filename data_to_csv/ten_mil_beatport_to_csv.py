from collections import defaultdict
import pandas as pd
import os
import json
from tqdm import tqdm
from db_utils import convert_json_array_to_postgres_array, parse_release_date

# Currently will take about 11GB of RAM

def process_beatport_data():
    """Read beatport data from multiple CSV files, join them, and save to csvs/10m_beatport/"""
    
    # Input files
    artists_input = "data/10m-beatport/sp_artist.csv"
    artist_release_input = "data/10m-beatport/sp_artist_release.csv"
    artist_track_input = "data/10m-beatport/sp_artist_track.csv"
    releases_input = "data/10m-beatport/sp_release.csv"
    tracks_input = "data/10m-beatport/sp_track.csv"
    
    # Output files
    tracks_output_file = "csvs/10m_beatport/tracks.csv"
    artists_output_file = "csvs/10m_beatport/artists.csv"
    albums_output_file = "csvs/10m_beatport/albums.csv"
    
    os.makedirs("csvs/10m_beatport", exist_ok=True)
    
    print("Reading input files...")
    artists_df = pd.read_csv(artists_input)
    artist_release_df = pd.read_csv(artist_release_input)
    artist_track_df = pd.read_csv(artist_track_input)
    releases_df = pd.read_csv(releases_input)
    tracks_df = pd.read_csv(tracks_input)
    
    print("Processing artists...")
    artists_data = []
    for _, row in tqdm(artists_df.iterrows(), total=len(artists_df), desc="Processing artists"):
        if pd.notna(row['artist_id']) and pd.notna(row['artist_name']):
            artists_data.append({
                'spotify_uri': f"spotify:artist:{row['artist_id']}",
                'name': row['artist_name']
            })
    
    print("Processing releases (albums)...")
    albums_data = []
    
    # Create a mapping from release_id to artist_uris
    release_artist_map = defaultdict(list)
    for _, row in tqdm(artist_release_df.iterrows(), total=len(artist_release_df), desc="Building release-artist mapping"):
        if pd.notna(row['release_id']) and pd.notna(row['artist_id']):
            release_id = row['release_id']
            artist_uri = f"spotify:artist:{row['artist_id']}"
            release_artist_map[release_id].append(artist_uri)
    
    for _, row in tqdm(releases_df.iterrows(), total=len(releases_df), desc="Processing releases"):
        if pd.notna(row['release_id']) and pd.notna(row['release_title']):
            release_date = None
            precision = None
            if pd.notna(row['release_date']):
                release_date, precision = parse_release_date(row['release_date'])
            
            release_id = row['release_id']
            artist_uris = release_artist_map.get(release_id, [])
            
            albums_data.append({
                'spotify_uri': f"spotify:album:{release_id}",
                'name': row['release_title'],
                'spotify_release_date': release_date,
                'release_date_precision': precision,
                'n_tracks': int(row['total_tracks']) if pd.notna(row['total_tracks']) else None,
                'album_type': row['album_type'] if pd.notna(row['album_type']) else None,
                'artist_spotify_uris': json.dumps(artist_uris)
            })
    
    print("Processing tracks...")
    tracks_data = []
    
    # Create a mapping from track_id to artist_uris
    track_artist_map = defaultdict(list)
    for _, row in tqdm(artist_track_df.iterrows(), total=len(artist_track_df), desc="Building track-artist mapping"):
        if pd.notna(row['track_id']) and pd.notna(row['artist_id']):
            track_id = row['track_id']
            artist_uri = f"spotify:artist:{row['artist_id']}"
            track_artist_map[track_id].append(artist_uri)
    
    for _, row in tqdm(tracks_df.iterrows(), total=len(tracks_df), desc="Processing tracks"):
        if pd.notna(row['track_id']) and pd.notna(row['track_title']):
            track_id = row['track_id']
            artist_uris = track_artist_map.get(track_id, [])
            
            tracks_data.append({
                'spotify_uri': f"spotify:track:{track_id}",
                'name': row['track_title'],
                'duration_ms': int(row['duration_ms']) if pd.notna(row['duration_ms']) else None,
                'explicit': {'t': True, 'f': False}.get(row['explicit']) if pd.notna(row['explicit']) else None,
                'disc_number': int(row['disc_number']) if pd.notna(row['disc_number']) else None,
                'track_number': int(row['track_number']) if pd.notna(row['track_number']) else None,
                'album_spotify_uri': f"spotify:album:{row['release_id']}" if pd.notna(row['release_id']) else None,
                'artist_spotify_uris': json.dumps(artist_uris),
                'isrc': row['isrc'] if pd.notna(row['isrc']) else None,
            })
    
    # Save to CSV files
    print("Saving tracks CSV...")
    tracks_df_out = pd.DataFrame(tracks_data)
    tracks_df_out['artist_spotify_uris'] = tracks_df_out['artist_spotify_uris'].apply(convert_json_array_to_postgres_array)
    tracks_df_out.to_csv(tracks_output_file, index=False)
    
    print("Saving artists CSV...")
    artists_df_out = pd.DataFrame(artists_data)
    artists_df_out.to_csv(artists_output_file, index=False)
    
    print("Saving albums CSV...")
    albums_df_out = pd.DataFrame(albums_data)
    albums_df_out['artist_spotify_uris'] = albums_df_out['artist_spotify_uris'].apply(convert_json_array_to_postgres_array)
    albums_df_out.to_csv(albums_output_file, index=False)
    
    print(f"Tracks file saved to {tracks_output_file}")
    print(f"Artists file saved to {artists_output_file}")
    print(f"Albums file saved to {albums_output_file}")
    print(f"Total tracks processed: {len(tracks_df_out)}")
    print(f"Total unique artists: {len(artists_df_out)}")
    print(f"Total unique albums: {len(albums_df_out)}")


if __name__ == "__main__":
    process_beatport_data()