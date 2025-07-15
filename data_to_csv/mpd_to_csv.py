#!/usr/bin/env python

import json
from pathlib import Path
from tqdm import tqdm
import csv

MPD_DIR = Path("data/mpd")
OUTPUT_DIR = Path("csvs/mpd")
BATCH_SIZE = 100_000  # Process in batches to avoid RAM issues


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Check if CSV files already exist
    csv_files = [
        OUTPUT_DIR / "artists.csv",
        OUTPUT_DIR / "albums.csv", 
        OUTPUT_DIR / "tracks.csv"
    ]
    
    existing_files = [f for f in csv_files if f.exists()]
    if existing_files:
        print(f"Found existing CSV files:")
        for f in existing_files:
            print(f"  - {f}")
        
        response = input("Delete existing files and continue? (y/N): ").strip().lower()
        if response not in ['y', 'yes']:
            print("Aborted.")
            return
        
        # Delete existing files
        for f in existing_files:
            f.unlink()
        print("Deleted existing files.")
    
    # Open CSV files for writing
    artists_file = open(OUTPUT_DIR / "artists.csv", "w", newline="", encoding="utf-8")
    albums_file = open(OUTPUT_DIR / "albums.csv", "w", newline="", encoding="utf-8")
    tracks_file = open(OUTPUT_DIR / "tracks.csv", "w", newline="", encoding="utf-8")
    
    artists_writer = csv.writer(artists_file)
    albums_writer = csv.writer(albums_file)
    tracks_writer = csv.writer(tracks_file)
    
    # Write headers
    artists_writer.writerow(["spotify_uri", "name"])
    albums_writer.writerow(["spotify_uri", "name", "artist_spotify_uris"])
    tracks_writer.writerow(["spotify_uri", "name", "duration_ms", "album_spotify_uri", "artist_spotify_uris"])
    
    # Keep track of seen URIs to avoid duplicates
    seen_artists = set()
    seen_albums = set()
    seen_tracks = set()
    
    # Batch data
    artists_batch = []
    albums_batch = []
    tracks_batch = []
    
    def flush_batches():
        """Write current batches to CSV and clear them"""
        if artists_batch:
            artists_writer.writerows(artists_batch)
            artists_batch.clear()
        if albums_batch:
            albums_writer.writerows(albums_batch)
            albums_batch.clear()
        if tracks_batch:
            tracks_writer.writerows(tracks_batch)
            tracks_batch.clear()
    
    json_files = list(MPD_DIR.glob("*.json"))
    print(f"Processing {len(json_files)} JSON files in batches of {BATCH_SIZE:,}...")
    
    for json_file in tqdm(json_files, desc="Reading JSON"):
        with open(json_file, "rb") as f:
            data = json.loads(f.read())
        
        for playlist in data["playlists"]:
            for track in playlist["tracks"]:
                # Extract artist info
                artist_uri = track["artist_uri"]
                if artist_uri not in seen_artists:
                    seen_artists.add(artist_uri)
                    artists_batch.append([artist_uri, track["artist_name"]])
                
                # Extract album info  
                album_uri = track["album_uri"]
                if album_uri not in seen_albums:
                    seen_albums.add(album_uri)
                    albums_batch.append([
                        album_uri,
                        track["album_name"],
                    ])
                
                # Extract track info
                track_uri = track["track_uri"]
                if track_uri not in seen_tracks:
                    seen_tracks.add(track_uri)
                    tracks_batch.append([
                        track_uri,
                        track["track_name"],
                        track["duration_ms"],
                        track["album_uri"],
                        track["artist_uri"]  # Single URI in list format
                    ])
                
                # Flush when batch gets too big
                if len(tracks_batch) >= BATCH_SIZE:
                    flush_batches()
    
    # Flush remaining data
    flush_batches()
    
    # Close files
    artists_file.close()
    albums_file.close()
    tracks_file.close()
    
    print("✓ Done!")
    print(f"  artists.csv: {len(seen_artists):,} rows")
    print(f"  albums.csv: {len(seen_albums):,} rows")
    print(f"  tracks.csv: {len(seen_tracks):,} rows")


if __name__ == "__main__":
    main()