#!/usr/bin/env python
"""
Convert all JSON files in data/mpd/ into three deduplicated CSV files:

  artists.csv     spotify_uri, mbid, name
  albums.csv      spotify_uri, mbid, name, first_artist_spotify_uri
  tracks.csv      spotify_uri, mbid, name, duration_ms,
                  album_spotify_uri, first_artist_spotify_uri

Strategy
========
• One worker process per JSON file → pulls lists of track dicts.
• Each worker returns three Python sets (artists/albums/tracks) so
  the main process can merge/dedupe cheaply.
• Main process batches 100 k new rows per CSV flush to limit RAM.

"""

# Data is from https://www.kaggle.com/datasets/himanshuwagh/spotify-million/data

import csv, orjson as json, pathlib, concurrent.futures, itertools
from collections import defaultdict
from tqdm import tqdm

CPU_LIMIT = 12

MPD_DIR  = pathlib.Path("data/mpd")
BATCH    = 100_000                  # flush to disk every N *new* rows
CSV_ART  = "csvs/artists.csv"
CSV_ALB  = "csvs/albums.csv"
CSV_TRK  = "csvs/tracks.csv"


def parse_file(path: pathlib.Path):
    with path.open("rb") as fh:
        doc = json.loads(fh.read())
    artist_set, album_set, track_set = set(), set(), set()

    for pl in doc["playlists"]:
        for t in pl["tracks"]:
            artist_uri = t["artist_uri"]
            album_uri = t["album_uri"]
            track_uri = t["track_uri"]

            artist_set.add((artist_uri, t["artist_name"]))
            album_set.add((album_uri, t["album_name"], artist_uri))
            track_set.add((
                track_uri, t["track_name"], t["duration_ms"],
                album_uri, artist_uri
            ))
    return artist_set, album_set, track_set

def csv_writer(path, header):
    fh = open(path, "w", newline="", encoding="utf-8")
    w  = csv.writer(fh)
    w.writerow(header)
    return fh, w

def main():
    # open CSVs & buffers
    fh_artist, writer_artist   = csv_writer(CSV_ART, ["spotify_uri", "mbid", "name"])
    fh_album, writer_album = csv_writer(CSV_ALB, ["spotify_uri", "mbid", "name", "first_artist_spotify_uri"])
    fh_track, writer_track = csv_writer(CSV_TRK,
                           ["spotify_uri","mbid","name","duration_ms",
                            "album_spotify_uri","first_artist_spotify_uri"])

    buffer_artist, buffer_album, buffer_track = [], [], []
    seen_artist, seen_album, seen_track = set(), set(), set()

    def flush():
        if buffer_artist:
            writer_artist.writerows(buffer_artist); buffer_artist.clear()
        if buffer_album:
            writer_album.writerows(buffer_album); buffer_album.clear()
        if buffer_track:
            writer_track.writerows(buffer_track); buffer_track.clear()

    json_files = sorted(MPD_DIR.glob("*.json"))
    with concurrent.futures.ProcessPoolExecutor(max_workers=CPU_LIMIT) as ex:
        for artist_set, album_set, track_set in tqdm(ex.map(parse_file, json_files),
                                         total=len(json_files),
                                         desc="Parsing"):
            # Artists
            for artist in artist_set:
                if artist[0] not in seen_artist:
                    seen_artist.add(artist[0])
                    buffer_artist.append([artist[0], "", artist[1]])
            # Albums
            for album in album_set:
                if album[0] not in seen_album:
                    seen_album.add(album[0])
                    buffer_album.append([album[0], "", album[1], album[2]])
            # Tracks
            for track in track_set:
                if track[0] not in seen_track:
                    seen_track.add(track[0])
                    buffer_track.append([
                        track[0], "", track[1], track[2], track[3], track[4]
                    ])
            if len(buffer_track) >= BATCH:
                flush()

    flush()
    for fh in (fh_artist, fh_album, fh_track): fh.close()
    print("✓ CSVs ready:", CSV_ART, CSV_ALB, CSV_TRK)

if __name__ == "__main__":
    main()
