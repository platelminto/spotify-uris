#!/usr/bin/env python
"""
load_csv.py  –  fast CSV → PostgreSQL with conflict policy,
row-provenance, metrics, and conflict logging.

"""

import os, sys, pathlib, time, psycopg
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()                       # reads PG_URL or DATABASE_URL

csv_path = pathlib.Path("csvs/tracks.csv")
SOURCE   = "MPD"
TIMESTAMP = datetime.now(timezone.utc)

# ────────────────────────────── CSV Column Definitions
# Define which columns are present in your CSV files
CSV_COLUMNS = {
    "artists": ["spotify_uri", "mbid", "name"],
    "albums": ["spotify_uri", "mbid", "name", "first_artist_spotify_uri"],
    "tracks": ["spotify_uri", "mbid", "name", "duration_ms", "album_spotify_uri", "first_artist_spotify_uri"],
}

# Define all possible columns for each entity (for the database tables)
ALL_COLUMNS = {
    "artists": {
        "spotify_uri": "text",
        "mbid": "text", 
        "name": "citext"
    },
    "albums": {
        "spotify_uri": "text",
        "mbid": "text",
        "name": "citext",
        "album_type": "text",
        "spotify_release_date": "date",
        "release_date_precision": "text",
        "n_tracks": "int",
        "first_artist_spotify_uri": "text"
    },
    "tracks": {
        "spotify_uri": "text",
        "mbid": "text",
        "name": "citext",
        "duration_ms": "int",
        "album_spotify_uri": "text",
        "explicit": "bool",
        "disc_number": "int",
        "track_number": "int",
        "first_artist_spotify_uri": "text"
    }
}

# Debug: Check first few lines of CSV
with open(csv_path, 'r') as f:
    for i in range(5):
        line = f.readline().strip()
        print(f"[DEBUG] Line {i}: {line}")

# ────────────────────────────── Conflict-resolution policy
POLICY = {
    "artists": {
        "name": "prefer_incoming",
        "mbid": "prefer_non_null",
        "spotify_uri": "prefer_non_null",
    },
    "albums": {
        "name": "prefer_incoming",
        "album_type": "prefer_non_null",
        "spotify_release_date": "prefer_non_null",
        "release_date_precision": "prefer_non_null",
        "n_tracks": "prefer_non_null",
        "mbid": "prefer_non_null",
        "spotify_uri": "prefer_non_null",
    },
    "tracks": {
        "name": "prefer_incoming",
        "duration_ms": "prefer_non_null",
        "explicit": "prefer_non_null",
        "disc_number": "prefer_non_null",
        "track_number": "prefer_non_null",
        "album_id": "prefer_non_null",
        "mbid": "prefer_non_null",
        "spotify_uri": "prefer_non_null",
    },
}

def build_set(entity: str, cols: list[str]) -> str:
    """Generate SET clause obeying POLICY."""
    rules = POLICY[entity]
    parts = []
    # Only update columns that are in our CSV
    csv_cols = CSV_COLUMNS # Remove plural 's'
    for col in cols:
        if col in csv_cols:  # Only if column exists in CSV
            mode = rules.get(col, "prefer_incoming")
            if mode == "prefer_incoming":
                parts.append(f"{col}=EXCLUDED.{col}")
            elif mode == "prefer_non_null":
                parts.append(f"{col}=COALESCE({entity}.{col},EXCLUDED.{col})")
            elif mode == "prefer_longer":
                parts.append(
                    f"{col}=CASE WHEN length(EXCLUDED.{col})>length({entity}.{col}) "
                    f"THEN EXCLUDED.{col} ELSE {entity}.{col} END")
    parts.append(f"source_name='{SOURCE}'")
    parts.append(f"ingested_at='{TIMESTAMP.isoformat()}'")
    return ", ".join(parts)

# Helper to get column value or NULL
def col_or_null(entity: str, col: str, prefix: str = "src") -> str:
    """Return column reference if in CSV, otherwise NULL"""
    if col in CSV_COLUMNS[entity]:
        return f"{prefix}.{col}"
    return "NULL"

# ────────────────────────────── Merge builders
def artist_merge():
    # Only update columns that exist in CSV
    updatable_cols = [c for c in ["name", "mbid", "spotify_uri"] if c in CSV_COLUMNS["artists"]]
    upd = build_set("artists", updatable_cols)
    
    return f"""
WITH src AS (
    SELECT s.spotify_uri, s.mbid, s.name,
           '{SOURCE}'::text AS source_name,
           '{TIMESTAMP.isoformat()}'::timestamptz AS ingested_at
    FROM staging_artists s
),
up1 AS (
    INSERT INTO artists (spotify_uri, mbid, name, source_name, ingested_at)
    SELECT src.spotify_uri,
           NULLIF(src.mbid,'')::uuid,
           src.name, src.source_name, src.ingested_at
    FROM src WHERE src.spotify_uri IS NOT NULL
    ON CONFLICT (spotify_uri) DO UPDATE SET {upd}
    RETURNING TRUE AS updated
),
up2 AS (
    INSERT INTO artists (spotify_uri, mbid, name, source_name, ingested_at)
    SELECT src.spotify_uri,
           NULLIF(src.mbid,'')::uuid,
           src.name, src.source_name, src.ingested_at
    FROM src WHERE src.spotify_uri IS NULL
    ON CONFLICT (mbid) DO UPDATE SET {upd}
    RETURNING TRUE AS updated
)
SELECT count(*) FROM up1
UNION ALL
SELECT count(*) FROM up2;
"""

def album_merge():
    # Only update columns that exist in CSV
    updatable_cols = [c for c in ["name","album_type","spotify_release_date","release_date_precision",
                                  "n_tracks","mbid","spotify_uri"] if c in CSV_COLUMNS["albums"]]
    upd = build_set("albums", updatable_cols)
    
    # Build insert column list dynamically
    insert_cols = ["spotify_uri", "mbid", "name", "source_name", "ingested_at"]
    insert_vals = [
        col_or_null("albums", "spotify_uri"),
        "NULLIF(" + col_or_null("albums", "mbid") + ",'')::uuid",
        col_or_null("albums", "name"),
        "src.source_name",
        "src.ingested_at"
    ]
    
    # Add optional columns if they exist in CSV
    optional_cols = ["album_type", "spotify_release_date", "release_date_precision", "n_tracks"]
    for col in optional_cols:
        if col in CSV_COLUMNS["albums"]:
            insert_cols.append(col)
            insert_vals.append(col_or_null("albums", col))
    
    return f"""
WITH src AS (
    SELECT *, '{SOURCE}'::text AS source_name,
           '{TIMESTAMP.isoformat()}'::timestamptz AS ingested_at
    FROM staging_albums
),
up AS (
    INSERT INTO albums ({', '.join(insert_cols)})
    SELECT {', '.join(insert_vals)}
    FROM src
    ON CONFLICT (spotify_uri) DO UPDATE SET {upd}
    RETURNING id, spotify_uri, TRUE AS updated
)
INSERT INTO album_artists (album_id, artist_id, position)
SELECT up.id, a.id, 0
FROM staging_albums s
JOIN up       ON up.spotify_uri = s.spotify_uri
JOIN artists a ON a.spotify_uri = s.first_artist_spotify_uri
ON CONFLICT DO NOTHING;
"""

def track_merge():
    # Only update columns that exist in CSV
    updatable_cols = [c for c in ["name","duration_ms","explicit","disc_number","track_number",
                                  "album_id","mbid","spotify_uri"] if c in CSV_COLUMNS["tracks"]]
    upd = build_set("tracks", updatable_cols)
    
    # Build insert column list dynamically
    insert_cols = ["spotify_uri", "mbid", "name", "source_name", "ingested_at"]
    insert_vals = [
        col_or_null("tracks", "spotify_uri"),
        "NULLIF(" + col_or_null("tracks", "mbid") + ",'')::uuid",
        col_or_null("tracks", "name"),
        "src.source_name",
        "src.ingested_at"
    ]
    
    # Always include album_id (via join)
    insert_cols.append("album_id")
    insert_vals.append("al.id")
    
    # Add optional columns if they exist in CSV
    optional_cols = ["duration_ms", "explicit", "disc_number", "track_number"]
    for col in optional_cols:
        if col in CSV_COLUMNS["tracks"]:
            insert_cols.append(col)
            insert_vals.append(col_or_null("tracks", col))
    
    return f"""
WITH src AS (
    SELECT *, '{SOURCE}'::text AS source_name,
           '{TIMESTAMP.isoformat()}'::timestamptz AS ingested_at
    FROM staging_tracks
),
up AS (
    INSERT INTO tracks ({', '.join(insert_cols)})
    SELECT {', '.join(insert_vals)}
    FROM src
    LEFT JOIN albums al ON al.spotify_uri = {col_or_null("tracks", "album_spotify_uri")}
    ON CONFLICT (spotify_uri) DO UPDATE SET {upd}
    RETURNING id, spotify_uri, TRUE AS updated
)
INSERT INTO track_artists (track_id, artist_id, position)
SELECT up.id, a.id, 0
FROM staging_tracks s
JOIN up        ON up.spotify_uri = s.spotify_uri
JOIN artists a ON a.spotify_uri = s.first_artist_spotify_uri
ON CONFLICT DO NOTHING;
"""

# ────────────────────────────── Settings per CSV
def build_staging_ddl(entity: str) -> str:
    """Build DDL for staging table based on CSV columns"""
    cols = []
    for col in CSV_COLUMNS[entity]:
        col_type = ALL_COLUMNS[entity].get(col, "text")
        cols.append(f"{col} {col_type}")
    return ", ".join(cols)

SETTINGS = {
    "artists.csv": dict(
        entity="artists",
        staging="staging_artists",
        ddl=build_staging_ddl("artists"),
        columns=CSV_COLUMNS["artists"],
        merge_sql=artist_merge(),
    ),
    "albums.csv": dict(
        entity="albums",
        staging="staging_albums",
        ddl=build_staging_ddl("albums"),
        columns=CSV_COLUMNS["albums"],
        merge_sql=album_merge(),
    ),
    "tracks.csv": dict(
        entity="tracks",
        staging="staging_tracks",
        ddl=build_staging_ddl("tracks"),
        columns=CSV_COLUMNS["tracks"],
        merge_sql=track_merge(),
    ),
}

# ────────────────────────────── Main
def main():
    if csv_path.name not in SETTINGS:
        sys.exit("File must be artists.csv, albums.csv or tracks.csv")
    cfg = SETTINGS[csv_path.name]
    t0   = time.time()
    pg_url = os.getenv("PG_URL") or os.getenv("DATABASE_URL")
    if not pg_url:
        sys.exit("Set PG_URL or DATABASE_URL in your .env")
    with psycopg.connect(pg_url, autocommit=False) as conn:
        # Drop and recreate staging table to ensure correct types
        conn.execute(f"DROP TABLE IF EXISTS {cfg['staging']};")
        conn.execute(f"CREATE UNLOGGED TABLE IF NOT EXISTS {cfg['staging']} ({cfg['ddl']});")
        # count before
        before = conn.execute(f"SELECT count(*) FROM {cfg['entity']}").fetchone()[0]
        try:
            with conn.cursor() as cur:
                # Use psycopg3's context manager for COPY
                col_list = ", ".join(cfg['columns'])
                with cur.copy(
                    f"COPY {cfg['staging']} ({col_list}) "
                    "FROM STDIN WITH CSV HEADER"
                ) as copy:
                    with open(csv_path, "rb") as f:
                        while data := f.read(8192):  # Read in chunks
                            copy.write(data)
            # Check how many rows landed
            rows_in_staging = conn.execute(
                f"SELECT count(*) FROM {cfg['staging']}").fetchone()[0]
            print(f"[DEBUG] copied {rows_in_staging:,} → {cfg['staging']}")
        except Exception as e:
            print(f"[ERROR] COPY failed: {type(e).__name__}: {e}")
            raise
        # merge
        conn.execute(cfg["merge_sql"])
        after = conn.execute(f"SELECT count(*) FROM {cfg['entity']}").fetchone()[0]
        conn.commit()
        elapsed = time.time() - t0
        print(f"✓ {csv_path.name}: +{after-before:,} rows | {elapsed:.1f}s | source '{SOURCE}'")

if __name__ == "__main__":
    main()