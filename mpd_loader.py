"""
mpd_loader.py - Configuration for loading MPD (Million Playlist Dataset) CSV files

Defines CSV structure and merge logic for MPD format data.
"""

from load_csv_engine import DEFAULT_POLICY

DEFAULT_SOURCE = "MPD"

# Define which columns are present in MPD CSV files
CSV_COLUMNS = {
    "artists": ["spotify_uri", "name"],  # MPD format (no mbid, no genres)
    "albums": [
        "spotify_uri",
        "name",
        "artist_spotify_uris",
    ],  # Now with artist lists!
    "tracks": [
        "spotify_uri",
        "name",
        "duration_ms",
        "album_spotify_uri",
        "artist_spotify_uris",
    ],  # Now with artist lists!
}

POLICY = {
    "artists": {
        "name": "prefer_incoming",
        "spotify_uri": "prefer_non_null",
    },
    "albums": {
        "name": "prefer_incoming",
        "album_type": "prefer_non_null",
        "spotify_release_date": "prefer_non_null",
        "release_date_precision": "prefer_non_null",
        "n_tracks": "prefer_non_null",
    },
    "tracks": {
        "name": "prefer_incoming",
        "duration_ms": "prefer_non_null",
        "explicit": "prefer_non_null",
        "disc_number": "prefer_non_null",
        "track_number": "prefer_non_null",
    },
}


def get_policy(entity):
    """Get policy for entity, merging default with config-specific overrides"""
    default = DEFAULT_POLICY.get(entity, {})
    config_policy = POLICY.get(entity, {})

    # Start with default, override with config-specific values
    merged = default.copy()
    merged.update(config_policy)

    # Only keep policies for columns that exist in this CSV
    csv_columns = CSV_COLUMNS.get(entity, [])
    return {col: policy for col, policy in merged.items() if col in csv_columns}


def build_set(entity: str, cols: list[str], source: str, timestamp: str) -> str:
    """Generate SET clause obeying policy."""
    policy = get_policy(entity)
    parts = []
    csv_cols = CSV_COLUMNS[entity]
    for col in cols:
        if col in csv_cols:  # Only if column exists in CSV
            mode = policy.get(col, "prefer_incoming")
            if mode == "prefer_incoming":
                parts.append(f"{col}=EXCLUDED.{col}")
            elif mode == "prefer_non_null":
                parts.append(f"{col}=COALESCE({entity}.{col},EXCLUDED.{col})")
            elif mode == "prefer_longer":
                parts.append(
                    f"{col}=CASE WHEN length(EXCLUDED.{col})>length({entity}.{col}) "
                    f"THEN EXCLUDED.{col} ELSE {entity}.{col} END"
                )
    parts.append(f"source_name='{source}'")
    parts.append(f"ingested_at='{timestamp}'")
    return ", ".join(parts)


def col_or_null(entity: str, col: str, prefix: str = "src") -> str:
    """Return column reference if in CSV, otherwise NULL"""
    if col in CSV_COLUMNS[entity]:
        return f"{prefix}.{col}"
    return "NULL"


# Define all possible columns for each entity (for the database tables)
ALL_COLUMNS = {
    "artists": {"spotify_uri": "text", "mbid": "text", "name": "citext"},
    "albums": {
        "spotify_uri": "text",
        "mbid": "text",
        "name": "citext",
        "album_type": "text",
        "spotify_release_date": "date",
        "release_date_precision": "text",
        "n_tracks": "int",
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
    },
}

# Handle list fields as text in staging (will be parsed later)
STAGING_OVERRIDES = {"artist_spotify_uris": "text", "genres": "text"}

# Policy overrides - only specify what's different from defaults
# For MPD, we use defaults for everything, so this is empty!
POLICY = {}


# Helper functions specific to this loader


def artist_merge(source: str, timestamp: str) -> str:
    """Generate merge SQL for artists"""
    # Only update columns that exist in CSV
    updatable_cols = [
        c for c in ["name", "mbid", "spotify_uri"] if c in CSV_COLUMNS["artists"]
    ]
    upd = build_set("artists", updatable_cols, source, timestamp)

    return f"""
WITH src AS (
    SELECT DISTINCT ON (s.spotify_uri) 
           s.spotify_uri,
           s.name,
           '{source}'::text AS source_name,
           '{timestamp}'::timestamptz AS ingested_at
    FROM staging_artists s
    WHERE s.spotify_uri IS NOT NULL
)
INSERT INTO artists (spotify_uri, name, source_name, ingested_at)
SELECT src.spotify_uri, src.name, src.source_name, src.ingested_at
FROM src
ON CONFLICT (spotify_uri) DO UPDATE SET {upd}
RETURNING TRUE AS updated;
"""


def album_merge(source: str, timestamp: str) -> str:
    """Generate merge SQL for albums with artist association handling"""
    # Only update columns that exist in CSV
    updatable_cols = [
        c
        for c in [
            "name",
            "album_type",
            "spotify_release_date",
            "release_date_precision",
            "n_tracks",
            "mbid",
            "spotify_uri",
        ]
        if c in CSV_COLUMNS["albums"]
    ]
    upd = build_set("albums", updatable_cols, source, timestamp)

    # Build insert column list dynamically
    insert_cols = ["spotify_uri", "mbid", "name", "source_name", "ingested_at"]
    insert_vals = [
        col_or_null("albums", "spotify_uri"),
        col_or_null("albums", "mbid") + "::uuid",
        col_or_null("albums", "name"),
        "src.source_name",
        "src.ingested_at",
    ]

    # Add optional columns if they exist in CSV
    optional_cols = [
        "album_type",
        "spotify_release_date",
        "release_date_precision",
        "n_tracks",
    ]
    for col in optional_cols:
        if col in CSV_COLUMNS["albums"]:
            insert_cols.append(col)
            insert_vals.append(col_or_null("albums", col))

    return f"""
WITH src AS (
    SELECT *, '{source}'::text AS source_name,
           '{timestamp}'::timestamptz AS ingested_at
    FROM staging_albums
),
up AS (
    INSERT INTO albums ({', '.join(insert_cols)})
    SELECT {', '.join(insert_vals)}
    FROM src
    ON CONFLICT (spotify_uri) DO UPDATE SET {upd}
    RETURNING id, spotify_uri, TRUE AS updated
)
-- Link albums to artists from artist list (if artist_spotify_uris column exists)
INSERT INTO album_artists (album_id, artist_id, position)
SELECT DISTINCT up.id, a.id, 
       -- Use array position as the artist position
       array_position(
           string_to_array(
               trim(both '[]' from replace(replace(s.artist_spotify_uris, '''', ''), '"', '')), 
               ','
           ),
           trim(a.spotify_uri)
       ) - 1 as position  -- 0-based position
FROM staging_albums s
JOIN up ON up.spotify_uri = s.spotify_uri
JOIN artists a ON a.spotify_uri = ANY(
    string_to_array(
        trim(both '[]' from replace(replace(s.artist_spotify_uris, '''', ''), '"', '')), 
        ','
    )
)
WHERE s.artist_spotify_uris != '[]' AND s.artist_spotify_uris IS NOT NULL
  AND EXISTS (SELECT 1 FROM information_schema.columns 
              WHERE table_name = 'staging_albums' AND column_name = 'artist_spotify_uris')
ON CONFLICT (album_id, artist_id) DO UPDATE SET position = EXCLUDED.position;
"""


def track_merge(source: str, timestamp: str) -> str:
    """Generate merge SQL for tracks with artist association handling"""
    # Only update columns that exist in CSV
    updatable_cols = [
        c
        for c in [
            "name",
            "duration_ms",
            "explicit",
            "disc_number",
            "track_number",
            "album_id",
            "mbid",
            "spotify_uri",
        ]
        if c in CSV_COLUMNS["tracks"]
    ]
    upd = build_set("tracks", updatable_cols, source, timestamp)

    # Build insert column list dynamically
    insert_cols = ["spotify_uri", "mbid", "name", "source_name", "ingested_at"]
    insert_vals = [
        col_or_null("tracks", "spotify_uri"),
        col_or_null("tracks", "mbid") + "::uuid",
        col_or_null("tracks", "name"),
        "src.source_name",
        "src.ingested_at",
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
    SELECT *, '{source}'::text AS source_name,
           '{timestamp}'::timestamptz AS ingested_at
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
-- Link tracks to artists from artist list (if artist_spotify_uris column exists)
INSERT INTO track_artists (track_id, artist_id, position)
SELECT DISTINCT up.id, a.id,
       -- Use array position as the artist position
       array_position(
           string_to_array(
               trim(both '[]' from replace(replace(s.artist_spotify_uris, '''', ''), '"', '')), 
               ','
           ),
           trim(a.spotify_uri)
       ) - 1 as position  -- 0-based position
FROM staging_tracks s
JOIN up ON up.spotify_uri = s.spotify_uri
JOIN artists a ON a.spotify_uri = ANY(
    string_to_array(
        trim(both '[]' from replace(replace(s.artist_spotify_uris, '''', ''), '"', '')), 
        ','
    )
)
WHERE s.artist_spotify_uris != '[]' AND s.artist_spotify_uris IS NOT NULL
  AND EXISTS (SELECT 1 FROM information_schema.columns 
              WHERE table_name = 'staging_tracks' AND column_name = 'artist_spotify_uris')
ON CONFLICT (track_id, artist_id) DO UPDATE SET position = EXCLUDED.position;
"""


# Configuration mapping
SETTINGS = {
    "artists": {
        "staging": "staging_artists",
        "columns": CSV_COLUMNS["artists"],
        "merge_func": artist_merge,
    },
    "albums": {
        "staging": "staging_albums",
        "columns": CSV_COLUMNS["albums"],
        "merge_func": album_merge,
    },
    "tracks": {
        "staging": "staging_tracks",
        "columns": CSV_COLUMNS["tracks"],
        "merge_func": track_merge,
    },
}
