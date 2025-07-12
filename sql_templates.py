"""
sql_templates.py - Reusable SQL generation for all loaders

Contains all the generic SQL patterns that can be shared across different
data source loaders (MPD, Last.fm, MusicBrainz, etc.)
"""

from load_csv_engine import get_policy, build_set, col_or_null


def generate_entity_upsert(entity: str, csv_columns: dict, policy: dict, source: str, timestamp: str) -> str:
    """Generate basic upsert SQL for any entity (artists, albums, tracks)"""
    # Determine which columns from the schema exist in this CSV
    updatable_cols = [c for c in ["name", "mbid", "spotify_uri"] if c in csv_columns[entity]]
    
    # Add entity-specific optional columns if they exist
    optional_cols = {
        "albums": ["album_type", "spotify_release_date", "release_date_precision", "n_tracks"],
        "tracks": ["duration_ms", "explicit", "disc_number", "track_number"]
    }
    
    if entity in optional_cols:
        updatable_cols.extend([c for c in optional_cols[entity] if c in csv_columns[entity]])
    
    upd = build_set(entity, updatable_cols, source, timestamp, csv_columns, policy)
    
    # Build column lists dynamically
    insert_cols = ["spotify_uri", "name", "source_name", "ingested_at"]
    insert_vals = ["s.spotify_uri", "s.name", f"'{source}'", f"'{timestamp}'::timestamptz"]
    
    # Add optional columns that exist in CSV
    for col in updatable_cols:
        if col not in ["spotify_uri", "name"]:  # Skip already added core columns
            if col in csv_columns[entity]:
                insert_cols.append(col)
                insert_vals.append(f"s.{col}")
    
    # Special handling for tracks (need album_id)
    if entity == "tracks":
        insert_cols.append("album_id")
        insert_vals.append("al.id")
        from_clause = """
FROM staging_tracks s
LEFT JOIN albums al ON al.spotify_uri = s.album_spotify_uri"""
    else:
        from_clause = f"\nFROM staging_{entity} s"
    
    return f"""
INSERT INTO {entity} ({', '.join(insert_cols)})
SELECT DISTINCT {', '.join(insert_vals)}{from_clause}
WHERE s.spotify_uri IS NOT NULL
ON CONFLICT (spotify_uri) DO UPDATE SET {upd}
"""


def generate_missing_artists_sql(entity: str, csv_columns: dict, source: str, timestamp: str) -> str:
    """Generate SQL to create missing artists referenced in associations"""
    if entity not in ["albums", "tracks"]:
        return ""
    
    # Check if this entity has artist associations in the CSV
    if "artist_spotify_uris" not in csv_columns[entity]:
        return ""
    
    return f"""
INSERT INTO artists (spotify_uri, name, source_name, ingested_at)
SELECT DISTINCT 
    artist_pos.artist_uri as spotify_uri,
    NULL as name,  -- NULL name, will be populated later
    '{source}' as source_name,
    '{timestamp}'::timestamptz as ingested_at
FROM staging_{entity} s
CROSS JOIN LATERAL (
    SELECT unnest(string_to_array(replace(s.artist_spotify_uris, '"', ''), ',')) as artist_uri
) as artist_pos
LEFT JOIN artists existing ON existing.spotify_uri = artist_pos.artist_uri
WHERE s.artist_spotify_uris IS NOT NULL 
  AND s.artist_spotify_uris != ''
  AND artist_pos.artist_uri IS NOT NULL
  AND artist_pos.artist_uri != ''
  AND existing.spotify_uri IS NULL
ON CONFLICT (spotify_uri) DO NOTHING
"""


def generate_missing_albums_sql(entity: str, csv_columns: dict, source: str, timestamp: str) -> str:
    """Generate SQL to create missing albums referenced by tracks"""
    if entity != "tracks":
        return ""
    
    # Check if tracks reference albums
    if "album_spotify_uri" not in csv_columns[entity]:
        return ""
    
    return f"""
INSERT INTO albums (spotify_uri, name, source_name, ingested_at)
SELECT DISTINCT 
    s.album_spotify_uri as spotify_uri,
    NULL as name,  -- NULL name, will be populated later
    '{source}' as source_name,
    '{timestamp}'::timestamptz as ingested_at
FROM staging_{entity} s
LEFT JOIN albums existing ON existing.spotify_uri = s.album_spotify_uri
WHERE s.album_spotify_uri IS NOT NULL 
  AND s.album_spotify_uri != ''
  AND existing.spotify_uri IS NULL
ON CONFLICT (spotify_uri) DO NOTHING
"""


def generate_association_sql(entity: str, csv_columns: dict, policy: dict = None) -> str:
    """Generate association table SQL for linking entities to artists or genres"""
    
    # Handle artist associations for albums/tracks
    if entity in ["albums", "tracks"] and "artist_spotify_uris" in csv_columns[entity]:
        return _generate_artist_associations(entity, csv_columns, policy)
    
    # Handle genre associations for artists
    if entity == "artists" and "genres" in csv_columns[entity]:
        return _generate_genre_associations(entity, csv_columns, policy)
    
    return ""


def _generate_artist_associations(entity: str, csv_columns: dict, policy: dict = None) -> str:
    """Generate artist association SQL for albums/tracks"""
    
    association_table = f"{entity[:-1]}_artists"  # albums -> album_artists, tracks -> track_artists
    entity_singular = entity[:-1]  # albums -> album, tracks -> track
    
    # Check if policy exists for artist associations
    if policy is None:
        policy = {}
    
    if "artist-associations" not in policy:
        raise ValueError(f"Association data found for {entity} but no 'artist-associations' policy defined. Define a policy or remove association data from CSV.")
    
    if entity not in policy["artist-associations"]:
        raise ValueError(f"Association data found for {entity} but no policy defined for {entity} in 'artist-associations'. Define a policy for {entity} or remove association data from CSV.")
    
    association_policy = policy["artist-associations"][entity]
    
    # Generate SQL based on policy
    if association_policy == 'replace':
        # Replace: Delete all existing associations and insert new ones from CSV
        return f"""
-- Delete all existing associations for this entity
DELETE FROM {association_table} 
WHERE {entity_singular}_id IN (
    SELECT e.id FROM staging_{entity} s
    JOIN {entity} e ON e.spotify_uri = s.spotify_uri
    WHERE s.artist_spotify_uris IS NOT NULL AND s.artist_spotify_uris != ''
);

-- Insert new associations from CSV
INSERT INTO {association_table} ({entity_singular}_id, artist_id, position)
SELECT DISTINCT 
    e.id,
    ar.id,
    artist_pos.pos - 1 as position
FROM staging_{entity} s
JOIN {entity} e ON e.spotify_uri = s.spotify_uri  
CROSS JOIN LATERAL (
    SELECT unnest(string_to_array(replace(s.artist_spotify_uris, '"', ''), ',')) as artist_uri,
           generate_series(1, array_length(string_to_array(replace(s.artist_spotify_uris, '"', ''), ','), 1)) as pos
) as artist_pos
JOIN artists ar ON ar.spotify_uri = artist_pos.artist_uri
WHERE s.artist_spotify_uris IS NOT NULL 
  AND s.artist_spotify_uris != ''
"""
    elif association_policy == 'extend':
        # Extend: Keep existing associations, only add new ones
        return f"""
INSERT INTO {association_table} ({entity_singular}_id, artist_id, position)
SELECT DISTINCT 
    e.id,
    ar.id,
    COALESCE(max_pos.max_position, -1) + 
    ROW_NUMBER() OVER (PARTITION BY e.id ORDER BY artist_pos.pos) as position
FROM staging_{entity} s
JOIN {entity} e ON e.spotify_uri = s.spotify_uri  
CROSS JOIN LATERAL (
    SELECT unnest(string_to_array(replace(s.artist_spotify_uris, '"', ''), ',')) as artist_uri,
           generate_series(1, array_length(string_to_array(replace(s.artist_spotify_uris, '"', ''), ','), 1)) as pos
) as artist_pos
JOIN artists ar ON ar.spotify_uri = artist_pos.artist_uri
LEFT JOIN {association_table} existing ON existing.{entity_singular}_id = e.id AND existing.artist_id = ar.id
LEFT JOIN (
    SELECT {entity_singular}_id, MAX(position) as max_position 
    FROM {association_table} 
    GROUP BY {entity_singular}_id
) max_pos ON max_pos.{entity_singular}_id = e.id
WHERE s.artist_spotify_uris IS NOT NULL 
  AND s.artist_spotify_uris != ''
  AND existing.{entity_singular}_id IS NULL  -- Only add new associations
"""
    else:
        raise ValueError(f"Unknown association policy: {association_policy} for entity {entity}. Use 'replace' or 'extend'.")


def generate_merge_function(entity: str, csv_columns: dict, policy: dict):
    """Generate complete merge function for any entity"""
    def merge_func(source: str, timestamp: str) -> list[str]:
        sql_statements = []
        
        # Step 1: Create missing albums (if tracks reference them)
        missing_albums_sql = generate_missing_albums_sql(entity, csv_columns, source, timestamp)
        if missing_albums_sql:
            sql_statements.append(missing_albums_sql)
        
        # Step 2: Create missing artists (if applicable)
        missing_artists_sql = generate_missing_artists_sql(entity, csv_columns, source, timestamp)
        if missing_artists_sql:
            sql_statements.append(missing_artists_sql)
        
        # Step 3: Upsert the main entity
        sql_statements.append(generate_entity_upsert(entity, csv_columns, policy, source, timestamp))
        
        # Step 4: Handle associations (if applicable)
        association_sql = generate_association_sql(entity, csv_columns, policy)
        if association_sql:
            sql_statements.append(association_sql)
        
        
        return sql_statements
    
    return merge_func


def _generate_genre_associations(entity: str, csv_columns: dict, policy: dict = None) -> str:
    """Generate genre association SQL for artists"""
    
    association_table = "artist_genres"
    
    # Check if policy exists for genre associations
    if policy is None:
        policy = {}
    
    if "genre-associations" not in policy:
        raise ValueError(f"Genre data found for {entity} but no 'genre-associations' policy defined. Define a policy or remove genre data from CSV.")
    
    if entity not in policy["genre-associations"]:
        raise ValueError(f"Genre data found for {entity} but no policy defined for {entity} in 'genre-associations'. Define a policy for {entity} or remove genre data from CSV.")
    
    association_policy = policy["genre-associations"][entity]
    
    # Generate SQL based on policy
    if association_policy == 'replace':
        # Replace: Delete all existing genre associations and insert new ones from CSV
        return f"""
-- Delete existing genre associations for artists being updated
DELETE FROM {association_table} 
WHERE artist_id IN (
    SELECT a.id FROM staging_artists s
    JOIN artists a ON a.spotify_uri = s.spotify_uri
    WHERE s.genres IS NOT NULL AND s.genres != ''
);

-- Insert new genre associations from CSV
INSERT INTO {association_table} (artist_id, genre_id)
SELECT DISTINCT 
    a.id,
    g.id
FROM staging_artists s
JOIN artists a ON a.spotify_uri = s.spotify_uri  
CROSS JOIN LATERAL (
    SELECT unnest(string_to_array(s.genres, ',')) as genre_name
) as genre_pos
JOIN genres g ON TRIM(g.name) = TRIM(genre_pos.genre_name)
WHERE s.genres IS NOT NULL 
  AND s.genres != ''
"""
    elif association_policy == 'extend':
        # Extend: Keep existing associations, only add new ones
        return f"""
INSERT INTO {association_table} (artist_id, genre_id)
SELECT DISTINCT 
    a.id,
    g.id
FROM staging_artists s
JOIN artists a ON a.spotify_uri = s.spotify_uri  
CROSS JOIN LATERAL (
    SELECT unnest(string_to_array(s.genres, ',')) as genre_name
) as genre_pos
JOIN genres g ON TRIM(g.name) = TRIM(genre_pos.genre_name)
LEFT JOIN {association_table} existing ON existing.artist_id = a.id AND existing.genre_id = g.id
WHERE s.genres IS NOT NULL 
  AND s.genres != ''
  AND existing.artist_id IS NULL  -- Only add new associations
"""
    else:
        raise ValueError(f"Unknown association policy: {association_policy} for entity {entity}. Use 'replace' or 'extend'.")