"""
sql_templates.py - Reusable SQL generation for all loaders

Contains all the generic SQL patterns that can be shared across different
data source loaders (MPD, Last.fm, MusicBrainz, etc.)
"""


def get_policy(entity, csv_columns, config_policy):
    """Get policy for entity from config"""
    if not config_policy or entity not in config_policy:
        raise ValueError(f"No policy defined for entity: {entity}")

    # Only keep policies for columns that exist in this CSV
    entity_csv_columns = csv_columns.get(entity, [])
    entity_policy = config_policy[entity]
    return {
        col: policy
        for col, policy in entity_policy.items()
        if col in entity_csv_columns
    }

def build_set(
    entity: str,
    cols: list[str],
    source: str,
    timestamp: str,
    csv_columns: dict,
    config_policy: dict,
) -> str:
    """Generate SET clause obeying policy."""
    policy = get_policy(entity, csv_columns, config_policy)
    parts = []
    csv_cols = csv_columns[entity]
    for col in cols:
        if col in csv_cols:  # Only if column exists in CSV
            mode = policy.get(col, "prefer_incoming")
            if mode == "prefer_incoming":
                parts.append(f"{col}=EXCLUDED.{col}")
            elif mode == "prefer_non_null":
                parts.append(f"{col}=CASE WHEN {entity}.{col} IS NOT NULL THEN {entity}.{col} ELSE EXCLUDED.{col} END")
            elif mode == "prefer_longer":
                parts.append(
                    f"{col}=CASE WHEN length(EXCLUDED.{col})>length({entity}.{col}) "
                    f"THEN EXCLUDED.{col} ELSE {entity}.{col} END"
                )
            elif mode == "extend" and col == "genres":
                # Special handling for genres array - extend means merge arrays
                parts.append(f"{col}=COALESCE({entity}.{col}, ARRAY[]::text[]) || COALESCE(EXCLUDED.{col}, ARRAY[]::text[])")
    
    # Special handling for tracks: if album_spotify_uri exists in CSV, update album_id
    if entity == "tracks" and "album_spotify_uri" in csv_cols:
        album_policy = policy.get("album_spotify_uri", "prefer_incoming")
        if album_policy == "prefer_incoming":
            parts.append("album_id=EXCLUDED.album_id")
        elif album_policy == "prefer_non_null":
            parts.append("album_id=CASE WHEN tracks.album_id IS NOT NULL THEN tracks.album_id ELSE EXCLUDED.album_id END")
    
    parts.append(f"source_name='{source}'")
    parts.append(f"ingested_at='{timestamp}'")
    return ", ".join(parts)


def generate_entity_upsert(entity: str, csv_columns: dict, policy: dict, source: str, timestamp: str) -> str:
    """Generate basic upsert SQL for any entity (artists, albums, tracks)"""
    # Determine which columns from the schema exist in this CSV
    updatable_cols = [c for c in ["name", "mbid", "spotify_uri"] if c in csv_columns[entity]]
    
    # Add entity-specific optional columns if they exist
    optional_cols = {
        "artists": ["genres"],
        "albums": ["album_type", "spotify_release_date", "release_date_precision", "n_tracks"],
        "tracks": ["duration_ms", "explicit", "disc_number", "track_number"]
    }
    
    if entity in optional_cols:
        updatable_cols.extend([c for c in optional_cols[entity] if c in csv_columns[entity]])
    
    upd = build_set(entity, updatable_cols, source, timestamp, csv_columns, policy)
    
    # Build column lists dynamically - start with required metadata
    insert_cols = ["source_name", "ingested_at"]
    insert_vals = [f"'{source}'", f"'{timestamp}'::timestamptz"]
    
    # Add ID columns that exist in CSV (at least one of spotify_uri or mbid must exist)
    if "spotify_uri" in csv_columns[entity]:
        insert_cols.insert(0, "spotify_uri")
        insert_vals.insert(0, "s.spotify_uri")
    if "mbid" in csv_columns[entity]:
        insert_cols.insert(-2, "mbid")  # Insert before source_name
        insert_vals.insert(-2, "s.mbid")
    
    # Add name column only if it exists in CSV
    if "name" in csv_columns[entity]:
        insert_cols.insert(-2, "name")  # Insert before source_name
        insert_vals.insert(-2, "s.name")
    
    # Add optional columns that exist in CSV
    for col in updatable_cols:
        if col not in ["spotify_uri", "mbid", "name"]:  # Skip already added core columns
            if col in csv_columns[entity]:
                insert_cols.append(col)
                insert_vals.append(f"s.{col}")
    
    # Special handling for tracks (need album_id)
    if entity == "tracks":
        # Only add album_id if we have album_spotify_uri in CSV
        if "album_spotify_uri" in csv_columns[entity]:
            insert_cols.append("album_id")
            insert_vals.append("al.id")
            from_clause = """
FROM staging_tracks s
LEFT JOIN albums al ON al.spotify_uri = s.album_spotify_uri"""
        else:
            from_clause = f"\nFROM staging_{entity} s"
    else:
        from_clause = f"\nFROM staging_{entity} s"
    
    # Build WHERE clause based on available ID columns
    where_conditions = []
    if "spotify_uri" in csv_columns[entity]:
        where_conditions.append("s.spotify_uri IS NOT NULL")
    if "mbid" in csv_columns[entity]:
        where_conditions.append("s.mbid IS NOT NULL")
    where_clause = f"WHERE ({' OR '.join(where_conditions)})" if where_conditions else ""
    
    # Build ON CONFLICT clause based on available ID columns
    if "spotify_uri" in csv_columns[entity]:
        conflict_clause = f"ON CONFLICT (spotify_uri) DO UPDATE SET {upd}"
    elif "mbid" in csv_columns[entity]:
        conflict_clause = f"ON CONFLICT (mbid) DO UPDATE SET {upd}"
    else:
        raise ValueError(f"No ID columns (spotify_uri or mbid) found for entity {entity}")
    
    return f"""
INSERT INTO {entity} ({', '.join(insert_cols)})
SELECT DISTINCT {', '.join(insert_vals)}{from_clause}
{where_clause}
{conflict_clause}
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
    SELECT unnest(string_to_array(trim(both '{{}}' from s.artist_spotify_uris), ',')) as artist_uri
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



def generate_association_sql(entity: str, csv_columns: dict, policy: dict | None = None) -> str:
    """Generate association table SQL for linking entities to artists"""
    # Handle artist associations for albums/tracks
    if entity in ["albums", "tracks"] and "artist_spotify_uris" in csv_columns[entity]:
        return _generate_artist_associations(entity, csv_columns, policy)
    
    return ""


def _generate_artist_associations(entity: str, csv_columns: dict, policy: dict | None = None) -> str:
    """Generate artist association SQL for albums/tracks"""
    
    association_table = f"{entity[:-1]}_artists"  # albums -> album_artists, tracks -> track_artists
    entity_singular = entity[:-1]  # albums -> album, tracks -> track
    
    # Check if policy exists for artist associations
    if policy is None:
        policy = {}
    
    if entity not in policy:
        raise ValueError(f"Association data found for {entity} but no policy defined for {entity}. Define a policy or remove association data from CSV.")
    
    if "artists" not in policy[entity]:
        raise ValueError(f"Association data found for {entity} but no 'artists' policy defined for {entity}. Define a policy for {entity}.artists or remove association data from CSV.")
    
    association_policy = policy[entity]["artists"]
    
    # Generate SQL based on policy
    if association_policy == 'prefer_incoming':
        # prefer_incoming: Delete all existing associations and insert new ones from CSV
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
    SELECT unnest(string_to_array(trim(both '{{}}' from s.artist_spotify_uris), ',')) as artist_uri,
           generate_series(1, array_length(string_to_array(trim(both '{{}}' from s.artist_spotify_uris), ','), 1)) as pos
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
    SELECT unnest(string_to_array(trim(both '{{}}' from s.artist_spotify_uris), ',')) as artist_uri,
           generate_series(1, array_length(string_to_array(trim(both '{{}}' from s.artist_spotify_uris), ','), 1)) as pos
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
    elif association_policy == 'prefer_non_null':
        # prefer_non_null: Only add associations if the entity has no existing associations
        return f"""
INSERT INTO {association_table} ({entity_singular}_id, artist_id, position)
SELECT DISTINCT 
    e.id,
    ar.id,
    artist_pos.pos - 1 as position
FROM staging_{entity} s
JOIN {entity} e ON e.spotify_uri = s.spotify_uri  
CROSS JOIN LATERAL (
    SELECT unnest(string_to_array(trim(both '{{}}' from s.artist_spotify_uris), ',')) as artist_uri,
           generate_series(1, array_length(string_to_array(trim(both '{{}}' from s.artist_spotify_uris), ','), 1)) as pos
) as artist_pos
JOIN artists ar ON ar.spotify_uri = artist_pos.artist_uri
WHERE s.artist_spotify_uris IS NOT NULL 
  AND s.artist_spotify_uris != ''
  AND NOT EXISTS (
    SELECT 1 FROM {association_table} existing 
    WHERE existing.{entity_singular}_id = e.id
  )  -- Only add if no existing associations
"""
    else:
        raise ValueError(f"Unknown association policy: {association_policy} for entity {entity}. Use 'prefer_incoming', 'extend', or 'prefer_non_null'.")


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
        
        
        # Step 4: Upsert the main entity
        sql_statements.append(generate_entity_upsert(entity, csv_columns, policy, source, timestamp))
        
        # Step 5: Handle associations (if applicable)
        association_sql = generate_association_sql(entity, csv_columns, policy)
        if association_sql:
            sql_statements.append(association_sql)
        
        
        return sql_statements
    
    return merge_func


