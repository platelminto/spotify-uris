"""
association_changes.py - Association table change analysis

Analyzes what association changes will happen by comparing staging vs pre-merge association tables.
"""

from typing import Dict, List, Any


def analyze_association_changes(conn, entity: str, csv_columns: Dict[str, List[str]], policy: Dict = None) -> List[Dict[str, Any]]:
    """Analyze association table changes using pre-merge comparison"""
    if entity not in ["albums", "tracks"]:
        return []
    
    # Check if artist_spotify_uris column exists in CSV - if not, no association changes
    if "artist_spotify_uris" not in csv_columns.get(entity, []):
        return []
    
    assoc_table = f"{entity[:-1]}_artists"
    entity_singular = entity[:-1]
    
    # Get current associations for entities in staging (before merge)
    current_assocs_query = f"""
    SELECT DISTINCT e.spotify_uri, ar.spotify_uri as artist_uri
    FROM {assoc_table} ta
    JOIN {entity} e ON e.id = ta.{entity_singular}_id
    JOIN artists ar ON ar.id = ta.artist_id
    WHERE e.spotify_uri IN (SELECT spotify_uri FROM staging_{entity})
    """
    
    # Get new associations from staging data
    new_assocs_query = f"""
    SELECT DISTINCT s.spotify_uri, artist_pos.artist_uri
    FROM staging_{entity} s
    CROSS JOIN LATERAL (
        SELECT unnest(string_to_array(replace(s.artist_spotify_uris, '"', ''), ',')) as artist_uri
    ) as artist_pos
    WHERE s.artist_spotify_uris IS NOT NULL 
      AND s.artist_spotify_uris != ''
      AND artist_pos.artist_uri IS NOT NULL
      AND artist_pos.artist_uri != ''
    """
    
    try:
        current_assocs = set()
        for row in conn.execute(current_assocs_query).fetchall():
            current_assocs.add((row[0], row[1]))
        
        new_assocs = set()
        for row in conn.execute(new_assocs_query).fetchall():
            new_assocs.add((row[0], row[1]))
        
        # Calculate differences based on policy
        artist_policy = policy.get(entity, {}).get('artists', 'prefer_incoming') if policy else 'prefer_incoming'
        
        if artist_policy == 'extend':
            # For extend policy: keep all current, add new ones that don't exist
            to_delete = set()  # Never delete anything
            to_insert = new_assocs - current_assocs  # Only truly new associations
            recreated = current_assocs & new_assocs  # Associations that already exist
        elif artist_policy == 'prefer_non_null':
            # For prefer_non_null: only add if current is empty/null
            if current_assocs:
                # Already has associations, ignore staging data
                to_delete = set()
                to_insert = set()
                recreated = current_assocs  # Keep existing unchanged
            else:
                # No existing associations, add new ones
                to_delete = set()
                to_insert = new_assocs
                recreated = set()
        else:
            # For prefer_incoming: replace all
            to_delete = current_assocs - new_assocs
            to_insert = new_assocs - current_assocs
            recreated = current_assocs & new_assocs
        
        # Total current associations in the table (before any changes)
        total_current = conn.execute(f"SELECT COUNT(*) FROM {assoc_table}").fetchone()[0]
        
        return [{
            'table_name': assoc_table,
            'current_associations': total_current,
            'potential_associations': 0,
            'new_associations': len(to_insert),
            'recreated_associations': len(recreated),
            'deleted_associations': len(to_delete),
            'entities_with_changes': len(set(pair[0] for pair in (to_delete | to_insert)))
        }]
        
    except Exception as e:
        print(f"[DEBUG] Association analysis failed: {e}")
        return []