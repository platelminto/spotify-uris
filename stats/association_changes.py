"""
association_changes.py - Association table change analysis

Analyzes what association changes will happen by comparing staging vs pre-merge association tables.
"""

from typing import Dict, List, Any


def analyze_association_changes_with_comparison(conn, entity: str, csv_columns: Dict[str, List[str]], policy: Dict = None) -> tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """Analyze association changes and return both current policy results and policy comparison"""
    
    # Calculate comparison for all three policies
    comparison = {}
    current_changes = []
    
    if entity in ["albums", "tracks"] and "artist_spotify_uris" in csv_columns.get(entity, []):
        # Get current policy type - fail if not found
        if not policy or entity not in policy or 'artists' not in policy[entity]:
            raise ValueError(f"Policy for {entity}.artists not found in policy: {policy}")
        
        current_policy_type = policy[entity]['artists']
        
        for policy_type in ['extend', 'prefer_incoming', 'prefer_non_null']:
            # Create temporary policy for comparison
            temp_policy = {entity: {'artists': policy_type}}
            result = analyze_association_changes(conn, entity, csv_columns, temp_policy)
            if result:
                comparison[policy_type] = result[0]  # analyze_association_changes returns a list
                comparison[policy_type]['policy_type'] = policy_type
                # Add net_change calculation
                comparison[policy_type]['net_change'] = (
                    comparison[policy_type]['new_associations'] - 
                    comparison[policy_type]['deleted_associations']
                )
                
                # Extract current policy results
                if policy_type == current_policy_type:
                    current_changes = result
    
    # Add change distribution analysis for current policy
    change_distribution = {}
    if current_changes:
        change_distribution = _analyze_artist_change_distribution(conn, entity, csv_columns, policy)
    
    return current_changes, comparison, change_distribution


def _analyze_artist_change_distribution(conn, entity: str, csv_columns: Dict[str, List[str]], policy: Dict = None) -> Dict[str, Any]:
    """Analyze how many artists each entity gains/loses/keeps"""
    if entity not in ["albums", "tracks"] or "artist_spotify_uris" not in csv_columns.get(entity, []):
        return {}
    
    entity_singular = entity[:-1]
    artist_policy = policy.get(entity, {}).get('artists', 'prefer_incoming') if policy else 'prefer_incoming'
    
    # Query to get current and new artist counts per entity
    query = f"""
    WITH current_counts AS (
        SELECT e.spotify_uri, COUNT(ta.artist_id) as current_count
        FROM {entity} e
        LEFT JOIN {entity_singular}_artists ta ON e.id = ta.{entity_singular}_id
        WHERE e.spotify_uri IN (SELECT spotify_uri FROM staging_{entity})
        GROUP BY e.spotify_uri
    ),
    new_counts AS (
        SELECT s.spotify_uri, COUNT(DISTINCT artist_pos.artist_uri) as new_count
        FROM staging_{entity} s
        CROSS JOIN LATERAL (
            SELECT unnest(string_to_array(trim(both '{{}}' from s.artist_spotify_uris), ',')) as artist_uri
        ) as artist_pos
        WHERE s.artist_spotify_uris IS NOT NULL 
          AND s.artist_spotify_uris != ''
          AND artist_pos.artist_uri IS NOT NULL
          AND artist_pos.artist_uri != ''
        GROUP BY s.spotify_uri
    ),
    effective_counts AS (
        SELECT 
            COALESCE(cc.spotify_uri, nc.spotify_uri) as spotify_uri,
            COALESCE(cc.current_count, 0) as current_count,
            COALESCE(nc.new_count, 0) as new_count,
            CASE 
                WHEN '{artist_policy}' = 'prefer_non_null' AND COALESCE(cc.current_count, 0) > 0 
                THEN COALESCE(cc.current_count, 0)  -- Keep current if exists
                WHEN '{artist_policy}' = 'extend' 
                THEN GREATEST(COALESCE(cc.current_count, 0), COALESCE(nc.new_count, 0))  -- Take max
                ELSE COALESCE(nc.new_count, 0)  -- prefer_incoming: use new
            END as effective_count
        FROM current_counts cc
        FULL OUTER JOIN new_counts nc ON cc.spotify_uri = nc.spotify_uri
    )
    SELECT 
        CASE 
            WHEN effective_count > current_count THEN 'gaining'
            WHEN effective_count < current_count THEN 'losing'
            ELSE 'same'
        END as change_type,
        effective_count - current_count as net_change,
        COUNT(*) as entity_count
    FROM effective_counts
    GROUP BY change_type, net_change
    ORDER BY change_type, net_change
    """
    
    try:
        results = conn.execute(query).fetchall()
        
        distribution = {
            'gaining': {},  # {1: 1234, 2: 567, 3: 89} = 1234 entities gained 1 artist, etc
            'losing': {},   # {-1: 234, -2: 56} = 234 entities lost 1 artist, etc  
            'same': 0       # count of entities with no change
        }
        
        for row in results:
            change_type, net_change, entity_count = row
            if change_type == 'gaining' and net_change > 0:
                distribution['gaining'][net_change] = entity_count
            elif change_type == 'losing' and net_change < 0:
                distribution['losing'][abs(net_change)] = entity_count
            elif change_type == 'same':
                distribution['same'] = entity_count
        
        return distribution
        
    except Exception as e:
        print(f"[DEBUG] Artist change distribution analysis failed: {e}")
        return {}


def analyze_association_changes(conn, entity: str, csv_columns: Dict[str, List[str]], policy: Dict = None) -> List[Dict[str, Any]]:
    """Analyze association table changes using pre-merge comparison"""
    import time
    
    if entity not in ["albums", "tracks"]:
        return []
    
    # Check if artist_spotify_uris column exists in CSV - if not, no association changes
    if "artist_spotify_uris" not in csv_columns.get(entity, []):
        return []
    
    assoc_table = f"{entity[:-1]}_artists"
    entity_singular = entity[:-1]
    
    # Get current associations for entities in staging (before merge)
    # Optimized: start with staging data and join outward
    current_assocs_query = f"""
    SELECT DISTINCT s.spotify_uri, ar.spotify_uri as artist_uri
    FROM staging_{entity} s
    JOIN {entity} e ON e.spotify_uri = s.spotify_uri  
    JOIN {assoc_table} ta ON ta.{entity_singular}_id = e.id
    JOIN artists ar ON ar.id = ta.artist_id
    """
    
    # Get new associations from staging data
    new_assocs_query = f"""
    SELECT DISTINCT s.spotify_uri, artist_pos.artist_uri
    FROM staging_{entity} s
    CROSS JOIN LATERAL (
        SELECT unnest(string_to_array(trim(both '{{}}' from s.artist_spotify_uris), ',')) as artist_uri
    ) as artist_pos
    WHERE s.artist_spotify_uris IS NOT NULL 
      AND s.artist_spotify_uris != ''
      AND artist_pos.artist_uri IS NOT NULL
      AND artist_pos.artist_uri != ''
    """
    
    try:
        # Execute current associations query
        current_assocs = set()
        for row in conn.execute(current_assocs_query).fetchall():
            current_assocs.add((row[0], row[1]))
        
        # Execute new associations query
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
        raise e