"""
column_changes.py - Column-level change analysis

Analyzes what column values will change by comparing staging vs pre-merge main table.
"""

from typing import Dict, List


def analyze_column_changes_with_comparison(conn, entity: str, csv_columns: Dict[str, List[str]], policy: Dict) -> tuple[Dict[str, int], Dict[str, Dict[str, int]]]:
    """Analyze column changes and return both current policy results and policy comparison"""
    
    # Fail fast if policy is invalid
    if not policy or entity not in policy:
        raise ValueError(f"Policy for {entity} not found in policy: {policy}")
    
    # Calculate comparison for all columns in CSV
    comparison = {}
    current_changes = {}
    
    for col in csv_columns.get(entity, []):
        if col in ['artists', 'artist_spotify_uris']:
            continue
        
        # Create temporary policies for comparison
        prefer_non_null_policy = {entity: {col: 'prefer_non_null'}}
        prefer_incoming_policy = {entity: {col: 'prefer_incoming'}}
        
        comparison[col] = {
            'prefer_non_null': analyze_column_changes(conn, entity, csv_columns, prefer_non_null_policy).get(col, 0),
            'prefer_incoming': analyze_column_changes(conn, entity, csv_columns, prefer_incoming_policy).get(col, 0)
        }
        
        # Extract current policy results if this column is in the policy
        if col in policy[entity]:
            current_policy_type = policy[entity][col]
            if current_policy_type in comparison[col]:
                current_changes[col] = comparison[col][current_policy_type]
    
    return current_changes, comparison


def analyze_column_changes(conn, entity: str, csv_columns: Dict[str, List[str]], policy: Dict) -> Dict[str, int]:
    """Analyze column-level changes by comparing staging vs main BEFORE merge"""
    if not policy or entity not in policy:
        return {}
    
    column_changes = {}
    
    for col, policy_type in policy[entity].items():
        # Skip artist relationships (handled by association analysis)
        if col in ['artists', 'artist_spotify_uris']:
            continue
        
        # Special handling for album_spotify_uri (relationship column)
        if col == 'album_spotify_uri' and entity == 'tracks':
            if col in csv_columns[entity]:
                # Compare current album_id with what new album_id would be
                if policy_type == 'prefer_incoming':
                    query = f"""
                    SELECT COUNT(*) FROM staging_{entity} s
                    JOIN {entity} m ON m.spotify_uri = s.spotify_uri
                    LEFT JOIN albums al ON al.spotify_uri = s.album_spotify_uri
                    WHERE m.album_id IS DISTINCT FROM al.id
                    """
                elif policy_type == 'prefer_non_null':
                    query = f"""
                    SELECT COUNT(*) FROM staging_{entity} s
                    JOIN {entity} m ON m.spotify_uri = s.spotify_uri
                    LEFT JOIN albums al ON al.spotify_uri = s.album_spotify_uri
                    WHERE m.album_id IS NULL AND al.id IS NOT NULL
                    """
                else:
                    continue
                
                try:
                    count = conn.execute(query).fetchone()[0]
                    if count > 0:
                        column_changes[col] = count
                except Exception as e:
                    print(f"[DEBUG] Album relationship analysis failed for {entity}.{col}: {e}")
                    print(f"[DEBUG] Query: {query}")
            continue
            
        if col in csv_columns[entity]:
            staging_col = f"s.{col}"
            
            if policy_type == 'prefer_incoming':
                query = f"""
                SELECT COUNT(*) FROM staging_{entity} s
                JOIN {entity} m ON m.spotify_uri = s.spotify_uri
                WHERE {staging_col} IS DISTINCT FROM m.{col}
                """
            elif policy_type == 'prefer_non_null':
                query = f"""
                SELECT COUNT(*) FROM staging_{entity} s
                JOIN {entity} m ON m.spotify_uri = s.spotify_uri
                WHERE m.{col} IS NULL AND {staging_col} IS NOT NULL
                """
            else:
                continue
            
            try:
                count = conn.execute(query).fetchone()[0]
                if count > 0:
                    column_changes[col] = count
            except Exception as e:
                print(f"[DEBUG] Column analysis failed for {entity}.{col}: {e}")
                print(f"[DEBUG] Query: {query}")
                raise  # Re-raise to see the actual error
    
    return column_changes