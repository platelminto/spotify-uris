"""
column_changes.py - Column-level change analysis

Analyzes what column values will change by comparing staging vs pre-merge main table.
"""

from typing import Dict, List


def analyze_column_changes(conn, entity: str, csv_columns: Dict[str, List[str]], policy: Dict) -> Dict[str, int]:
    """Analyze column-level changes by comparing staging vs main BEFORE merge"""
    if not policy or entity not in policy:
        return {}
    
    column_changes = {}
    
    for col, policy_type in policy[entity].items():
        if col != 'artists' and col in csv_columns[entity]:
            if policy_type == 'prefer_incoming':
                query = f"""
                SELECT COUNT(*) FROM staging_{entity} s
                JOIN {entity} m ON m.spotify_uri = s.spotify_uri
                WHERE s.{col} IS DISTINCT FROM m.{col}
                """
            elif policy_type == 'prefer_non_null':
                query = f"""
                SELECT COUNT(*) FROM staging_{entity} s
                JOIN {entity} m ON m.spotify_uri = s.spotify_uri
                WHERE m.{col} IS NULL AND s.{col} IS NOT NULL
                """
            else:
                continue
            
            try:
                count = conn.execute(query).fetchone()[0]
                if count > 0:
                    column_changes[col] = count
            except:
                pass
    
    return column_changes