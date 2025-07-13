"""
association_stats.py - Association table analysis

Analyzes changes to association tables (album_artists, track_artists)
including new associations, recreated associations, and deletions.
"""

from typing import Dict, List, Any


class AssociationStatsAnalyzer:
    def __init__(self, conn, entity: str, csv_columns: Dict[str, List[str]]):
        self.conn = conn
        self.entity = entity
        self.staging_table = f"staging_{entity}"
        self.csv_columns = csv_columns[entity]

    def _count_main_rows(self) -> int:
        """Count total rows in main table"""
        result = self.conn.execute(f"SELECT COUNT(*) FROM {self.entity}").fetchone()
        return result[0] if result else 0

    def analyze_all(self) -> List[Dict[str, Any]]:
        """Analyze association table changes for all applicable associations"""
        associations = []
        
        # Artist associations (for albums and tracks)
        if self.entity in ["albums", "tracks"] and "artist_spotify_uris" in self.csv_columns:
            artist_assoc = self._analyze_artist_associations()
            if artist_assoc:
                associations.append(artist_assoc)
        
                
        return associations

    def _analyze_artist_associations(self) -> Dict[str, Any]:
        """Analyze artist association table changes using optimized single CTE query"""
        association_table = f"{self.entity[:-1]}_artists"  # albums -> album_artists
        entity_singular = self.entity[:-1]  # albums -> album
        
        # Check if association table exists
        table_exists_sql = f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = '{association_table}'
        )
        """
        exists = self.conn.execute(table_exists_sql).fetchone()[0]
        if not exists:
            return {"error": f"Association table {association_table} does not exist"}
        
        # Single optimized CTE query to get all counts at once
        is_empty_main = self._count_main_rows() == 0
        
        if is_empty_main:
            # For empty main table, simplified analysis
            analysis_sql = f"""
            WITH current_count AS (
                SELECT COUNT(*) as current_associations FROM {association_table}
            ),
            artist_splits AS (
                SELECT DISTINCT s.spotify_uri, artist_pos.artist_uri
                FROM staging_{self.entity} s
                CROSS JOIN LATERAL (
                    SELECT unnest(string_to_array(replace(s.artist_spotify_uris, '"', ''), ',')) as artist_uri
                ) as artist_pos
                JOIN artists ar ON ar.spotify_uri = artist_pos.artist_uri
                WHERE s.artist_spotify_uris IS NOT NULL 
                  AND s.artist_spotify_uris != ''
            ),
            potential_count AS (
                SELECT COUNT(*) as potential_associations FROM artist_splits
            ),
            entities_count AS (
                SELECT COUNT(DISTINCT s.spotify_uri) as entities_with_changes
                FROM staging_{self.entity} s
                WHERE s.artist_spotify_uris IS NOT NULL AND s.artist_spotify_uris != ''
            )
            SELECT 
                current_count.current_associations,
                potential_count.potential_associations,
                potential_count.potential_associations as new_associations,
                0 as recreated_associations,
                0 as deleted_associations,
                entities_count.entities_with_changes
            FROM current_count, potential_count, entities_count
            """
        else:
            # For existing data, full analysis with all counts in one query
            analysis_sql = f"""
            WITH current_count AS (
                SELECT COUNT(*) as current_associations FROM {association_table}
            ),
            artist_splits AS (
                SELECT s.spotify_uri, artist_pos.artist_uri
                FROM staging_{self.entity} s
                CROSS JOIN LATERAL (
                    SELECT unnest(string_to_array(replace(s.artist_spotify_uris, '"', ''), ',')) as artist_uri
                ) as artist_pos
                JOIN artists ar ON ar.spotify_uri = artist_pos.artist_uri
                WHERE s.artist_spotify_uris IS NOT NULL 
                  AND s.artist_spotify_uris != ''
            ),
            potential_count AS (
                SELECT COUNT(DISTINCT (e.id, ar.id)) as potential_associations
                FROM artist_splits aspl
                JOIN {self.entity} e ON e.spotify_uri = aspl.spotify_uri
                JOIN artists ar ON ar.spotify_uri = aspl.artist_uri
            ),
            recreated_count AS (
                SELECT COUNT(DISTINCT (e.id, ar.id)) as recreated_associations
                FROM artist_splits aspl
                JOIN {self.entity} e ON e.spotify_uri = aspl.spotify_uri
                JOIN artists ar ON ar.spotify_uri = aspl.artist_uri
                INNER JOIN {association_table} existing ON existing.{entity_singular}_id = e.id AND existing.artist_id = ar.id
            ),
            deleted_count AS (
                SELECT COUNT(*) as deleted_associations
                FROM {association_table} existing
                WHERE existing.{entity_singular}_id IN (
                    SELECT e.id FROM staging_{self.entity} s
                    JOIN {self.entity} e ON e.spotify_uri = s.spotify_uri
                    WHERE s.artist_spotify_uris IS NOT NULL AND s.artist_spotify_uris != ''
                )
            ),
            entities_count AS (
                SELECT COUNT(DISTINCT e.id) as entities_with_changes
                FROM staging_{self.entity} s
                JOIN {self.entity} e ON e.spotify_uri = s.spotify_uri
                WHERE s.artist_spotify_uris IS NOT NULL AND s.artist_spotify_uris != ''
            )
            SELECT 
                current_count.current_associations,
                potential_count.potential_associations,
                potential_count.potential_associations - recreated_count.recreated_associations as new_associations,
                recreated_count.recreated_associations,
                deleted_count.deleted_associations,
                entities_count.entities_with_changes
            FROM current_count, potential_count, recreated_count, deleted_count, entities_count
            """
        
        try:
            result = self.conn.execute(analysis_sql).fetchone()
            return {
                "table_name": association_table,
                "current_associations": result[0],
                "potential_associations": result[1],
                "new_associations": result[2],
                "recreated_associations": result[3],
                "deleted_associations": result[4],
                "entities_with_changes": result[5]
            }
        except Exception as e:
            return {"error": f"Analysis failed: {e}"}

