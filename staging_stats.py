"""
staging_stats.py - Statistics analysis for staging vs main database comparison

Provides detailed statistics about what would happen during a merge operation,
including new rows, updates, column-level changes, and duplicate detection.
"""

import psycopg
from typing import Dict, List, Tuple, Any


class StagingAnalyzer:
    def __init__(self, conn, entity: str, csv_columns: Dict[str, List[str]]):
        self.conn = conn
        self.entity = entity
        self.staging_table = f"staging_{entity}"
        self.csv_columns = csv_columns[entity]
        self.main_columns = self._get_main_table_columns()
        self.comparable_columns = [col for col in self.csv_columns if col in self.main_columns]
        
    def _get_main_table_columns(self) -> List[str]:
        """Get list of column names from the main table"""
        sql = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{self.entity}' 
        AND table_schema = 'public'
        """
        result = self.conn.execute(sql).fetchall()
        return [row[0] for row in result]
        
    def analyze_all(self) -> Dict[str, Any]:
        """Run all analysis and return comprehensive statistics"""
        stats = {}
        
        # Basic counts
        stats['staging_rows'] = self._count_staging_rows()
        stats['main_rows'] = self._count_main_rows()
        
        # New vs existing analysis
        stats['new_rows'] = self._count_new_rows()
        stats['existing_rows'] = self._count_existing_rows()
        stats['updates_needed'] = self._count_updates_needed()
        stats['no_change_updates'] = self._count_no_change_updates()
        
        # Column-level analysis
        stats['column_changes'] = self._analyze_column_changes()
        
        # Association table analysis (if applicable)
        stats['association_stats'] = self._analyze_associations()
        
        # Side-effect entity creation analysis
        stats['side_effect_creation'] = self._analyze_side_effect_creation()
        
        return stats
    
    def _count_staging_rows(self) -> int:
        """Count total rows in staging table"""
        result = self.conn.execute(f"SELECT COUNT(*) FROM {self.staging_table}").fetchone()
        return result[0] if result else 0
    
    def _count_main_rows(self) -> int:
        """Count total rows in main table"""
        result = self.conn.execute(f"SELECT COUNT(*) FROM {self.entity}").fetchone()
        return result[0] if result else 0
    
    def _count_new_rows(self) -> int:
        """Count rows in staging that don't exist in main (new inserts)"""
        sql = f"""
        SELECT COUNT(*) 
        FROM {self.staging_table} s
        LEFT JOIN {self.entity} m ON m.spotify_uri = s.spotify_uri
        WHERE m.spotify_uri IS NULL AND s.spotify_uri IS NOT NULL
        """
        result = self.conn.execute(sql).fetchone()
        return result[0] if result else 0
    
    def _count_existing_rows(self) -> int:
        """Count rows in staging that exist in main (potential updates)"""
        sql = f"""
        SELECT COUNT(*) 
        FROM {self.staging_table} s
        INNER JOIN {self.entity} m ON m.spotify_uri = s.spotify_uri
        WHERE s.spotify_uri IS NOT NULL
        """
        result = self.conn.execute(sql).fetchone()
        return result[0] if result else 0
    
    def _count_updates_needed(self) -> int:
        """Count existing rows where at least one column would actually change"""
        # Build comparison conditions for each column that exists in both tables
        conditions = []
        for col in self.comparable_columns:
            if col != 'spotify_uri':  # Skip the key column
                conditions.append(f"(s.{col} IS DISTINCT FROM m.{col})")
        
        if not conditions:
            return 0
            
        where_clause = " OR ".join(conditions)
        sql = f"""
        SELECT COUNT(*) 
        FROM {self.staging_table} s
        INNER JOIN {self.entity} m ON m.spotify_uri = s.spotify_uri
        WHERE s.spotify_uri IS NOT NULL AND ({where_clause})
        """
        result = self.conn.execute(sql).fetchone()
        return result[0] if result else 0
    
    def _count_no_change_updates(self) -> int:
        """Count existing rows where no columns would actually change"""
        return self._count_existing_rows() - self._count_updates_needed()
    
    def _analyze_column_changes(self) -> Dict[str, int]:
        """Analyze which columns are changing and how often"""
        column_stats = {}
        
        for col in self.comparable_columns:
            if col == 'spotify_uri':  # Skip the key column
                continue
                
            sql = f"""
            SELECT COUNT(*) 
            FROM {self.staging_table} s
            INNER JOIN {self.entity} m ON m.spotify_uri = s.spotify_uri
            WHERE s.spotify_uri IS NOT NULL 
              AND s.{col} IS DISTINCT FROM m.{col}
            """
            result = self.conn.execute(sql).fetchone()
            column_stats[col] = result[0] if result else 0
            
        return column_stats
    
    def _analyze_associations(self) -> List[Dict[str, Any]]:
        """Analyze association table changes for all applicable associations"""
        associations = []
        
        # Artist associations (for albums and tracks)
        if self.entity in ["albums", "tracks"] and "artist_spotify_uris" in self.csv_columns:
            artist_assoc = self._analyze_artist_associations()
            if artist_assoc:
                associations.append(artist_assoc)
        
        # Genre associations (for artists)
        if self.entity == "artists" and "genres" in self.csv_columns:
            genre_assoc = self._analyze_genre_associations()
            if genre_assoc:
                associations.append(genre_assoc)
                
        return associations

    def _analyze_artist_associations(self) -> Dict[str, Any]:
        """Analyze artist association table changes"""
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
        
        # Count current associations in main table
        current_associations_sql = f"SELECT COUNT(*) FROM {association_table}"
        current_associations = self.conn.execute(current_associations_sql).fetchone()[0]
        
        # Count what associations would be created from staging
        potential_associations_sql = f"""
        SELECT COUNT(DISTINCT (s.spotify_uri, artist_pos.artist_uri))
        FROM staging_{self.entity} s
        CROSS JOIN LATERAL (
            SELECT unnest(string_to_array(replace(s.artist_spotify_uris, '"', ''), ',')) as artist_uri
        ) as artist_pos
        JOIN artists ar ON ar.spotify_uri = artist_pos.artist_uri
        WHERE s.artist_spotify_uris IS NOT NULL 
          AND s.artist_spotify_uris != ''
        """
        
        try:
            potential_associations = self.conn.execute(potential_associations_sql).fetchone()[0]
        except Exception:
            potential_associations = 0
        
        # Count associations that would be truly new (not existing)
        # For new entities (empty main table), all associations are new
        if self._count_main_rows() == 0:
            new_only_sql = potential_associations_sql
        else:
            new_only_sql = f"""
            SELECT COUNT(DISTINCT (e.id, ar.id))
            FROM staging_{self.entity} s
            JOIN {self.entity} e ON e.spotify_uri = s.spotify_uri  
            CROSS JOIN LATERAL (
                SELECT unnest(string_to_array(replace(s.artist_spotify_uris, '"', ''), ',')) as artist_uri
            ) as artist_pos
            JOIN artists ar ON ar.spotify_uri = artist_pos.artist_uri
            LEFT JOIN {association_table} existing ON existing.{entity_singular}_id = e.id AND existing.artist_id = ar.id
            WHERE s.artist_spotify_uris IS NOT NULL 
              AND s.artist_spotify_uris != ''
              AND existing.{entity_singular}_id IS NULL
            """
        
        try:
            new_associations = self.conn.execute(new_only_sql).fetchone()[0]
        except Exception:
            new_associations = 0
        
        # Count associations that already exist and will be recreated (identical pairs)
        # For new entities (empty main table), no associations are recreated
        if self._count_main_rows() == 0:
            recreated_associations = 0
        else:
            recreated_sql = f"""
            SELECT COUNT(DISTINCT (e.id, ar.id))
            FROM staging_{self.entity} s
            JOIN {self.entity} e ON e.spotify_uri = s.spotify_uri  
            CROSS JOIN LATERAL (
                SELECT unnest(string_to_array(replace(s.artist_spotify_uris, '"', ''), ',')) as artist_uri
            ) as artist_pos
            JOIN artists ar ON ar.spotify_uri = artist_pos.artist_uri
            INNER JOIN {association_table} existing ON existing.{entity_singular}_id = e.id AND existing.artist_id = ar.id
            WHERE s.artist_spotify_uris IS NOT NULL 
              AND s.artist_spotify_uris != ''
            """
            
            try:
                recreated_associations = self.conn.execute(recreated_sql).fetchone()[0]
            except Exception:
                recreated_associations = 0
        
        # Count associations that would be deleted (for prefer_incoming policy)
        # For new entities (empty main table), no associations are deleted
        if self._count_main_rows() == 0:
            deleted_associations = 0
        else:
            deleted_associations_sql = f"""
            SELECT COUNT(*)
            FROM {association_table} existing
            WHERE existing.{entity_singular}_id IN (
                SELECT e.id FROM staging_{self.entity} s
                JOIN {self.entity} e ON e.spotify_uri = s.spotify_uri
                WHERE s.artist_spotify_uris IS NOT NULL AND s.artist_spotify_uris != ''
            )
            """
            
            try:
                deleted_associations = self.conn.execute(deleted_associations_sql).fetchone()[0]
            except Exception:
                deleted_associations = 0
        
        # Count how many entities will have their associations changed
        # For new entities (empty main table), count distinct entities in staging
        if self._count_main_rows() == 0:
            entities_with_changes_sql = f"""
            SELECT COUNT(DISTINCT s.spotify_uri)
            FROM staging_{self.entity} s
            WHERE s.artist_spotify_uris IS NOT NULL AND s.artist_spotify_uris != ''
            """
        else:
            entities_with_changes_sql = f"""
            SELECT COUNT(DISTINCT e.id)
            FROM staging_{self.entity} s
            JOIN {self.entity} e ON e.spotify_uri = s.spotify_uri
            WHERE s.artist_spotify_uris IS NOT NULL AND s.artist_spotify_uris != ''
            """
        
        try:
            entities_with_changes = self.conn.execute(entities_with_changes_sql).fetchone()[0]
        except Exception:
            entities_with_changes = 0
        
        return {
            "table_name": association_table,
            "current_associations": current_associations,
            "potential_associations": potential_associations,
            "new_associations": new_associations,
            "recreated_associations": recreated_associations,
            "deleted_associations": deleted_associations,
            "entities_with_changes": entities_with_changes
        }

    def _analyze_genre_associations(self) -> Dict[str, Any]:
        """Analyze artist-genre association table changes"""
        association_table = "artist_genres"
        
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
        
        # Count current associations in main table
        current_associations_sql = f"SELECT COUNT(*) FROM {association_table}"
        current_associations = self.conn.execute(current_associations_sql).fetchone()[0]
        
        # Count what associations would be created from staging
        new_associations_sql = f"""
        SELECT COUNT(DISTINCT (a.id, g.id))
        FROM staging_artists s
        JOIN artists a ON a.spotify_uri = s.spotify_uri  
        CROSS JOIN LATERAL (
            SELECT unnest(string_to_array(s.genres, ',')) as genre_name
        ) as genre_pos
        JOIN genres g ON TRIM(g.name) = TRIM(genre_pos.genre_name)
        WHERE s.genres IS NOT NULL 
          AND s.genres != ''
        """
        
        try:
            potential_associations = self.conn.execute(new_associations_sql).fetchone()[0]
        except Exception:
            potential_associations = 0
        
        # Count associations that would be new (not existing)
        new_only_sql = f"""
        SELECT COUNT(DISTINCT (a.id, g.id))
        FROM staging_artists s
        JOIN artists a ON a.spotify_uri = s.spotify_uri  
        CROSS JOIN LATERAL (
            SELECT unnest(string_to_array(s.genres, ',')) as genre_name
        ) as genre_pos
        JOIN genres g ON TRIM(g.name) = TRIM(genre_pos.genre_name)
        LEFT JOIN {association_table} existing ON existing.artist_id = a.id AND existing.genre_id = g.id
        WHERE s.genres IS NOT NULL 
          AND s.genres != ''
          AND existing.artist_id IS NULL
        """
        
        try:
            new_associations = self.conn.execute(new_only_sql).fetchone()[0]
        except Exception:
            new_associations = 0
        
        # Count associations that already exist and will be recreated (identical pairs)
        recreated_sql = f"""
        SELECT COUNT(DISTINCT (a.id, g.id))
        FROM staging_artists s
        JOIN artists a ON a.spotify_uri = s.spotify_uri  
        CROSS JOIN LATERAL (
            SELECT unnest(string_to_array(s.genres, ',')) as genre_name
        ) as genre_pos
        JOIN genres g ON TRIM(g.name) = TRIM(genre_pos.genre_name)
        INNER JOIN {association_table} existing ON existing.artist_id = a.id AND existing.genre_id = g.id
        WHERE s.genres IS NOT NULL 
          AND s.genres != ''
        """
        
        try:
            recreated_associations = self.conn.execute(recreated_sql).fetchone()[0]
        except Exception:
            recreated_associations = 0
        
        return {
            "table_name": association_table,
            "current_associations": current_associations,
            "potential_associations": potential_associations,
            "new_associations": new_associations,
            "recreated_associations": recreated_associations
        }

    def _analyze_side_effect_creation(self) -> List[Dict[str, Any]]:
        """Analyze entities that will be created as side effects (e.g., artists from album associations)"""
        side_effects = []
        
        # Artists created from album/track associations
        if self.entity in ["albums", "tracks"] and "artist_spotify_uris" in self.csv_columns:
            artist_creation = self._analyze_artist_creation_from_associations()
            if artist_creation:
                side_effects.append(artist_creation)
        
        # Albums created from track associations
        if self.entity == "tracks" and "album_spotify_uri" in self.csv_columns:
            album_creation = self._analyze_album_creation_from_tracks()
            if album_creation:
                side_effects.append(album_creation)
                
        return side_effects

    def _analyze_artist_creation_from_associations(self) -> Dict[str, Any]:
        """Analyze how many new artists will be created from artist_spotify_uris"""
        # Count distinct artist URIs in staging that don't exist in artists table
        new_artists_sql = f"""
        SELECT COUNT(DISTINCT artist_pos.artist_uri)
        FROM staging_{self.entity} s
        CROSS JOIN LATERAL (
            SELECT unnest(string_to_array(replace(s.artist_spotify_uris, '"', ''), ',')) as artist_uri
        ) as artist_pos
        LEFT JOIN artists existing ON existing.spotify_uri = artist_pos.artist_uri
        WHERE s.artist_spotify_uris IS NOT NULL 
          AND s.artist_spotify_uris != ''
          AND artist_pos.artist_uri IS NOT NULL
          AND artist_pos.artist_uri != ''
          AND existing.spotify_uri IS NULL
        """
        
        try:
            new_artists = self.conn.execute(new_artists_sql).fetchone()[0]
        except Exception:
            new_artists = 0
            
        return {
            "entity_type": "artists",
            "new_entities": new_artists,
            "description": f"New artists created from {self.entity} associations"
        }

    def _analyze_album_creation_from_tracks(self) -> Dict[str, Any]:
        """Analyze how many new albums will be created from track album references"""
        # Count distinct album URIs in staging tracks that don't exist in albums table
        new_albums_sql = f"""
        SELECT COUNT(DISTINCT s.album_spotify_uri)
        FROM staging_tracks s
        LEFT JOIN albums existing ON existing.spotify_uri = s.album_spotify_uri
        WHERE s.album_spotify_uri IS NOT NULL 
          AND s.album_spotify_uri != ''
          AND existing.spotify_uri IS NULL
        """
        
        try:
            new_albums = self.conn.execute(new_albums_sql).fetchone()[0]
        except Exception:
            new_albums = 0
            
        return {
            "entity_type": "albums",
            "new_entities": new_albums,
            "description": "New albums created from track references"
        }
    
    def print_report(self, stats: Dict[str, Any]):
        """Print a formatted report of the statistics"""
        print(f"\n=== STAGING vs MAIN ANALYSIS for {self.entity.upper()} ===")
        print(f"Staging table rows: {stats['staging_rows']:,}")
        print(f"Main table rows: {stats['main_rows']:,}")
        print()
        
        print("📊 MERGE IMPACT:")
        print(f"  New rows (inserts): {stats['new_rows']:,}")
        print(f"  Existing rows (potential updates): {stats['existing_rows']:,}")
        print(f"  Actual updates needed: {stats['updates_needed']:,}")
        print(f"  No-change updates: {stats['no_change_updates']:,}")
        print()
        
        if stats['column_changes']:
            print("📋 COLUMN-LEVEL CHANGES:")
            for col, count in sorted(stats['column_changes'].items()):
                print(f"  {col}: {count:,} changes")
            print()
        
        # Summary percentages
        total_staging = stats['staging_rows']
        if total_staging > 0:
            new_pct = (stats['new_rows'] / total_staging) * 100
            update_pct = (stats['updates_needed'] / total_staging) * 100
            nochange_pct = (stats['no_change_updates'] / total_staging) * 100
            
            print("📈 PERCENTAGES:")
            print(f"  New data: {new_pct:.1f}%")
            print(f"  Updates: {update_pct:.1f}%")
            print(f"  No changes: {nochange_pct:.1f}%")
        
        # Association table stats
        if stats['association_stats']:
            for assoc in stats['association_stats']:
                if 'error' not in assoc:
                    print(f"\n🔗 ASSOCIATION CHANGES ({assoc['table_name'].upper()}):")
                    print(f"  Current associations: {assoc['current_associations']:,}")
                    print(f"  Entities with association changes: {assoc['entities_with_changes']:,}")
                    print(f"  New associations (never existed): {assoc['new_associations']:,}")
                    print(f"  Recreated associations (identical pairs): {assoc['recreated_associations']:,}")
                    print(f"  Associations to be deleted: {assoc['deleted_associations']:,}")
                    print(f"  Total associations after merge: {assoc['current_associations'] - assoc['deleted_associations'] + assoc['potential_associations']:,}")

        # Side effect entity creation stats
        if stats['side_effect_creation']:
            for side_effect in stats['side_effect_creation']:
                if side_effect['new_entities'] > 0:
                    print(f"\n➕ SIDE EFFECT CREATION:")
                    print(f"  {side_effect['description']}: {side_effect['new_entities']:,}")


def analyze_staging_vs_main(conn, entity: str, csv_columns: Dict[str, List[str]]) -> Dict[str, Any]:
    """Main entry point for staging vs main analysis"""
    analyzer = StagingAnalyzer(conn, entity, csv_columns)
    stats = analyzer.analyze_all()
    analyzer.print_report(stats)
    return stats