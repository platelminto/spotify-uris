"""
entity_stats.py - Basic entity counting and column change analysis

Provides core statistics about staging vs main database entities,
including row counts, new/existing analysis, and column-level changes.
"""

from typing import Dict, List, Any


class EntityStatsAnalyzer:
    def __init__(self, conn, entity: str, csv_columns: Dict[str, List[str]], policy: Dict = None):
        self.conn = conn
        self.entity = entity
        self.staging_table = f"staging_{entity}"
        self.csv_columns = csv_columns[entity]
        self.policy = policy
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

    def _count_staging_rows(self) -> int:
        """Count total rows in staging table"""
        result = self.conn.execute(f"SELECT COUNT(*) FROM {self.staging_table}").fetchone()
        return result[0] if result else 0
        
    def _count_main_rows(self) -> int:
        """Count total rows in main table"""
        result = self.conn.execute(f"SELECT COUNT(*) FROM {self.entity}").fetchone()
        return result[0] if result else 0
        
    def _count_new_rows(self) -> int:
        """Count rows in staging that don't exist in main (by spotify_uri)"""
        if 'spotify_uri' not in self.csv_columns:
            return 0
            
        sql = f"""
        SELECT COUNT(*)
        FROM {self.staging_table} s
        LEFT JOIN {self.entity} m ON m.spotify_uri = s.spotify_uri
        WHERE m.spotify_uri IS NULL
        """
        result = self.conn.execute(sql).fetchone()
        return result[0] if result else 0
        
    def _count_existing_rows(self) -> int:
        """Count rows in staging that already exist in main (by spotify_uri)"""
        if 'spotify_uri' not in self.csv_columns:
            return 0
            
        sql = f"""
        SELECT COUNT(*)
        FROM {self.staging_table} s
        INNER JOIN {self.entity} m ON m.spotify_uri = s.spotify_uri
        """
        result = self.conn.execute(sql).fetchone()
        return result[0] if result else 0
        
    def _count_updates_needed(self) -> int:
        """Count existing rows that need updates (have different values and policy allows updates)"""
        if 'spotify_uri' not in self.csv_columns or not self.comparable_columns:
            return 0
            
        # Build comparison conditions for each comparable column
        conditions = []
        for col in self.comparable_columns:
            if col != 'spotify_uri':  # Skip the join key
                # Special handling for array columns to ensure proper type casting
                if col == 'genres':
                    conditions.append(f"s.{col}::text[] IS DISTINCT FROM m.{col}::text[]")
                else:
                    conditions.append(f"s.{col} IS DISTINCT FROM m.{col}")
        
        if not conditions:
            return 0
            
        where_clause = " OR ".join(conditions)
        
        # Add policy-based WHERE clause if policy exists
        policy_where = self._build_policy_where_clause()
        if policy_where:
            where_clause = f"({where_clause}) AND ({policy_where})"
        
        sql = f"""
        SELECT COUNT(*)
        FROM {self.staging_table} s
        INNER JOIN {self.entity} m ON m.spotify_uri = s.spotify_uri
        WHERE {where_clause}
        """
        result = self.conn.execute(sql).fetchone()
        return result[0] if result else 0
        
    def _count_no_change_updates(self) -> int:
        """Count existing rows that don't need updates (identical values)"""
        return self._count_existing_rows() - self._count_updates_needed()
    
    def _build_policy_where_clause(self) -> str:
        """Build WHERE clause based on policy to match sql_templates logic"""
        # No WHERE clause needed - prefer_non_null logic is handled per-column in SET clause
        # Stats should show all potential changes, not filtered by prefer_non_null
        return ""

    def _analyze_column_changes(self) -> Dict[str, int]:
        """Analyze changes at the column level (respecting policy)"""
        if 'spotify_uri' not in self.csv_columns or not self.comparable_columns:
            return {}
            
        column_stats = {}
        policy_where = self._build_policy_where_clause()
        
        for col in self.comparable_columns:
            if col == 'spotify_uri':  # Skip the join key
                continue
            
            # Special handling for array columns to ensure proper type casting
            if col == 'genres':
                where_clause = f"s.{col}::text[] IS DISTINCT FROM m.{col}::text[]"
            else:
                where_clause = f"s.{col} IS DISTINCT FROM m.{col}"
            if policy_where:
                where_clause = f"({where_clause}) AND ({policy_where})"
                
            sql = f"""
            SELECT COUNT(*)
            FROM {self.staging_table} s
            INNER JOIN {self.entity} m ON m.spotify_uri = s.spotify_uri
            WHERE {where_clause}
            """
            result = self.conn.execute(sql).fetchone()
            column_stats[col] = result[0] if result else 0
            
        return column_stats

    def analyze_all(self) -> Dict[str, Any]:
        """Run all entity analysis and return comprehensive statistics"""
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
        
        return stats