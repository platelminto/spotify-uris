"""
dry_run_stats.py - Dry-run analysis using actual merge SQL

Runs the real merge operations in a transaction, captures row counts,
then rolls back. This gives 100% accurate predictions with zero duplicate logic.
"""

from typing import Dict, List, Any
from datetime import datetime, timezone
from .column_changes import analyze_column_changes
from .association_changes import analyze_association_changes


class DryRunStatsAnalyzer:
    def __init__(self, conn, entity: str, csv_columns: Dict[str, List[str]], policy: Dict, source_name: str = "DRY_RUN"):
        self.conn = conn
        self.entity = entity
        self.csv_columns = csv_columns
        self.policy = policy
        self.source_name = source_name
        self.timestamp = datetime.now(timezone.utc).isoformat()
        
    
    def _capture_initial_state(self) -> Dict[str, Any]:
        """Capture counts before any changes"""
        staging_count = self.conn.execute(f"SELECT COUNT(*) FROM staging_{self.entity}").fetchone()[0]
        main_count = self.conn.execute(f"SELECT COUNT(*) FROM {self.entity}").fetchone()[0]
        
        # Analyze column changes BEFORE any merge happens
        column_changes = analyze_column_changes(self.conn, self.entity, self.csv_columns, self.policy)
        
        # Analyze association changes BEFORE any merge happens
        association_stats = analyze_association_changes(self.conn, self.entity, self.csv_columns, self.policy)
        
        return {
            'staging_rows': staging_count,
            'main_rows': main_count,
            'policy': self.policy,
            'entity': self.entity,
            'column_changes': column_changes,
            'association_stats': association_stats
        }
    
    def _capture_table_counts(self) -> Dict[str, int]:
        """Capture row counts for all relevant tables"""
        counts = {}
        
        # Main entity table
        counts[self.entity] = self.conn.execute(f"SELECT COUNT(*) FROM {self.entity}").fetchone()[0]
        
        # Association tables if they exist
        if self.entity in ["albums", "tracks"]:
            assoc_table = f"{self.entity[:-1]}_artists"
            try:
                counts[assoc_table] = self.conn.execute(f"SELECT COUNT(*) FROM {assoc_table}").fetchone()[0]
            except:
                counts[assoc_table] = 0
        
        # Side effect tables (artists, albums if we're loading tracks)
        if self.entity == "tracks":
            counts['albums'] = self.conn.execute("SELECT COUNT(*) FROM albums").fetchone()[0]
        
        if self.entity in ["albums", "tracks"]:
            counts['artists'] = self.conn.execute("SELECT COUNT(*) FROM artists").fetchone()[0]
            
        return counts
    
    
    def _calculate_changes(self, before: Dict[str, int], after: Dict[str, int]) -> Dict[str, int]:
        """Calculate the changes between before and after counts"""
        changes = {}
        for table in before:
            changes[table] = after.get(table, 0) - before.get(table, 0)
        return changes
    
    def _capture_final_state(self, stats: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze the final results and compute summary stats"""
        
        # Calculate entity-level changes by comparing before/after main table counts
        initial_main = stats['main_rows']
        final_main = self._capture_table_counts()[self.entity]
        new_rows = final_main - initial_main
        
        # Existing rows = staging rows that didn't result in new main table rows
        existing_rows = stats['staging_rows'] - new_rows
        
        # Check if there are any changes (column changes or association changes)
        column_updates = sum(stats.get('column_changes', {}).values())
        association_updates = 0
        if stats.get('changes', {}).get(f"{self.entity[:-1]}_artists", 0) != 0:
            association_updates = existing_rows  # If associations changed, all existing rows with associations are "updated"
        
        updates_needed = max(column_updates, association_updates)
        no_change_updates = existing_rows - updates_needed
        
        # Association stats - combine pre-merge analysis with post-merge count
        pre_merge_association_stats = stats.get('association_stats', [])
        if pre_merge_association_stats:
            # Update with actual post-merge count
            assoc_table = pre_merge_association_stats[0]['table_name']
            actual_final_count = self.conn.execute(f"SELECT COUNT(*) FROM {assoc_table}").fetchone()[0]
            pre_merge_association_stats[0]['potential_associations'] = actual_final_count
        association_stats = pre_merge_association_stats
        
        # Side effect stats
        side_effect_stats = self._analyze_side_effects(stats)
        
        return {
            'new_rows': new_rows,
            'existing_rows': existing_rows,
            'updates_needed': updates_needed,
            'no_change_updates': no_change_updates,
            'association_stats': association_stats,
            'side_effect_creation': side_effect_stats
        }
    
    def _analyze_side_effects(self, stats: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze side effect entity creation using simple before/after counts"""
        side_effects = []
        changes = stats.get('changes', {})
        
        # Check for new artists created
        new_artists = changes.get('artists', 0)
        if new_artists > 0:
            side_effects.append({
                'description': f"New artists created from {self.entity} references",
                'new_entities': new_artists
            })
        
        # Check for new albums created (when loading tracks)
        if self.entity == 'tracks':
            new_albums = changes.get('albums', 0)
            if new_albums > 0:
                side_effects.append({
                    'description': f"New albums created from {self.entity} references",
                    'new_entities': new_albums
                })
        
        return side_effects
    


def analyze_staging_vs_main_with_merge(conn, entity: str, csv_columns: Dict[str, List[str]], policy: Dict, merge_sql: List[str], source_name: str) -> Dict[str, Any]:
    """Run actual merge and capture stats, but don't commit/rollback (transaction managed externally)"""
    analyzer = DryRunStatsAnalyzer(conn, entity, csv_columns, policy, source_name)
    
    # Capture initial state
    stats = analyzer._capture_initial_state()
    
    # Capture before counts
    before_counts = analyzer._capture_table_counts()
    
    # Execute all merge SQL
    for sql in merge_sql:
        conn.execute(sql)
    
    # Capture after counts
    after_counts = analyzer._capture_table_counts()
    
    # Calculate changes
    changes = analyzer._calculate_changes(before_counts, after_counts)
    stats['changes'] = changes
    
    # Capture final state
    stats.update(analyzer._capture_final_state(stats))
    
    _print_stats_report(entity, stats)
    
    return stats


def _print_stats_report(entity: str, stats: Dict[str, Any]):
    """Print the stats report (shared between dry-run and actual merge)"""
    print(f"\n=== MERGE ANALYSIS for {entity.upper()} ===")
    print(f"Staging table rows: {stats['staging_rows']:,}")
    print(f"Main table rows: {stats['main_rows']:,}")
    
    # Show policy info
    if stats.get('policy') and entity in stats['policy']:
        print(f"\n🔧 MERGE POLICY for {entity}:")
        for col, policy_type in stats['policy'][entity].items():
            print(f"  {col}: {policy_type}")
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
            print(f"\n🔗 ASSOCIATION CHANGES ({assoc['table_name'].upper()}):")
            print(f"  Current associations: {assoc['current_associations']:,}")
            print(f"  Entities with association changes: {assoc['entities_with_changes']:,}")
            print(f"  New associations (never existed): {assoc['new_associations']:,}")
            print(f"  Recreated associations (identical pairs): {assoc['recreated_associations']:,}")
            print(f"  Associations to be deleted: {assoc['deleted_associations']:,}")
            print(f"  Total associations after merge: {assoc['potential_associations']:,}")

    # Side effect entity creation stats
    if stats['side_effect_creation']:
        for side_effect in stats['side_effect_creation']:
            if side_effect['new_entities'] > 0:
                print(f"\n➕ SIDE EFFECT CREATION:")
                print(f"  {side_effect['description']}: {side_effect['new_entities']:,}")