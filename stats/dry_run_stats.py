"""
dry_run_stats.py - Dry-run analysis using actual merge SQL

Runs the real merge operations in a transaction, captures row counts,
then rolls back. This gives 100% accurate predictions with zero duplicate logic.
"""

from typing import Dict, List, Any
from datetime import datetime, timezone
import time
from .column_changes import analyze_column_changes_with_comparison
from .association_changes import analyze_association_changes_with_comparison


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
        
        import time
        t_start = time.time()
        # Analyze column changes BEFORE any merge happens
        column_changes, column_comparison = analyze_column_changes_with_comparison(self.conn, self.entity, self.csv_columns, self.policy)
        t_elapsed = time.time() - t_start
        print(f"[DEBUG] [{time.strftime('%H:%M:%S', time.localtime())}] Column changes analysis took {t_elapsed:.2f}s")

        # Analyze association changes BEFORE any merge happens
        t_start = time.time()
        association_stats, association_comparison, artist_change_distribution = analyze_association_changes_with_comparison(self.conn, self.entity, self.csv_columns, self.policy)
        t_elapsed = time.time() - t_start
        print(f"[DEBUG] [{time.strftime('%H:%M:%S', time.localtime())}] Association changes analysis took {t_elapsed:.2f}s")

        return {
            'staging_rows': staging_count,
            'main_rows': main_count,
            'policy': self.policy,
            'entity': self.entity,
            'column_changes': column_changes,
            'column_comparison': column_comparison,
            'association_stats': association_stats,
            'association_comparison': association_comparison,
            'artist_change_distribution': artist_change_distribution
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
        column_updates = self._count_unique_rows_with_column_changes(stats.get('column_changes', {}))
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
    

    def _count_unique_rows_with_column_changes(self, column_changes: Dict[str, int]) -> int:
        """Count unique rows that have ANY column changes (not sum of individual column changes)"""
        if not column_changes or not self.policy or self.entity not in self.policy:
            return 0
        
        # Build a query to count distinct rows that have changes in ANY tracked column
        conditions = []
        
        for col, policy_type in self.policy[self.entity].items():
            if col in ['artists', 'artist_spotify_uris']:
                continue
            
            # Special handling for album_spotify_uri (relationship column)
            if col == 'album_spotify_uri' and self.entity == 'tracks' and col in column_changes:
                if policy_type == 'prefer_incoming':
                    conditions.append("m.album_id IS DISTINCT FROM al.id")
                elif policy_type == 'prefer_non_null':
                    conditions.append("(m.album_id IS NULL AND al.id IS NOT NULL)")
                continue
                
            if col in self.csv_columns[self.entity] and col in column_changes:
                if policy_type == 'prefer_incoming':
                    conditions.append(f"s.{col} IS DISTINCT FROM m.{col}")
                elif policy_type == 'prefer_non_null':
                    conditions.append(f"(m.{col} IS NULL AND s.{col} IS NOT NULL)")
        
        if not conditions:
            return 0
        
        # Use OR to combine conditions so we count each row only once
        where_clause = " OR ".join(conditions)
        
        # Add album join if we have album_spotify_uri conditions
        album_join = ""
        if 'album_spotify_uri' in column_changes and self.entity == 'tracks':
            album_join = "LEFT JOIN albums al ON al.spotify_uri = s.album_spotify_uri"
        
        query = f"""
            SELECT COUNT(DISTINCT m.id) FROM staging_{self.entity} s
            JOIN {self.entity} m ON m.spotify_uri = s.spotify_uri
            {album_join}
            WHERE {where_clause}
        """
        
        try:
            result = self.conn.execute(query).fetchone()
            return result[0] if result else 0
        except Exception as e:
            print(f"[DEBUG] Count unique rows query failed: {e}")
            print(f"[DEBUG] Query: {query}")
            raise  # Don't silently fall back - show the actual error


def analyze_staging_vs_main_with_merge(conn, entity: str, csv_columns: Dict[str, List[str]], policy: Dict, merge_sql: List[str], source_name: str) -> Dict[str, Any]:
    """Run actual merge and capture stats, but don't commit/rollback (transaction managed externally)"""
    analyzer = DryRunStatsAnalyzer(conn, entity, csv_columns, policy, source_name)
    
    # Capture initial state
    start_time = time.time()
    print(f"[DEBUG] [{time.strftime('%H:%M:%S', time.localtime(start_time))}] Capturing initial state for {entity}...")
    stats = analyzer._capture_initial_state()
    initial_elapsed = time.time() - start_time
    
    # Capture before counts
    before_start = time.time()
    print(f"[DEBUG] [{time.strftime('%H:%M:%S', time.localtime(before_start))}] Capturing before counts for {entity}... (initial state took {initial_elapsed:.2f}s)")
    before_counts = analyzer._capture_table_counts()
    before_elapsed = time.time() - before_start
    
    # Execute all merge SQL
    merge_start = time.time()
    print(f"[DEBUG] [{time.strftime('%H:%M:%S', time.localtime(merge_start))}] Executing merge SQL for {entity}... (before counts took {before_elapsed:.2f}s)")
    for i, sql in enumerate(merge_sql):
        stmt_start = time.time()
        # Print first 100 chars to identify the statement
        sql_preview = sql.strip()[:100].replace('\n', ' ')
        print(f"[DEBUG] Statement {i+1}: {sql_preview}...")
        conn.execute(sql)
        stmt_elapsed = time.time() - stmt_start
        print(f"[DEBUG] Statement {i+1} took {stmt_elapsed:.2f}s")
    merge_elapsed = time.time() - merge_start
    
    # Capture after counts
    after_start = time.time()
    print(f"[DEBUG] [{time.strftime('%H:%M:%S', time.localtime(after_start))}] Capturing after counts for {entity}... (merge took {merge_elapsed:.2f}s)")
    after_counts = analyzer._capture_table_counts()
    after_elapsed = time.time() - after_start
    
    # Calculate changes
    changes_start = time.time()
    print(f"[DEBUG] [{time.strftime('%H:%M:%S', time.localtime(changes_start))}] Calculating changes for {entity}... (after counts took {after_elapsed:.2f}s)")
    changes = analyzer._calculate_changes(before_counts, after_counts)
    stats['changes'] = changes
    changes_elapsed = time.time() - changes_start
    
    # Capture final state
    final_start = time.time()
    print(f"[DEBUG] [{time.strftime('%H:%M:%S', time.localtime(final_start))}] Capturing final state for {entity}... (changes took {changes_elapsed:.2f}s)")
    stats.update(analyzer._capture_final_state(stats))
    final_elapsed = time.time() - final_start
    
    total_elapsed = time.time() - start_time
    print(f"[DEBUG] [{time.strftime('%H:%M:%S', time.localtime())}] Analysis complete for {entity} (final state took {final_elapsed:.2f}s, total: {total_elapsed:.2f}s)")
    
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
    
    if stats.get('column_comparison'):
        print("🔄 COLUMN POLICY COMPARISON:")
        print(f"{'Column':<20} {'prefer_non_null':<15} {'prefer_incoming':<15}")
        print("-" * 52)
        
        entity_policy = stats.get('policy', {}).get(entity, {})
        for col in sorted(stats['column_comparison'].keys()):
            counts = stats['column_comparison'][col]
            non_null_count = counts['prefer_non_null']
            incoming_count = counts['prefer_incoming']
            
            # Add asterisk to show current policy
            current_policy = entity_policy.get(col, 'prefer_incoming')  # default fallback
            non_null_label = f"{non_null_count:,}*" if current_policy == 'prefer_non_null' else f"{non_null_count:,}"
            incoming_label = f"{incoming_count:,}*" if current_policy == 'prefer_incoming' else f"{incoming_count:,}"
            
            # Only show rows that have some changes
            if non_null_count > 0 or incoming_count > 0:
                print(f"{col:<20} {non_null_label:<15} {incoming_label:<15}")
        print()
    
    # Artist policy comparison
    if stats.get('association_comparison'):
        entity_name = "TRACK" if entity == "tracks" else "ALBUM"
        print(f"🔗 ARTIST POLICY COMPARISON ({entity_name}-ARTIST ASSOCIATIONS):")
        print(f"{'Policy':<15} {'New Assocs':<12} {'Deleted Assocs':<15} {'Net Change':<12} {'Affected ' + entity_name.title() + 's':<15}")
        print("-" * 70)
        
        current_artist_policy = stats.get('policy', {}).get(entity, {}).get('artists', 'prefer_incoming')
        for policy_type in ['extend', 'prefer_incoming', 'prefer_non_null']:
            if policy_type in stats['association_comparison']:
                comp = stats['association_comparison'][policy_type]
                policy_label = f"{policy_type}*" if policy_type == current_artist_policy else policy_type
                print(f"{policy_label:<15} {comp['new_associations']:<12,} {comp['deleted_associations']:<15,} {comp['net_change']:<12,} {comp['entities_with_changes']:<15,}")
        print()
    
    # Artist change distribution
    if stats.get('artist_change_distribution'):
        entity_name = "TRACK" if entity == "tracks" else "ALBUM"
        current_artist_policy = stats.get('policy', {}).get(entity, {}).get('artists', 'prefer_incoming')
        print(f"📊 ARTIST CHANGE BREAKDOWN ({entity_name}S, current policy: {current_artist_policy}):")
        
        dist = stats['artist_change_distribution']
        
        # Show gaining artists
        if dist.get('gaining'):
            gaining_items = []
            three_plus_total = 0
            for change, count in sorted(dist['gaining'].items()):
                if change >= 3:
                    three_plus_total += count
                else:
                    gaining_items.append(f"+{change}: {count:,}")
            if three_plus_total > 0:
                gaining_items.append(f"+3+: {three_plus_total:,}")
            if gaining_items:
                print(f"  Gaining artists: {', '.join(gaining_items)} {entity_name.lower()}s")
        
        # Show losing artists  
        if dist.get('losing'):
            losing_items = []
            for change, count in sorted(dist['losing'].items()):
                losing_items.append(f"-{change}: {count:,}")
            if losing_items:
                print(f"  Losing artists: {', '.join(losing_items)} {entity_name.lower()}s")
        
        # Show same artists
        if dist.get('same', 0) > 0:
            print(f"  Same artists: {dist['same']:,} {entity_name.lower()}s")
        
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