"""
staging_stats.py - Comprehensive staging vs main database analysis

Orchestrates analysis across entity stats, association stats, and side effect stats
to provide complete merge operation insights.
"""

from typing import Dict, List, Any
from .entity_stats import EntityStatsAnalyzer
from .association_stats import AssociationStatsAnalyzer
from .side_effect_stats import SideEffectStatsAnalyzer


class StagingAnalyzer:
    def __init__(self, conn, entity: str, csv_columns: Dict[str, List[str]], policy: Dict = None):
        self.conn = conn
        self.entity = entity
        self.entity_analyzer = EntityStatsAnalyzer(conn, entity, csv_columns, policy)
        self.association_analyzer = AssociationStatsAnalyzer(conn, entity, csv_columns)
        self.side_effect_analyzer = SideEffectStatsAnalyzer(conn, entity, csv_columns)
        
    def analyze_all(self) -> Dict[str, Any]:
        """Run all analysis and return comprehensive statistics"""
        # Get basic entity stats
        stats = self.entity_analyzer.analyze_all()
        
        # Add association analysis
        stats['association_stats'] = self.association_analyzer.analyze_all()
        
        # Add side effect analysis
        stats['side_effect_creation'] = self.side_effect_analyzer.analyze_all()
        
        return stats
    
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


def analyze_staging_vs_main(conn, entity: str, csv_columns: Dict[str, List[str]], policy: Dict = None) -> Dict[str, Any]:
    """Main entry point for staging vs main analysis"""
    analyzer = StagingAnalyzer(conn, entity, csv_columns, policy)
    stats = analyzer.analyze_all()
    analyzer.print_report(stats)
    return stats