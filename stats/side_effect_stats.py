"""
side_effect_stats.py - Side effect entity creation analysis

Analyzes entities that will be created as side effects during merge operations,
such as artists created from album/track associations or albums from track references.
"""

from typing import Dict, List, Any


class SideEffectStatsAnalyzer:
    def __init__(self, conn, entity: str, csv_columns: Dict[str, List[str]]):
        self.conn = conn
        self.entity = entity
        self.staging_table = f"staging_{entity}"
        self.csv_columns = csv_columns[entity]

    def analyze_all(self) -> List[Dict[str, Any]]:
        """Analyze entities that will be created as side effects"""
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

