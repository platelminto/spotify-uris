#!/usr/bin/env python
"""
load_csv_engine.py - Generic CSV → PostgreSQL loader engine

Usage: python load_csv_engine.py --config mpd_loader --file csvs/mpd/artists.csv
"""

import os, sys, pathlib, time, psycopg
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()


# Define all possible columns for each entity (for the database tables)
ALL_COLUMNS = {
    "artists": {"spotify_uri": "text", "mbid": "text", "name": "citext", "genres": "text[]"},
    "albums": {
        "spotify_uri": "text",
        "mbid": "text",
        "name": "citext",
        "album_type": "text",
        "spotify_release_date": "date",
        "release_date_precision": "text",
        "n_tracks": "int",
    },
    "tracks": {
        "spotify_uri": "text",
        "mbid": "text",
        "name": "citext",
        "duration_ms": "int",
        "album_spotify_uri": "text",
        "explicit": "bool",
        "disc_number": "int",
        "track_number": "int",
    },
}


def col_or_null(entity: str, col: str, csv_columns: dict, prefix: str = "src") -> str:
    """Return column reference if in CSV, otherwise NULL"""
    if col in csv_columns[entity]:
        return f"{prefix}.{col}"
    return "NULL"


class CSVLoader:
    def __init__(self, entity, csv_paths, csv_columns, policy, source_name="UNKNOWN"):
        self.entity = entity
        self.csv_path = pathlib.Path(csv_paths[entity])
        self.csv_columns = csv_columns
        self.policy = policy
        self.source_name = source_name
        self.timestamp = datetime.now(timezone.utc)
        # Generate merge functions dynamically
        self.merge_functions = self._generate_merge_functions()

    def _generate_merge_functions(self):
        """Generate merge functions for all entities using sql_templates"""
        from sql_templates import generate_merge_function

        merge_functions = {}
        for entity in self.csv_columns.keys():
            merge_functions[entity] = generate_merge_function(
                entity, self.csv_columns, self.policy
            )
        return merge_functions

    def get_merge_function(self, entity):
        """Get merge function for entity"""
        if entity not in self.merge_functions:
            raise ValueError(f"No merge function found for entity: {entity}")
        return self.merge_functions[entity]

    def build_staging_ddl(self, entity):
        """Build DDL for staging table based on CSV columns"""
        cols = []
        for col in self.csv_columns[entity]:
            # Use type from ALL_COLUMNS if available, otherwise default to text
            col_type = ALL_COLUMNS[entity].get(col, "text")
            cols.append(f"{col} {col_type}")
        return ", ".join(cols)

    def create_staging_indexes(self, conn, entity):
        """Create indexes on staging table to match main table performance characteristics"""
        staging_table = f"staging_{entity}"
        columns = self.csv_columns[entity]
        
        print(f"[DEBUG] Creating staging indexes for {staging_table} with columns: {columns}")
        
        # Create indexes based on available columns that match main table indexes
        if entity == "artists" and "spotify_uri" in columns:
            sql = f"CREATE INDEX IF NOT EXISTS idx_{staging_table}_spotify_uri ON {staging_table}(spotify_uri)"
            print(f"[DEBUG] Executing: {sql}")
            conn.execute(sql)
        if entity == "artists" and "mbid" in columns:
            sql = f"CREATE INDEX IF NOT EXISTS idx_{staging_table}_mbid ON {staging_table}(mbid)"
            print(f"[DEBUG] Executing: {sql}")
            conn.execute(sql)
        if entity == "artists" and "name" in columns:
            sql = f"CREATE INDEX IF NOT EXISTS idx_{staging_table}_name ON {staging_table}(name)"
            print(f"[DEBUG] Executing: {sql}")
            conn.execute(sql)
            
        if entity == "albums" and "spotify_uri" in columns:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{staging_table}_spotify_uri ON {staging_table}(spotify_uri)")
        if entity == "albums" and "mbid" in columns:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{staging_table}_mbid ON {staging_table}(mbid)")
        if entity == "albums" and "name" in columns:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{staging_table}_name ON {staging_table}(name)")
            
        if entity == "tracks" and "spotify_uri" in columns:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{staging_table}_spotify_uri ON {staging_table}(spotify_uri)")
        if entity == "tracks" and "mbid" in columns:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{staging_table}_mbid ON {staging_table}(mbid)")
        if entity == "tracks" and "name" in columns:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{staging_table}_name ON {staging_table}(name)")
        if entity == "tracks" and "album_spotify_uri" in columns:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{staging_table}_album_spotify_uri ON {staging_table}(album_spotify_uri)")
            
        # Add unique constraints on spotify_uri for data integrity
        if "spotify_uri" in columns:
            try:
                conn.execute(f"ALTER TABLE {staging_table} ADD CONSTRAINT unique_{staging_table}_spotify_uri UNIQUE (spotify_uri)")
            except Exception as e:
                if "already exists" not in str(e):
                    raise

    def load(self):
        """Main loading logic"""
        entity = self.entity
        merge_func = self.get_merge_function(entity)

        # Generate staging table name and columns dynamically
        staging_table = f"staging_{entity}"
        columns = self.csv_columns[entity]

        print(
            f"[INFO] Loading {entity} from {self.csv_path.name} (source: {self.source_name})"
        )

        t0 = time.time()
        pg_url = os.getenv("PG_URL") or os.getenv("DATABASE_URL")
        if not pg_url:
            sys.exit("Set PG_URL or DATABASE_URL in your .env")

        with psycopg.connect(pg_url, autocommit=False) as conn:
            # Drop and recreate staging table
            staging_ddl = self.build_staging_ddl(entity)
            conn.execute(f"DROP TABLE IF EXISTS {staging_table};") 
            conn.execute(f"CREATE UNLOGGED TABLE IF NOT EXISTS {staging_table} ({staging_ddl});")

            # Count before
            before_result = conn.execute(f"SELECT count(*) FROM {entity}").fetchone()
            before = before_result[0] if before_result else 0

            try:
                # COPY CSV data to staging first
                with conn.cursor() as cur:
                    col_list = ", ".join(columns)
                    with cur.copy(
                        f"COPY {staging_table} ({col_list}) FROM STDIN WITH CSV HEADER"
                    ) as copy:
                        with open(self.csv_path, "rb") as f:
                            while data := f.read(1048576):
                                copy.write(data)


                # Create indexes only if you want to look up data in staging
                self.create_staging_indexes(conn, entity)
                
                # Commit everything
                conn.commit()
                result = conn.execute(f"SELECT indexname FROM pg_indexes WHERE tablename = '{staging_table}'").fetchall()
                print(f"[DEBUG] Indexes created: {[r[0] for r in result]}")

                # Check staging results
                staging_result = conn.execute(f"SELECT count(*) FROM {staging_table}").fetchone()
                rows_in_staging = staging_result[0] if staging_result else 0
                print(f"[DEBUG] Copied {rows_in_staging:,} → {staging_table}")

                # Start transaction for merge with stats analysis
                conn.execute("BEGIN")
                
                try:
                    # Run merge with stats analysis (but don't auto-rollback)
                    from stats.dry_run_stats import analyze_staging_vs_main_with_merge
                    print("[DEBUG] Running merge with stats analysis...")
                    merge_sql = merge_func(self.source_name, self.timestamp.isoformat())
                    analyze_staging_vs_main_with_merge(conn, entity, self.csv_columns, self.policy, merge_sql, self.source_name)
                    
                    # User decides: commit or rollback
                    response = input("\nCommit merge? (y/N): ").strip().lower()
                    if response in ['y', 'yes']:
                        # Count after merge but before commit
                        after_result = conn.execute(f"SELECT count(*) FROM {entity}").fetchone()
                        after = after_result[0] if after_result else 0
                        
                        conn.execute("COMMIT")
                        elapsed = time.time() - t0
                        print("✓ Merge committed!")
                        print(
                            f"✓ {self.csv_path.name}: +{after-before:,} rows | {elapsed:.1f}s | source '{self.source_name}'"
                        )
                    else:
                        conn.execute("ROLLBACK")
                        elapsed = time.time() - t0
                        print("✗ Merge rolled back.")
                        print(f"✓ {self.csv_path.name}: staging loaded, merge cancelled | {elapsed:.1f}s")
                        return
                        
                except Exception as e:
                    conn.execute("ROLLBACK")
                    print(f"[ERROR] Merge failed and rolled back: {type(e).__name__}: {e}")
                    raise

            except Exception as e:
                print(f"[ERROR] COPY failed: {type(e).__name__}: {e}")
                raise


if __name__ == "__main__":
    from loaders import one_mil_songs_loader as loader

    entity = "tracks"

    csv_loader = CSVLoader(
        entity=entity,
        csv_paths=loader.CSV_PATHS,
        csv_columns=loader.CSV_COLUMNS,
        policy=loader.POLICY,
        source_name=loader.SOURCE_NAME,
    )
    csv_loader.load()
