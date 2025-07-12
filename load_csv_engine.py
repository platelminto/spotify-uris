#!/usr/bin/env python
"""
load_csv_engine.py - Generic CSV → PostgreSQL loader engine

Usage: python load_csv_engine.py --config mpd_loader --file csvs/mpd/artists.csv
"""

import os, sys, pathlib, time, psycopg, importlib
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()


# Define all possible columns for each entity (for the database tables)
ALL_COLUMNS = {
    "artists": {"spotify_uri": "text", "mbid": "text", "name": "citext"},
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


# Helper functions for merge SQL generation


def get_policy(entity, csv_columns, config_policy):
    """Get policy for entity from config"""
    if not config_policy or entity not in config_policy:
        raise ValueError(f"No policy defined for entity: {entity}")

    # Only keep policies for columns that exist in this CSV
    entity_csv_columns = csv_columns.get(entity, [])
    entity_policy = config_policy[entity]
    return {
        col: policy
        for col, policy in entity_policy.items()
        if col in entity_csv_columns
    }


def build_set(
    entity: str,
    cols: list[str],
    source: str,
    timestamp: str,
    csv_columns: dict,
    config_policy: dict,
) -> str:
    """Generate SET clause obeying policy."""
    policy = get_policy(entity, csv_columns, config_policy)
    parts = []
    csv_cols = csv_columns[entity]
    for col in cols:
        if col in csv_cols:  # Only if column exists in CSV
            mode = policy.get(col, "prefer_incoming")
            if mode == "prefer_incoming":
                parts.append(f"{col}=EXCLUDED.{col}")
            elif mode == "prefer_non_null":
                parts.append(f"{col}=COALESCE({entity}.{col},EXCLUDED.{col})")
            elif mode == "prefer_longer":
                parts.append(
                    f"{col}=CASE WHEN length(EXCLUDED.{col})>length({entity}.{col}) "
                    f"THEN EXCLUDED.{col} ELSE {entity}.{col} END"
                )
    parts.append(f"source_name='{source}'")
    parts.append(f"ingested_at='{timestamp}'")
    return ", ".join(parts)


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
            conn.execute(f"DROP TABLE IF EXISTS {staging_table};")  # type: ignore
            conn.execute(f"CREATE UNLOGGED TABLE IF NOT EXISTS {staging_table} ({staging_ddl});")  # type: ignore

            # Count before
            before_result = conn.execute(f"SELECT count(*) FROM {entity}").fetchone()  # type: ignore
            before = before_result[0] if before_result else 0

            try:
                # COPY CSV data to staging
                with conn.cursor() as cur:
                    col_list = ", ".join(columns)
                    with cur.copy(
                        f"COPY {staging_table} ({col_list}) FROM STDIN WITH CSV HEADER"  # type: ignore
                    ) as copy:
                        with open(self.csv_path, "rb") as f:
                            while data := f.read(8192):
                                copy.write(data)

                # Check staging results
                staging_result = conn.execute(f"SELECT count(*) FROM {staging_table}").fetchone()  # type: ignore
                rows_in_staging = staging_result[0] if staging_result else 0
                print(f"[DEBUG] Copied {rows_in_staging:,} → {staging_table}")

                # Analyze staging vs main before merge
                from staging_stats import analyze_staging_vs_main
                analyze_staging_vs_main(conn, entity, self.csv_columns)
                
                # Confirm before proceeding with merge
                response = input("\nProceed with merge? (y/N): ").strip().lower()
                if response not in ['y', 'yes']:
                    print("Merge cancelled.")
                    return

            except Exception as e:
                print(f"[ERROR] COPY failed: {type(e).__name__}: {e}")
                raise

            # Execute merge SQL (can be single statement or list)
            merge_sql = merge_func(self.source_name, self.timestamp.isoformat())

            if isinstance(merge_sql, list):
                # Execute multiple statements in order
                for sql in merge_sql:
                    conn.execute(sql)  # type: ignore
            else:
                # Single statement (backwards compatible)
                conn.execute(merge_sql)  # type: ignore

            # Count after
            after_result = conn.execute(f"SELECT count(*) FROM {entity}").fetchone()  # type: ignore
            after = after_result[0] if after_result else 0

            conn.commit()
            elapsed = time.time() - t0
            print(
                f"✓ {self.csv_path.name}: +{after-before:,} rows | {elapsed:.1f}s | source '{self.source_name}'"
            )


if __name__ == "__main__":
    from csv_to_db import mpd_loader

    entity = "tracks"

    csv_loader = CSVLoader(
        entity=entity,
        csv_paths=mpd_loader.CSV_PATHS,
        csv_columns=mpd_loader.CSV_COLUMNS,
        policy=mpd_loader.POLICY,
        source_name=mpd_loader.SOURCE_NAME,
    )
    csv_loader.load()
