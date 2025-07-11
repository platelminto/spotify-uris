#!/usr/bin/env python
"""
load_csv_engine.py - Generic CSV → PostgreSQL loader engine

Usage: python load_csv_engine.py --config mpd_loader --file csvs/mpd/artists.csv
"""

import os, sys, pathlib, time, psycopg, importlib, argparse
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# Default conflict resolution policy - covers most standard cases
DEFAULT_POLICY = {
    "artists": {
        "name": "prefer_incoming",
        "mbid": "prefer_non_null",
        "spotify_uri": "prefer_non_null",
        "genres": "prefer_incoming",
    },
    "albums": {
        "name": "prefer_incoming",
        "album_type": "prefer_non_null",
        "spotify_release_date": "prefer_non_null",
        "release_date_precision": "prefer_non_null",
        "n_tracks": "prefer_non_null",
        "mbid": "prefer_non_null",
        "spotify_uri": "prefer_non_null",
    },
    "tracks": {
        "name": "prefer_incoming",
        "duration_ms": "prefer_non_null",
        "explicit": "prefer_non_null",
        "disc_number": "prefer_non_null",
        "track_number": "prefer_non_null",
        "album_id": "prefer_non_null",
        "mbid": "prefer_non_null",
        "spotify_uri": "prefer_non_null",
    },
}


# Helper functions moved back to individual loaders for simplicity


class CSVLoader:
    def __init__(self, config_module, csv_path, source_name=None):
        self.config = config_module
        self.csv_path = pathlib.Path(csv_path)
        self.source_name = source_name or getattr(
            config_module, "DEFAULT_SOURCE", "UNKNOWN"
        )
        self.timestamp = datetime.now(timezone.utc)

    def get_entity_type(self):
        """Determine entity type from filename"""
        filename = self.csv_path.name
        for entity in self.config.CSV_COLUMNS.keys():
            if filename.startswith(entity):
                return entity
        raise ValueError(f"Cannot determine entity type from filename: {filename}")

    def get_config_for_entity(self, entity):
        """Get all configuration for a specific entity"""
        if entity not in self.config.SETTINGS:
            raise ValueError(f"No configuration found for entity: {entity}")
        return self.config.SETTINGS[entity]

    def build_staging_ddl(self, entity):
        """Build DDL for staging table based on CSV columns"""
        cols = []
        for col in self.config.CSV_COLUMNS[entity]:
            if (
                hasattr(self.config, "STAGING_OVERRIDES")
                and col in self.config.STAGING_OVERRIDES
            ):
                # Handle special staging types (like list fields as text)
                cols.append(f"{col} {self.config.STAGING_OVERRIDES[col]}")
            else:
                col_type = self.config.ALL_COLUMNS[entity].get(col, "text")
                cols.append(f"{col} {col_type}")
        return ", ".join(cols)

    def debug_csv_preview(self):
        """Show first few lines of CSV for debugging"""
        with open(self.csv_path, "r") as f:
            for i in range(5):
                line = f.readline().strip()
                print(f"[DEBUG] Line {i}: {line}")

    def load(self):
        """Main loading logic"""
        entity = self.get_entity_type()
        cfg = self.get_config_for_entity(entity)

        print(
            f"[INFO] Loading {entity} from {self.csv_path.name} (source: {self.source_name})"
        )

        t0 = time.time()
        pg_url = os.getenv("PG_URL") or os.getenv("DATABASE_URL")
        if not pg_url:
            sys.exit("Set PG_URL or DATABASE_URL in your .env")

        self.debug_csv_preview()

        with psycopg.connect(pg_url, autocommit=False) as conn:
            # Drop and recreate staging table
            staging_ddl = self.build_staging_ddl(entity)
            conn.execute(f"DROP TABLE IF EXISTS {cfg['staging']};")  # type: ignore
            conn.execute(f"CREATE UNLOGGED TABLE IF NOT EXISTS {cfg['staging']} ({staging_ddl});")  # type: ignore

            # Count before
            before_result = conn.execute(f"SELECT count(*) FROM {entity}").fetchone()  # type: ignore
            before = before_result[0] if before_result else 0

            try:
                # COPY CSV data to staging
                with conn.cursor() as cur:
                    col_list = ", ".join(cfg["columns"])
                    with cur.copy(
                        f"COPY {cfg['staging']} ({col_list}) FROM STDIN WITH CSV HEADER"  # type: ignore
                    ) as copy:
                        with open(self.csv_path, "rb") as f:
                            while data := f.read(8192):
                                copy.write(data)

                # Check staging results
                staging_result = conn.execute(f"SELECT count(*) FROM {cfg['staging']}").fetchone()  # type: ignore
                rows_in_staging = staging_result[0] if staging_result else 0
                print(f"[DEBUG] Copied {rows_in_staging:,} → {cfg['staging']}")

            except Exception as e:
                print(f"[ERROR] COPY failed: {type(e).__name__}: {e}")
                raise

            # Execute merge SQL
            merge_sql = cfg["merge_func"](self.source_name, self.timestamp.isoformat())
            conn.execute(merge_sql)  # type: ignore

            # Count after
            after_result = conn.execute(f"SELECT count(*) FROM {entity}").fetchone()  # type: ignore
            after = after_result[0] if after_result else 0

            conn.commit()
            elapsed = time.time() - t0
            print(
                f"✓ {self.csv_path.name}: +{after-before:,} rows | {elapsed:.1f}s | source '{self.source_name}'"
            )


def main():
    parser = argparse.ArgumentParser(description="Load CSV data into PostgreSQL")
    parser.add_argument(
        "--config", required=True, help="Config module name (e.g., mpd_loader)"
    )
    parser.add_argument("--file", required=True, help="Path to CSV file")
    parser.add_argument("--source", help="Source name override")

    args = parser.parse_args()

    # Import the configuration module
    try:
        config_module = importlib.import_module(args.config)
    except ImportError:
        sys.exit(f"Could not import config module: {args.config}")

    # Create and run loader
    loader = CSVLoader(config_module, args.file, args.source)
    loader.load()


if __name__ == "__main__":
    main()
