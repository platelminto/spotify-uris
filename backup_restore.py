import subprocess
import psycopg
import time

# Ignore the pg_restore error saying database does not exist
# because it will be created during the restore process.

DB_NAME = "music"
PG_USER = "postgres"
PG_PASSWORD = "pw"
PG_HOST = "localhost"
BACKUP_FILE = "backups/music-data-20250714-031843.backup"

def terminate_connections(dbname):
    print(f"Terminating active connections to '{dbname}'...")
    with psycopg.connect(
        dbname="postgres", user=PG_USER, password=PG_PASSWORD, host=PG_HOST
    ) as conn:
        conn.execute(f"""
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = %s AND pid <> pg_backend_pid()
        """, (dbname,))
        conn.commit()

def drop_database(dbname):
    print(f"Dropping database '{dbname}' if it exists...")
    with psycopg.connect(
        dbname="postgres", user=PG_USER, password=PG_PASSWORD, host=PG_HOST, autocommit=True
    ) as conn:
        conn.execute(f"DROP DATABASE IF EXISTS {dbname}")

def restore_backup():
    print(f"Restoring from {BACKUP_FILE}...")
    result = subprocess.run([
        "pg_restore",
        "-h", PG_HOST,
        "-U", PG_USER,
        "--create",
        "--clean",
        "-d", "postgres",
        BACKUP_FILE
    ], env={"PGPASSWORD": PG_PASSWORD})
    if result.returncode == 0:
        print("✅ Restore complete.")
    else:
        print(f"⚠️ Restore failed with exit code {result.returncode}")

if __name__ == "__main__":
    terminate_connections(DB_NAME)
    time.sleep(1)  # Wait a sec for connection cleanup
    drop_database(DB_NAME)
    restore_backup()
