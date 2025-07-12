import subprocess
from datetime import datetime

DB_NAME = "music"
PG_USER = "postgres"
PG_PASSWORD = "pw"
PG_HOST = "localhost"
BACKUP_FILE = f"backups/music-data-{datetime.now():%Y%m%d-%H%M%S}.backup"

def create_backup():
    print(f"Creating backup of database '{DB_NAME}' → {BACKUP_FILE}...")
    result = subprocess.run([
        "pg_dump",
        "-h", PG_HOST,
        "-U", PG_USER,
        "-F", "c",  # custom format for pg_restore
        "-f", BACKUP_FILE,
        DB_NAME
    ], env={"PGPASSWORD": PG_PASSWORD})
    if result.returncode == 0:
        print("✅ Backup complete.")
    else:
        print(f"⚠️ Backup failed with exit code {result.returncode}")

if __name__ == "__main__":
    create_backup()
