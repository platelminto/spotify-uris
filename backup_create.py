import subprocess
from datetime import datetime

DB_NAME = "music"
PG_USER = "postgres"
PG_PASSWORD = "pw"
PG_HOST = "localhost"
DEFAULT_BACKUP_NAME = f"music-data-{datetime.now():%Y%m%d-%H%M%S}"

def create_backup(backup_name=DEFAULT_BACKUP_NAME):
    backup_file = f"backups/{backup_name}.backup"
    print(f"Creating backup of database '{DB_NAME}' → {backup_file}...")
    result = subprocess.run([
        "pg_dump",
        "-h", PG_HOST,
        "-U", PG_USER,
        "-F", "c",  # custom format for pg_restore
        "-f", backup_file,
        DB_NAME
    ], env={"PGPASSWORD": PG_PASSWORD})
    if result.returncode == 0:
        print("✅ Backup complete.")
    else:
        print(f"⚠️ Backup failed with exit code {result.returncode}")


if __name__ == "__main__":
    backup_name = "pre-6.6mil"
    create_backup(backup_name)
