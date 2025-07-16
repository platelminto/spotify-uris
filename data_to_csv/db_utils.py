import psycopg
from dotenv import load_dotenv
import os
import pandas as pd
import re

load_dotenv()


def get_db_connection() -> psycopg.Connection:
    """Get database connection using environment variables"""
    return psycopg.connect(os.environ["DATABASE_URL"])


def get_artist_uri(artist_name: str, cursor: psycopg.Cursor) -> str | None:
    """Get artist URI from database using exact name match"""
    cursor.execute("SELECT spotify_uri FROM artists WHERE name = %s", (artist_name,))
    result = cursor.fetchone()
    return result[0] if result else None


def get_artist_uris_batch(artist_names: list[str], cursor: psycopg.Cursor) -> dict[str, str]:
    """Get artist URIs for a list of artist names. Returns dict with URI as key, name as value"""
    if not artist_names:
        return {}
    
    cursor.execute(
        "SELECT name, spotify_uri FROM artists WHERE name = ANY(%s)",
        (list(artist_names),)
    )
    # Swap key-value to use URI as key
    return {uri: name for name, uri in cursor.fetchall() if uri is not None}


def convert_json_array_to_postgres_array(val):
    if pd.isna(val) or val == '[]':
        return '{}'
    # Remove outer brackets and convert to PostgreSQL format
    val = str(val)
    if val.startswith('[') and val.endswith(']'):
        # Extract content between brackets
        content = val[1:-1]
        # Split by comma and clean up quotes
        items = re.split(r',\s*', content)
        cleaned_items = []
        for item in items:
            # Remove surrounding quotes (both ' and ")
            item = item.strip()
            if (item.startswith("'") and item.endswith("'")) or (item.startswith('"') and item.endswith('"')):
                item = item[1:-1]
            cleaned_items.append(item)
        return '{' + ','.join(cleaned_items) + '}'
    return val


def parse_release_date(date_str):
    """Parse release date and return (formatted_date, precision)"""
    if pd.isna(date_str) or not date_str or date_str == "0000":
        return None, None
    
    date_str = str(date_str).strip()
    
    # Full date format: YYYY-MM-DD
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return date_str, 'day'
    
    # Year-month format: YYYY-MM
    if re.match(r'^\d{4}-\d{2}$', date_str):
        return f"{date_str}-01", 'month'
    
    # Year only: YYYY
    if re.match(r'^\d{4}$', date_str):
        return f"{date_str}-01-01", 'year'
    
    return None, None