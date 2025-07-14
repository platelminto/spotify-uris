# Claude Memory for Spotify URIs Project

## Database Connection
- Command to run SQL queries: `psql postgresql://postgres:pw@localhost:5432/music -c "SQL_QUERY"`
- Feel free to run SQL queries to test and debug issues, but nothing destructive unless explicitly told. Only SELECTs and all that.
- Use psycopg (version 3), not psycopg2 

## Project Structure
- `load_csv_engine.py`: Generic CSV loader with staging analysis
- `staging_stats.py`: Analyzes staging vs main table differences
- `mpd_loader.py`: Configuration for MPD dataset loading
- Entity tables: artists, albums, tracks
- Association tables: album_artists, track_artists
- For an idea of the db structure make sure to read `models.py` 