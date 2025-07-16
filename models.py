import enum, os
from datetime import datetime
from sqlalchemy import (Column, String, Integer, Boolean, Date, Enum, ForeignKey,
                        Table, DateTime, CheckConstraint, create_engine, text, ARRAY)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import declarative_base, relationship
from dotenv import load_dotenv

load_dotenv()
Base = declarative_base()

class AlbumType(str, enum.Enum):
    album = "album"; single = "single"; compilation = "compilation"
    ep = "ep"; other = "other"

class DatePrecision(str, enum.Enum):
    year = "year"; month = "month"; day = "day"

# — link tables —

album_artists = Table(
    "album_artists", Base.metadata,
    Column("album_id",  ForeignKey("albums.id",  ondelete="CASCADE"), primary_key=True),
    Column("artist_id", ForeignKey("artists.id", ondelete="CASCADE"), primary_key=True),
    Column("position",  Integer, nullable=False),
)

track_artists = Table(
    "track_artists", Base.metadata,
    Column("track_id",  ForeignKey("tracks.id",  ondelete="CASCADE"), primary_key=True),
    Column("artist_id", ForeignKey("artists.id", ondelete="CASCADE"), primary_key=True),
    Column("position",  Integer, nullable=False),
)

class Artist(Base):
    __tablename__ = "artists"
    id         = Column(Integer, primary_key=True)
    mbid       = Column(UUID,   unique=True, nullable=True)
    spotify_uri = Column(String, unique=True, nullable=True)
    name       = Column(String, nullable=True)                  # -> CITEXT later, NULL allowed for auto-created artists
    genres     = Column(ARRAY(String), nullable=True)          # Simple array of genre strings
    source_name = Column(String)               # last feed that touched the row
    ingested_at = Column(DateTime)             # when it happened
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    __table_args__ = (
        CheckConstraint("(mbid IS NOT NULL) OR (spotify_uri IS NOT NULL)", name="artist_has_id"),
    )

class Album(Base):
    __tablename__ = "albums"
    id         = Column(Integer, primary_key=True)
    mbid       = Column(UUID,   unique=True, nullable=True)
    spotify_uri = Column(String, unique=True, nullable=True)
    name       = Column(String, nullable=True)                  # -> CITEXT later, NULL allowed for auto-created albums
    album_type = Column(Enum(AlbumType), nullable=True)
    source_name = Column(String)               # last feed that touched the row
    ingested_at = Column(DateTime)             # when it happened
    spotify_release_date   = Column(Date, nullable=True)
    release_date_precision = Column(Enum(DatePrecision), nullable=True)
    n_tracks   = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    artists    = relationship("Artist", secondary=album_artists, backref="albums")
    __table_args__ = (
        CheckConstraint("(mbid IS NOT NULL) OR (spotify_uri IS NOT NULL)", name="album_has_id"),
    )

class Track(Base):
    __tablename__ = "tracks"
    id         = Column(Integer, primary_key=True)
    mbid       = Column(UUID,   unique=True, nullable=True)
    spotify_uri = Column(String, unique=True, nullable=True)
    name       = Column(String, nullable=True)                  # -> CITEXT later, NULL allowed for auto-created tracks
    duration_ms  = Column(Integer, nullable=True)
    explicit     = Column(Boolean, nullable=True)
    isrc         = Column(String, nullable=True)
    source_name = Column(String)               # last feed that touched the row
    ingested_at = Column(DateTime)             # when it happened
    disc_number  = Column(Integer, nullable=True)
    track_number = Column(Integer, nullable=True)
    album_id     = Column(Integer, ForeignKey("albums.id", ondelete="SET NULL"), nullable=True)
    created_at   = Column(DateTime, default=datetime.utcnow)
    updated_at   = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    album   = relationship("Album", backref="tracks")
    artists = relationship("Artist", secondary=track_artists, backref="tracks")
    __table_args__ = (
        CheckConstraint("(mbid IS NOT NULL) OR (spotify_uri IS NOT NULL)", name="track_has_id"),
    )

def init_db():
    eng = create_engine(os.environ["SEQUEL_ALCHEMY_URL"], future=True)
    Base.metadata.create_all(eng)

    # one-time: enable citext & convert name columns
    with eng.begin() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS citext"))
        for tbl in ("artists", "albums", "tracks"):
            conn.execute(text(f"""
                ALTER TABLE {tbl}
                ALTER COLUMN name TYPE CITEXT
                USING name::citext;
            """))

        # Join indexes (tiny, but make look-ups fast)
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_album_artists_album  ON album_artists(album_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_album_artists_artist ON album_artists(artist_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_track_artists_track  ON track_artists(track_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_track_artists_artist ON track_artists(artist_id)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tracks_album        ON tracks(album_id)"))
        
        # Name indexes for performance on name-based queries and comparisons
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_artists_name ON artists(name)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_albums_name  ON albums(name)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_tracks_name  ON tracks(name)"))
        
        # Genre array index for fast queries
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_artists_genres ON artists USING GIN(genres)"))
