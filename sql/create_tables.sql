-- Create tables for RDS PostgreSQL database
-- Create users table
-- This table stores user demographic and account information
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    user_name TEXT,
    user_age INTEGER,
    user_country TEXT,
    created_at DATE
);
-- Add index on user_country for geographic analysis
CREATE INDEX idx_users_country ON users(user_country);
-- Create songs table
-- This table stores detailed song metadata including audio features and track information
CREATE TABLE IF NOT EXISTS songs (
    id INTEGER PRIMARY KEY,
    track_id TEXT,
    artists TEXT,
    album_name TEXT,
    track_name TEXT,
    popularity INTEGER,
    -- Popularity score (0-100)
    duration_ms INTEGER,
    -- Duration in milliseconds
    explicit BOOLEAN,
    -- Whether the track has explicit content
    danceability FLOAT,
    -- Danceability score (0.0-1.0)
    energy FLOAT,
    -- Energy score (0.0-1.0)
    song_key INTEGER,
    -- Musical key of the track
    loudness FLOAT,
    -- Overall loudness in decibels
    mode INTEGER,
    -- Modality of the track (0 = minor, 1 = major)
    speechiness FLOAT,
    -- Presence of spoken words (0.0-1.0)
    acousticness FLOAT,
    -- Amount of acoustic sound (0.0-1.0)
    instrumentalness FLOAT,
    -- Amount of instrumental content (0.0-1.0)
    liveness FLOAT,
    -- Presence of live performance elements (0.0-1.0)
    valence FLOAT,
    -- Musical positiveness (0.0-1.0)
    tempo FLOAT,
    -- Estimated tempo in BPM
    time_signature INTEGER,
    -- Estimated time signature
    track_genre TEXT -- Primary genre of the track
);
-- Add indexes for common query patterns
CREATE INDEX idx_songs_track_id ON songs(track_id);
CREATE INDEX idx_songs_genre ON songs(track_genre);
CREATE INDEX idx_songs_popularity ON songs(popularity);
-- Add helpful comments to the tables
COMMENT ON TABLE users IS 'User demographic and account information';
COMMENT ON TABLE songs IS 'Detailed song metadata including audio features and track information';
-- Add column comments for users table
COMMENT ON COLUMN users.user_id IS 'Unique identifier for each user';
COMMENT ON COLUMN users.user_name IS 'Name of the user';
COMMENT ON COLUMN users.user_age IS 'Age of the user';
COMMENT ON COLUMN users.user_country IS 'Country where the user is located';
COMMENT ON COLUMN users.created_at IS 'Date when the user account was created';
-- Add column comments for songs table
COMMENT ON COLUMN songs.id IS 'Primary key for the songs table';
COMMENT ON COLUMN songs.track_id IS 'Unique identifier for the track';
COMMENT ON COLUMN songs.artists IS 'Name(s) of the artist(s)';
COMMENT ON COLUMN songs.album_name IS 'Name of the album';
COMMENT ON COLUMN songs.track_name IS 'Name of the track';
COMMENT ON COLUMN songs.popularity IS 'Popularity score from 0-100';
COMMENT ON COLUMN songs.duration_ms IS 'Duration of the track in milliseconds';
COMMENT ON COLUMN songs.explicit IS 'Whether the track has explicit content';
COMMENT ON COLUMN songs.danceability IS 'How suitable the track is for dancing (0.0-1.0)';
COMMENT ON COLUMN songs.energy IS 'Perceptual measure of intensity and activity (0.0-1.0)';
COMMENT ON COLUMN songs.song_key IS 'The key the track is in';
COMMENT ON COLUMN songs.loudness IS 'Overall loudness in decibels';
COMMENT ON COLUMN songs.mode IS 'Modality of the track (0 = minor, 1 = major)';
COMMENT ON COLUMN songs.speechiness IS 'Presence of spoken words (0.0-1.0)';
COMMENT ON COLUMN songs.acousticness IS 'Amount of acoustic sound (0.0-1.0)';
COMMENT ON COLUMN songs.instrumentalness IS 'Amount of instrumental content (0.0-1.0)';
COMMENT ON COLUMN songs.liveness IS 'Presence of live performance elements (0.0-1.0)';
COMMENT ON COLUMN songs.valence IS 'Musical positiveness (0.0-1.0)';
COMMENT ON COLUMN songs.tempo IS 'Estimated tempo in beats per minute';
COMMENT ON COLUMN songs.time_signature IS 'Estimated time signature';
COMMENT ON COLUMN songs.track_genre IS 'Primary genre of the track';