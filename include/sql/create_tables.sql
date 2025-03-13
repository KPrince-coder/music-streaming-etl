-- Create tables for both user and song metadata
-------------------------------------------
-- USER METADATA
-------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    user_name TEXT,
    user_age INT,
    user_country TEXT,
    created_at DATE
);
-------------------------------------------
-- SONG METADATA
-------------------------------------------
CREATE TABLE IF NOT EXISTS songs (
    id INTEGER PRIMARY KEY,
    track_id TEXT,
    artists TEXT,
    album_name TEXT,
    track_name TEXT,
    popularity INTEGER,
    duration_ms INTEGER,
    explicit BOOLEAN,
    danceability FLOAT,
    energy FLOAT,
    song_key INTEGER,
    loudness FLOAT,
    mode INTEGER,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    time_signature INTEGER,
    track_genre TEXT
);