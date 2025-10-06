-- Spotify Data Platform Database Schema
-- Created: Day 2

-- Artists table (master data)
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    genres TEXT[],
    popularity INTEGER,
    followers INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Albums table 
CREATE TABLE IF NOT EXISTS albums (
    album_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    artist_id VARCHAR(50) REFERENCES artists(artist_id),
    release_date DATE,
    total_tracks INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tracks table
CREATE TABLE IF NOT EXISTS tracks (
    track_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    album_id VARCHAR(50) REFERENCES albums(album_id),
    artist_id VARCHAR(50) REFERENCES artists(artist_id),
    duration_ms INTEGER,
    explicit BOOLEAN,
    popularity INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Audio features table (from Spotify API)
CREATE TABLE IF NOT EXISTS audio_features (
    track_id VARCHAR(50) PRIMARY KEY REFERENCES tracks(track_id),
    danceability FLOAT,
    energy FLOAT,
    key INTEGER,
    loudness FLOAT,
    mode INTEGER,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    time_signature INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Listening history (main fact table)
CREATE TABLE IF NOT EXISTS listening_history (
    id SERIAL PRIMARY KEY,
    track_id VARCHAR(50) REFERENCES tracks(track_id),
    played_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_listening_history_played_at 
ON listening_history(played_at);

CREATE INDEX IF NOT EXISTS idx_listening_history_track_id 
ON listening_history(track_id);