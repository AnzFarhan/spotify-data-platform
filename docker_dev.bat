@echo off
echo 🔧 Development Mode - Spotify Data Platform
echo ===========================================

REM Start services in development mode
docker-compose up -d postgres redis

echo ⏳ Waiting for database...
timeout /t 5 /nobreak >nul

echo 🗄️ Setting up database if needed...
docker-compose exec postgres createdb -U postgres spotify_data 2>nul || echo "Database already exists"

echo 🏗️ Creating database schema...
docker-compose run --rm spotify_app python scripts/setup_database.py

echo 💻 Starting interactive development container...
docker-compose run --rm spotify_app bash

pause