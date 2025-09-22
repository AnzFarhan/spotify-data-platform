@echo off
echo 🚀 Running Spotify ETL Pipeline in Docker
echo ============================================

REM Start all services
echo 📦 Starting all Docker services...
docker-compose up -d

echo ⏳ Waiting for services to be ready...
timeout /t 5 /nobreak >nul

echo 🧪 Running connection tests...
docker-compose exec spotify_app python tests/test_day2.py

echo 🔄 Running ETL Pipeline...
docker-compose exec spotify_app python scripts/run_etl_pipeline.py --limit 20

echo.
echo 📊 Checking database contents...
docker-compose exec postgres psql -U postgres -d spotify_data -c "SELECT COUNT(*) as total_tracks FROM tracks;"
docker-compose exec postgres psql -U postgres -d spotify_data -c "SELECT COUNT(*) as listening_history FROM listening_history;"

echo.
echo ✅ Pipeline execution complete!
echo 📋 To see logs: docker-compose logs spotify_app
echo 🛑 To stop: docker_stop.bat
pause