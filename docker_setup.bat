@echo off
echo 🐳 Spotify Data Platform - Docker Setup
echo ================================================

REM Check if Docker is running
docker info >nul 2>&1
if %errorlevel% neq 0 (
    echo ❌ Docker is not running. Please start Docker Desktop first.
    pause
    exit /b 1
)

echo ✅ Docker is running

REM Check if .env file exists
if not exist .env (
    echo ⚠️ Creating .env file from example...
    copy .env.example .env
    echo 📝 Please edit .env with your Spotify API credentials
    pause
)

echo 📦 Building Docker containers...
docker-compose build

echo 🚀 Starting services...
docker-compose up -d postgres redis

echo ⏳ Waiting for database to be ready...
timeout /t 10 /nobreak >nul

echo 🗄️ Setting up database schema...
docker-compose exec postgres psql -U postgres -d spotify_data -c "\dt"

echo ✅ Docker setup complete!
echo.
echo 🎯 Next steps:
echo   1. Edit .env with your Spotify credentials
echo   2. Run: docker_run.bat
echo.
pause