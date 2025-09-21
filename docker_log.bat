@echo off
echo 📋 Docker Logs - Spotify Data Platform
echo =====================================

echo Choose service logs to view:
echo 1. All services
echo 2. Application only
echo 3. Database only
echo 4. Redis only

set /p choice="Enter choice (1-4): "

if "%choice%"=="1" docker-compose logs -f
if "%choice%"=="2" docker-compose logs -f spotify_app
if "%choice%"=="3" docker-compose logs -f postgres
if "%choice%"=="4" docker-compose logs -f redis

pause