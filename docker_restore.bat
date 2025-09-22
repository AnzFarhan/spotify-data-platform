@echo off
echo 📥 Restore Spotify Data
echo =======================

dir backup_*.sql

set /p backup_file="Enter backup filename: "

if not exist "%backup_file%" (
    echo ❌ Backup file not found!
    pause
    exit /b 1
)

echo Restoring from %backup_file%...
docker-compose exec -T postgres psql -U postgres spotify_data < %backup_file%

echo ✅ Restore complete!
pause