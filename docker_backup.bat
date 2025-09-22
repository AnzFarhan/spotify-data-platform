@echo off
echo 💾 Backup Spotify Data
echo ======================

set backup_date=%date:~-4,4%-%date:~-10,2%-%date:~-7,2%_%time:~0,2%-%time:~3,2%-%time:~6,2%
set backup_date=%backup_date: =0%

echo Creating backup: backup_%backup_date%.sql

docker-compose exec postgres pg_dump -U postgres spotify_data > backup_%backup_date%.sql

echo ✅ Backup created: backup_%backup_date%.sql
pause