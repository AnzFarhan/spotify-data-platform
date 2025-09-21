@echo off
echo 🧹 Cleaning up Docker resources
echo ===============================

echo Stopping all containers...
docker-compose down

echo Removing containers and networks...
docker-compose down --remove-orphans

echo Cleaning up unused Docker resources...
docker system prune -f

echo.
echo ⚠️ To completely reset (including data):
echo   docker-compose down -v
echo   docker system prune -a -f
echo.
echo ✅ Cleanup complete!
pause