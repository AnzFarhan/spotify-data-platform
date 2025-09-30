@echo off
echo.
echo ========================================
echo   🎵 Spotify Data Platform - Airflow
echo ========================================
echo.

cd /d "%~dp0"

echo [1/5] Creating Airflow directories...
if not exist "logs" mkdir logs
if not exist "plugins" mkdir plugins

echo [2/5] Setting up Docker environment...
echo Copying environment file...
if exist "..\\.env" (
    copy "..\\.env" ".env" >nul 2>&1
    echo ✅ Environment file copied successfully!
) else (
    echo ❌ Warning: Parent .env file not found. Please ensure .env exists in parent directory.
)

echo [3/5] Making scripts executable...
echo Scripts are ready for Docker execution.

echo [4/5] Ready to initialize Airflow...
echo.
echo ========================================
echo   🚀 Getting Started Instructions
echo ========================================
echo.
echo 1. Initialize Airflow (run once):
echo    docker-compose up airflow-init
echo.
echo 2. Start all services:
echo    docker-compose up -d
echo.
echo 3. Access services:
echo    • Airflow Web UI: http://localhost:8080
echo      Username: your_username
echo      Password: your_password
echo.
echo    • Flower (Worker monitoring): http://localhost:5555
echo    • PostgreSQL: localhost:5432
echo.
echo 4. Monitor your pipeline:
echo    • View DAG: your_dag_name
echo    • Check logs in Airflow UI
echo    • Monitor tasks in Graph View
echo.
echo 5. Useful commands:
echo    docker-compose ps           (check services)
echo    docker-compose logs airflow-scheduler
echo    docker-compose logs airflow-webserver
echo    docker-compose down         (stop services)
echo.
echo ========================================
echo Ready! Run the commands above to start.
echo ========================================
echo.
pause