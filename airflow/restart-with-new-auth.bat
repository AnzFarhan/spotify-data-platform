@echo off
echo Stopping Airflow containers...
docker compose down

echo Waiting for containers to stop...
timeout /t 5

echo Starting Airflow with new authentication settings...
docker compose up airflow-init

echo Waiting for initialization...
timeout /t 10

echo Starting all Airflow services...
docker compose up -d

echo.
echo ===============================================
echo Airflow Authentication Updated!
echo ===============================================
echo.
echo New Login Credentials:
echo Username: admin
echo Password: admin01
echo.
echo Web UI: http://localhost:8080
echo.
echo Waiting for services to be ready...
timeout /t 20

echo Checking container status...
docker compose ps