#!/usr/bin/env bash

# Airflow Entrypoint Script
# This script prepares the Airflow environment before starting services

echo "🚀 Initializing Airflow environment..."

# Install additional Python packages from requirements.txt
if [[ -f "/requirements.txt" ]]; then
    echo "📦 Installing additional packages from requirements.txt..."
    pip install --no-cache-dir -r /requirements.txt
    echo "✅ Additional packages installed successfully!"
fi

# Set PYTHONPATH to include the project modules
export PYTHONPATH="${PYTHONPATH}:/opt/airflow:/opt/airflow/DE:/opt/airflow/config"

# Set up environment variables from .env file if it exists
if [[ -f "/opt/airflow/.env" ]]; then
    echo "🔧 Loading environment variables from .env file..."
    set -a
    source /opt/airflow/.env
    set +a
    echo "✅ Environment variables loaded!"
fi

# Create necessary directories if they don't exist
mkdir -p /opt/airflow/logs
mkdir -p /opt/airflow/plugins
mkdir -p /opt/airflow/dags

# Set proper permissions
chmod -R 775 /opt/airflow/logs
chmod -R 775 /opt/airflow/plugins

echo "🎵 Spotify Data Platform - Airflow environment ready!"
echo "📍 PYTHONPATH: ${PYTHONPATH}"

# Execute the original command
exec "$@"