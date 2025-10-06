# Spotify Data Platform

A comprehensive data platform showcasing **Data Engineering**, **Analytics**, and **Data Science** skills using Spotify listening data.

# Spotify API ETL Pipeline
## 🎯 Project Overview

**Challenging ETL Learning Journey:**
- Real-time data extraction from Spotify Web API  
- Automated ETL processes with Apache Airflow
- Data quality validation and error handling
- Scalable database design with PostgreSQL
- Production-ready monitoring and logging

## 🏗️ Architecture
![assets/Architucture_Spotfiy_Data_Platform.drawio.png](https://github.com/AnzFarhan/spotify-data-platform/blob/ef07643d0da9f1206dfac6a34cbc39c3f3f067c6/assets/Architucture_Spotfiy_Data_Platform.drawio.png)

## 🚀 Features
### ✅ Data Extraction
- Spotify Web API integration with OAuth2
### ✅ Data Transformation
- Data cleaning and normalization 
- Duplicate detection and removal
### ✅ Data Loading
- PostgreSQL database operations
- Foreign key relationship handling
- Batch and streaming inserts
### ✅ Pipeline Automation
- Airflow DAGs for scheduling
- Data quality monitoring
- Error handling and retries
- Email notifications on failures

## ⚙️ Configuration
```bash
# Spotify API
SPOTIFY_CLIENT_ID=your_client_id
SPOTIFY_CLIENT_SECRET=your_client_secret
SPOTIFY_REDIRECT_URI=http://localhost:8888/callback
```
```bash
# Database
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=spotify_data
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
```
## 🧪 Testing
```bash
# Python virtual environment
.venv\Scripts\activate          # To activate the virtual environment
```
```bash
# Environment and connections
python tests/test_day1.py              # Test environment setup
python tests/test_day2.py              # Test API and DB connections  
python tests/test_day3.py              # Test complete ETL pipeline
```
```bash
# Database operations
python sql/scripts/setup_database.py       # Initialize database schema
```
```bash
# ETL Pipeline operations
python sql/scripts/run_etl_pipeline.py                      # Run full pipeline (50 tracks)
python sql/scripts/run_etl_pipeline.py --limit 200          # Run with more tracks
python sql/scripts/run_etl_pipeline.py --mode incremental   # Run incremental update
```
```bash
# Individual component testing
python DE/extractors/spotify_extractor_v2.py
python DE/transformers/data_transformer.py
python DE/loaders/database_loader.py
python DE/pipeline_orchestrator.py
```
*********************************************************************************************
## Quick Start 
### 1. Clone the project from the repository.
```bash
git clone https://github.com/YOUR_USERNAME/spotify-data-platform.git
cd spotify-data-platform
```
### 2. Clone the project from the repository.
```bash
# Copy environment template
cp .env.example .env
```
### 3. Environment Setup
```bash
# Create virtual environment
python -m venv venv
venv\Scripts\activate
# Install dependencies
pip install -r requirements.txt
```
### 4. Database Setup
```bash
# COnnect to database 
psql -h <hostname> -p <port> -U <username> -d <database_name>
```
```bash
# Create PostgreSQL database
createdb spotify_data
```
```bash
# Initialize schema
python scripts/setup_database.py
```
### 5. Airflow Setup 
```bash
# Navigate to airflow directory  
cd airflow
```
```bash
# Initialize Airflow (first time only)
docker-compose up airflow-init
```
```bash
# Start all services
docker-compose up -d
```
### 6. **Access the Services Airflow**
- **Airflow Web UI**: http://localhost:8080
  - Username: `username` 
  - Password: `password`
- **Flower (Celery Monitoring)**: http://localhost:5555
- **PostgreSQL Database**: localhost:5432

*********************************************************************************************
### 🎯 Next Steps (Future Phases)
- Phase 2: Data Analytics & Visualization
- Phase 3: Machine Learning Models  
- Phase 4: Real-time Recommendations

*********************************************************************************************