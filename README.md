# Spotify Data Platform

A comprehensive data platform showcasing **Data Engineering**, **Analytics**, and **Data Science** skills using Spotify listening data.

## 🎯 Project Overview

**Challenging 30-Day Learning Journey:**
- **Week 1:** Data Engineering (ETL pipelines, PostgreSQL)  
- **Week 2:** Data Analytics (Dashboards, insights)
- **Week 3:** Data Science (ML models, recommendations)
- **Week 4:** Integration & Portfolio

*********************************************************************************************

## 🚀 Current Status: Day 1 ✅

- git clone https://github.com/YOUR_USERNAME/spotify-data-platform.git
- cd spotify-data-platform
- python -m venv venv
- source venv/bin/activate
- pip install -r requirements.txt

### Completed:
- ✅ GitHub repository created
- ✅ Project structure established  
- ✅ Python environment configured
- ✅ Basic configuration files created
- ✅ Environment tests passing

*********************************************************************************************

## 🚀 Current Status: Day 2 ✅

### 1. Configure credentials 
- cp .env.example .env
#### Edit .env with your Spotify API and PostgreSQL credentials

### 2. Setup database 
- python sql\scripts\setup_database.py

### 3. Test everything works 
- python tests\test_day2.py
- python DE\spotify_extractor.py

### Completed:
- ✅ **Day 1:** Environment setup, project structure
- ✅ **Day 2:** Spotify API integration, PostgreSQL setup, database schema

### What Works Now:
- 🎵 **Spotify API**: Extract recently played tracks
- 🗄️ **PostgreSQL**: Database with proper schema
- 🔗 **Connections**: All systems tested and working
- 📊 **Data**: Can extract and save listening data

## 🎯 Success Metrics
### Day 2 Achievements:
- 🎵 Successfully connected to Spotify API
- 🗄️ PostgreSQL database operational with 5 tables
- 📊 Can extract and save real listening data
- 🧪 All tests passing

*********************************************************************************************

### Next Steps (Day 2):
- 🔄 Get Spotify API credentials
- 🔄 Install and configure PostgreSQL
- 🔄 Test API and database connections

### Next Steps (Day 3):
- 🔄 Build complete ETL pipeline
- 🔄 Add data transformation and cleaning
- 🔄 Automate data loading to database

*********************************************************************************************

## 📁 Project Structure
- potify-data-platform/
- ├── DE/     # Week 1: ETL pipelines
- ├── DA/       # Week 2: Analysis & dashboards
- ├── DS/         # Week 3: ML models
- ├── config/              # Configuration management
- ├── tests/               # Testing suite
- └── docs/                # Documentation

-- Progress Tracking --

Day 1: Environment Setup ✅
Day 2: API & Database Setup ✅
Day 3: ETL Pipeline 🔄
Day 8: Analytics Dashboard ⏳
Day 15: ML Models ⏳
Day 30: Complete Platform ⏳

