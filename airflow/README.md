# 🎵 Spotify Data Platform - Airflow Orchestration

This directory contains the Apache Airflow setup for automating your Spotify ETL pipeline with enterprise-grade orchestration and monitoring.

## 🚀 Quick Start

### 1. **Initialize Airflow** (First time only)
```bash
cd airflow
docker-compose up airflow-init
```

### 2. **Start All Services**
```bash
docker-compose up -d
```

### 3. **Access the Services**
- **Airflow Web UI**: http://localhost:8080
  - Username: `your_username` 
  - Password: `your_password`
- **Flower (Celery Monitoring)**: http://localhost:5555
- **PostgreSQL Database**: localhost:5432

## 📊 What's Included

### **Services Architecture**
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Airflow       │    │    PostgreSQL    │    │     Redis       │
│   Webserver     │◄──►│   (App Data)     │    │   (Message      │
│                 │    │                  │    │    Broker)      │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Airflow       │    │   PostgreSQL     │    │    Airflow      │
│   Scheduler     │    │  (Airflow Meta)  │    │    Worker       │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                                               │
         └───────────────────────────────────────────────┘
                              │
                    ┌─────────────────┐
                    │     Flower      │
                    │   (Monitoring)  │
                    └─────────────────┘
```

### **Production-Ready DAG: `spotify_etl_dag.py`**

**🔄 Automated ETL Workflow:**
```
Prerequisites Check (Parallel)
├── Database Health Check
└── Spotify Authentication
         │
         ▼
    Data Extraction
    ├── Recent tracks (configurable limit)
    ├── Audio features (with fallbacks)
    └── Artist details (genres, popularity)
         │
         ▼
    Data Transformation  
    ├── Data cleaning & validation
    ├── Feature engineering
    └── Analytics computation
         │
         ▼
    Database Loading
    ├── Artists (with detailed info)
    ├── Albums & Tracks
    ├── Audio Features
    └── Listening History
         │
         ▼
    Pipeline Report Generation
    └── Comprehensive execution summary
```

## ⚙️ Configuration

### **Schedule Settings**
- **Default**: Every 6 hours (`0 */6 * * *`)
- **Customizable**: Edit `SCHEDULE_INTERVAL` in `spotify_etl_dag.py`
- **Track Limit**: Set via Airflow Variable `spotify_track_limit` (default: 50)

### **Environment Variables** 
The DAG uses your existing `.env` file:
```env
# Database Configuration
POSTGRES_HOST=          # Uses Docker service name
POSTGRES_DB=
POSTGRES_USER=
POSTGRES_PASSWORD=

# Spotify API
SPOTIFY_CLIENT_ID=your_client_id
SPOTIFY_CLIENT_SECRET=your_client_secret
SPOTIFY_REDIRECT_URI=

# Airflow Admin
AIRFLOW_ADMIN_USER=
AIRFLOW_ADMIN_PASSWORD=
```

## 📈 Monitoring & Operations

### **Airflow Web UI Features**
1. **Graph View**: Visual workflow representation
2. **Tree View**: Historical run status
3. **Gantt Chart**: Task timing and duration
4. **Task Logs**: Detailed execution logs
5. **Data Lineage**: XCom data flow tracking

### **Key Metrics Tracked**
- ✅ **Extraction**: Records retrieved from Spotify API
- 🔄 **Transformation**: Features engineered and data quality
- 📤 **Loading**: Records loaded per database table
- ⏱️ **Performance**: Task duration and pipeline timing
- 🚨 **Errors**: Detailed failure reporting with retry logic

### **Monitoring Commands**
```bash
# Check service status
docker-compose ps

# View real-time logs
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-webserver

# Monitor worker performance
docker-compose logs -f airflow-worker

# Check Celery workers (Flower UI)
open http://localhost:5555
```

## 🔧 Task Configuration

### **Configurable Parameters**
Set these in Airflow UI → Admin → Variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `spotify_track_limit` | 50 | Number of recent tracks to extract |

### **Task Retry Logic**
- **Retries**: 2 attempts per task
- **Retry Delay**: 5 minutes
- **Exponential Backoff**: Built into Celery
- **Failure Notifications**: Configurable email alerts

## 🏗️ Architecture Benefits

### **vs Manual Pipeline (`pipeline_orchestrator.py`)**

| Feature | Manual Class | Airflow DAG |
|---------|-------------|-------------|
| **Scheduling** | Manual execution | Automated cron-like scheduling |
| **Monitoring** | Limited logging | Full web UI with metrics |
| **Error Handling** | Basic try/catch | Retry policies + notifications |
| **Scalability** | Single process | Distributed worker pool |
| **Data Lineage** | None | Full XCom tracking |
| **Task Dependencies** | Linear code | Visual DAG with parallel tasks |
| **Recovery** | Manual restart | Automatic retry + backfill |

### **Production Features**
✅ **High Availability**: Multi-worker Celery setup  
✅ **Resource Management**: Configurable concurrency limits  
✅ **Data Quality**: Task-level validation and reporting  
✅ **Observability**: Comprehensive logging and metrics  
✅ **Fault Tolerance**: Graceful handling of API limitations  
✅ **Scalability**: Easy to add workers or adjust schedules  

## 🚨 Troubleshooting

### **Common Issues**

1. **DAG Not Appearing**
   ```bash
   # Check scheduler logs
   docker-compose logs airflow-scheduler
   
   # Verify DAG syntax
   docker-compose exec airflow-webserver python -m py_compile /opt/airflow/dags/spotify_etl_dag.py
   ```

2. **Task Failures**
   ```bash
   # Check task-specific logs in Airflow UI
   # Or view container logs
   docker-compose logs airflow-worker
   ```

3. **Database Connection Issues**
   ```bash
   # Test database connectivity
   docker-compose exec postgres psql -U postgres -d spotify_data -c "SELECT version();"
   ```

4. **Spotify API Errors**  
   - Check your `.env` file credentials
   - Verify redirect URI matches Spotify app settings
   - Monitor rate limiting in task logs

### **Performance Tuning**
- Adjust `worker_concurrency` in docker-compose.yaml
- Modify task retries in DAG configuration  
- Scale workers: `docker-compose up --scale airflow-worker=3`

## 📋 Maintenance

### **Regular Operations**
```bash
# Graceful shutdown
docker-compose down

# Update and restart
docker-compose pull && docker-compose up -d

# Clean up old logs (optional)
docker-compose exec airflow-webserver airflow db clean --clean-before-timestamp $(date -d '7 days ago' -Iseconds)

# Backup database
docker-compose exec postgres pg_dump -U postgres spotify_data > backup.sql
```

### **Scaling Options**
```bash
# Add more workers
docker-compose up -d --scale airflow-worker=3

# Monitor worker utilization
docker-compose exec airflow-flower celery inspect active
```

---

## 🎯 Next Steps

1. **Start the services**: Run `setup.bat` or follow Quick Start
2. **Monitor first run**: Watch the DAG execution in Airflow UI
3. **Customize schedule**: Adjust timing based on your needs  
4. **Set up alerts**: Configure email notifications for failures
5. **Scale as needed**: Add workers based on workload

Your Spotify data pipeline is now running on enterprise-grade orchestration! 🚀

**Access Points:**
- 🌐 **Airflow**: http://localhost:8080
- 🌸 **Flower**: http://localhost:5555
- 🗄️ **Database**: localhost:5432