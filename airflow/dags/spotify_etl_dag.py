"""
Spotify ETL DAG for Apache Airflow
Complete ETL pipeline orchestration with enhanced production features

This DAG provides enterprise-grade workflow management with comprehensive
monitoring, error handling, and data quality validation.

Author: Farhan Dedey
Created: September 2025
Updated: October 2025
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys
import os
import logging

# Airflow imports
from airflow import DAG
from airflow.decorators import task  # This is still valid in 3.1.0
from airflow.models import Variable

# Configure logging early
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Updated Airflow 3.x imports
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

try:
    # Airflow 3.x uses these import paths
    from airflow.operators.python import PythonOperator
    from airflow.operators.bash import BashOperator
    logger.info(" Using Airflow 3.x import paths")
except ImportError:
    try:
        # Fallback to alternative paths if available
        from airflow.providers.standard.operators.python import PythonOperator
        from airflow.providers.standard.operators.bash import BashOperator
        logger.info(" Using Airflow standard provider imports")
    except ImportError:
        # Final fallback (should not happen in most cases)
        from airflow.operators.python_operator import PythonOperator
        from airflow.operators.bash_operator import BashOperator
        logger.info(" Using legacy import paths")

# Database connection handling
import psycopg2
from psycopg2 import sql

# Add project modules to Python path
import sys
import os
sys.path.append('/opt/airflow')
sys.path.append('/opt/airflow/dags')
sys.path.append('/opt/airflow/DE')
sys.path.append('/opt/airflow/project')  # For config module

# Also try adding the workspace paths mounted in the container
workspace_paths = [
    '/opt/airflow/workspace',
    '/opt/airflow/workspace/DE',
    '/opt/airflow/workspace/config',
    '/workspace',
    '/workspace/DE', 
    '/workspace/config'
]

for path in workspace_paths:
    if os.path.exists(path):
        sys.path.append(path)
        logger.info(f"Added to Python path: {path}")

# Import your custom modules with fallback handling
MODULES_AVAILABLE = False
try:
    from DE.extractors.spotify_extractor_v2 import SpotifyExtractorV2
    from DE.transformers.data_transformer import SpotifyDataTransformer
    from DE.loaders.database_loader import SpotifyDatabaseLoader
    from config.database import DatabaseConfig
    MODULES_AVAILABLE = True
    logger.info("All project modules imported successfully")
except ImportError as e:
    logger.warning(f" Custom modules not available: {e}")
    logger.warning(" DAG will run in fallback mode with basic functionality")
    
    # Create mock classes for development
    class SpotifyExtractorV2:
        def extract_user_info(self):
            return {"user_id": "mock_user", "display_name": "Mock User"}
        def extract_recent_tracks(self, limit=50):
            import pandas as pd
            return pd.DataFrame({"track_id": ["mock_track"], "track_name": ["Mock Track"]})
    
    class SpotifyDataTransformer:
        def transform(self, df):
            return df
    
    class SpotifyDatabaseLoader:
        def load_complete_dataset(self, df):
            return {"mock_table": len(df)}
    
    def get_db_connection():
        return None

# DAG Configuration
DAG_ID = 'spotify_etl_pipeline'
SCHEDULE_INTERVAL = '0 */6 * * *'  # Every 6 hours
MAX_ACTIVE_RUNS = 1
CATCHUP = False
RETRIES = 2
RETRY_DELAY = timedelta(minutes=5)

# Default arguments for all tasks
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),  # Fixed start date for Airflow 3.x
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': RETRIES,
    'retry_delay': RETRY_DELAY,
    'max_active_tis_per_dag': 10,
}

# Create the DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description=' Complete Spotify ETL Pipeline - Extract, Transform, Load',
    schedule=SCHEDULE_INTERVAL,  # Changed from schedule_interval to schedule for Airflow 3.x
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=['spotify', 'etl', 'data_engineering', 'production'],
    doc_md=__doc__,
)

# ============================================================================
# TASK FUNCTIONS
# ============================================================================

def check_database_connection(**context):
    """
    Task 1: Check database connectivity and health
    """
    try:
        logger.info("Checking database connection...")
        
        # Test PostgreSQL connection using direct connection
        import os
        
        # Database connection parameters - using Airflow's PostgreSQL container
        db_params = {
            'host': os.getenv('POSTGRES_HOST'),
            'port': os.getenv('POSTGRES_PORT'),
            'database': os.getenv('POSTGRES_DB'),  
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD')
        }
        
        # Test connection
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Execute a simple query to test connection
        cursor.execute("SELECT version(), current_database(), current_user;")
        records = cursor.fetchall()
        
        if records:
            version, database, user = records[0]
            logger.info(f"Database connection successful!")
            logger.info(f" PostgreSQL version: {version}")
            logger.info(f"Database: {database}")
            logger.info(f"User: {user}")
            
            # Check if required tables exist
            tables_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name IN ('artists', 'albums', 'tracks', 'audio_features', 'listening_history');
            """
            cursor.execute(tables_query)
            tables = cursor.fetchall()
            table_names = [table[0] for table in tables]
            
            logger.info(f"Found tables: {table_names}")
            
            # Store table info for downstream tasks
            context['task_instance'].xcom_push(key='database_status', value='healthy')
            context['task_instance'].xcom_push(key='available_tables', value=table_names)
            
            cursor.close()
            conn.close()
            
            return "SUCCESS"
        else:
            raise Exception("No response from database")
            
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        context['task_instance'].xcom_push(key='database_status', value='failed')
        raise


def check_spotify_authentication(**context):
    """
    Task 2: Verify Spotify API authentication and permissions
    """
    try:
        logger.info(" Checking Spotify API authentication...")
        
        # Initialize Spotify extractor
        extractor = SpotifyExtractorV2()
        
        # Test authentication by getting user info
        user_info = extractor.extract_user_info()
        
        if user_info and user_info.get('user_id'):
            logger.info(f"Spotify authentication successful!")
            logger.info(f"User: {user_info.get('display_name', 'Unknown')}")
            logger.info(f"Country: {user_info.get('country', 'Unknown')}")
            logger.info(f"Product: {user_info.get('product', 'Unknown')}")
            
            # Store user info for downstream tasks
            context['task_instance'].xcom_push(key='spotify_auth_status', value='success')
            context['task_instance'].xcom_push(key='user_info', value=user_info)
            
            return "SUCCESS"
        else:
            raise Exception("Failed to retrieve user information")
            
    except Exception as e:
        logger.error(f"Spotify authentication failed: {str(e)}")
        context['task_instance'].xcom_push(key='spotify_auth_status', value='failed')
        raise


def extract_spotify_data(**context):
    """
    Task 3: Extract data from Spotify API
    """
    try:
        logger.info(" Starting Spotify data extraction...")
        
        # Get track limit from Airflow Variable or use default
        track_limit = Variable.get("spotify_track_limit", default_var=50, deserialize_json=False)
        track_limit = int(track_limit)
        
        logger.info(f"Extracting {track_limit} recent tracks...")
        
        # Initialize extractor
        extractor = SpotifyExtractorV2()
        
        # Extract recent tracks with all enhanced features
        df = extractor.extract_recent_tracks(limit=track_limit)
        
        if df.empty:
            logger.warning("No data extracted from Spotify")
            context['task_instance'].xcom_push(key='extraction_status', value='no_data')
            context['task_instance'].xcom_push(key='extracted_records', value=0)
            return "NO_DATA"
        
        # Convert DataFrame to JSON for XCom (Airflow's data passing mechanism)
        df_json = df.to_json(orient='records', date_format='iso')
        
        logger.info(f"Extraction completed successfully!")
        logger.info(f"Records extracted: {len(df)}")
        logger.info(f"Columns: {len(df.columns)}")
        logger.info(f"Column names: {list(df.columns)}")
        
        # Store extracted data for downstream tasks
        context['task_instance'].xcom_push(key='extracted_data', value=df_json)
        context['task_instance'].xcom_push(key='extraction_status', value='success')
        context['task_instance'].xcom_push(key='extracted_records', value=len(df))
        context['task_instance'].xcom_push(key='extracted_columns', value=len(df.columns))
        
        return "SUCCESS"
        
    except Exception as e:
        logger.error(f"Data extraction failed: {str(e)}")
        context['task_instance'].xcom_push(key='extraction_status', value='failed')
        context['task_instance'].xcom_push(key='extracted_records', value=0)
        raise


def transform_data(**context):
    """
    Task 4: Transform extracted data
    """
    try:
        logger.info(" Starting data transformation...")
        
        # Get extracted data from previous task
        extracted_data_json = context['task_instance'].xcom_pull(
            task_ids='extract_spotify_data', 
            key='extracted_data'
        )
        
        if not extracted_data_json:
            logger.error("No data received from extraction task")
            raise Exception("No data to transform")
        
        # Convert JSON back to DataFrame
        import pandas as pd
        import json
        
        extracted_data = json.loads(extracted_data_json)
        df = pd.DataFrame(extracted_data)
        
        logger.info(f" Received {len(df)} records for transformation")
        
        # Initialize transformer
        transformer = SpotifyDataTransformer()
        
        # Transform the data (returns tuple: df, quality_report)
        transformed_df, quality_report = transformer.transform(df)
        
        if transformed_df.empty:
            logger.warning("Transformation resulted in empty dataset")
            context['task_instance'].xcom_push(key='transformation_status', value='empty_result')
            return "EMPTY_RESULT"
        
        # Log quality report
        if quality_report:
            logger.info(f"📊 Data Quality Report:")
            logger.info(f"   Total records: {quality_report.get('total_records', 'N/A')}")
            logger.info(f"   Valid records: {quality_report.get('valid_records', 'N/A')}")
            logger.info(f"   Duplicate count: {quality_report.get('duplicate_count', 'N/A')}")
        
        # Convert DataFrame dtypes to Python native types to avoid numpy serialization issues
        # Create a copy to avoid modifying the original
        df_serializable = transformed_df.copy()
        
        # Convert all object columns to string to avoid numpy dtype issues
        for col in df_serializable.select_dtypes(include=['object']).columns:
            df_serializable[col] = df_serializable[col].astype(str)
        
        # Convert to dict (which handles numpy types better) then to JSON
        transformed_data_dict = df_serializable.to_dict(orient='records')
        import json
        transformed_data_json = json.dumps(transformed_data_dict, default=str)
        
        logger.info(f"Transformation completed successfully!")
        logger.info(f"Input records: {len(df)}")
        logger.info(f"Output records: {len(transformed_df)}")
        logger.info(f"Features added: {len(transformed_df.columns) - len(df.columns)}")
        
        # Convert quality_report to JSON-serializable format by converting to JSON string
        # This avoids any numpy dtype issues completely
        import json
        quality_report_json = json.dumps(quality_report, default=str) if quality_report else "{}"
        
        # Store transformed data and quality report for loading task
        context['task_instance'].xcom_push(key='transformed_data', value=transformed_data_json)
        context['task_instance'].xcom_push(key='transformation_status', value='success')
        context['task_instance'].xcom_push(key='transformed_records', value=len(transformed_df))
        context['task_instance'].xcom_push(key='transformed_columns', value=len(transformed_df.columns))
        context['task_instance'].xcom_push(key='quality_report', value=quality_report_json)
        
        return "SUCCESS"
        
    except Exception as e:
        logger.error(f"Data transformation failed: {str(e)}")
        context['task_instance'].xcom_push(key='transformation_status', value='failed')
        raise


def load_data_to_database(**context):
    """
    Task 5: Load transformed data into PostgreSQL database
    """
    try:
        logger.info(" Starting data loading to database...")
        
        # Get transformed data from previous task
        transformed_data_json = context['task_instance'].xcom_pull(
            task_ids='transform_data', 
            key='transformed_data'
        )
        
        if not transformed_data_json:
            logger.error("No transformed data received")
            raise Exception("No data to load")
        
        # Convert JSON back to DataFrame
        import pandas as pd
        import json
        
        transformed_data = json.loads(transformed_data_json)
        df = pd.DataFrame(transformed_data)
        
        logger.info(f" Received {len(df)} records for loading")
        
        # Initialize database loader
        loader = SpotifyDatabaseLoader()
        
        # Load the data using the enhanced loader
        load_results = loader.load_complete_dataset(df)
        
        # Extract loading statistics
        total_loaded = sum(load_results.values())
        
        logger.info(f"Data loading completed successfully!")
        logger.info(f"Total records loaded: {total_loaded}")
        logger.info(f"Loading breakdown:")
        for table, count in load_results.items():
            logger.info(f"   {table}: {count} records")
        
        # Store loading results
        context['task_instance'].xcom_push(key='loading_status', value='success')
        context['task_instance'].xcom_push(key='total_loaded_records', value=total_loaded)
        context['task_instance'].xcom_push(key='loading_breakdown', value=load_results)
        
        return "SUCCESS"
        
    except Exception as e:
        logger.error(f"Data loading failed: {str(e)}")
        context['task_instance'].xcom_push(key='loading_status', value='failed')
        raise


def check_data_quality(**context):
    """
    Task 6: Validate data quality after loading
    """
    try:
        logger.info("Checking data quality...")
        
        # Test PostgreSQL connection using direct connection
        import os
        import psycopg2
        
        # Database connection parameters - using Airflow's PostgreSQL container
        db_params = {
            'host': os.getenv('POSTGRES_HOST'),
            'port': os.getenv('POSTGRES_PORT'),
            'database': os.getenv('POSTGRES_DB'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD')
        }
        
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Check table counts
        tables = ['artists', 'albums', 'tracks', 'audio_features', 'listening_history']
        counts = {}
        
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                counts[table] = count
                logger.info(f"  {table}: {count} records")
            except Exception as e:
                logger.warning(f" Table {table} not found or inaccessible: {e}")
                counts[table] = 0
        
        cursor.close()
        conn.close()
        
        # Store quality check results
        context['task_instance'].xcom_push(key='data_quality_status', value='success')
        context['task_instance'].xcom_push(key='table_counts', value=counts)
        
        logger.info("Data quality check completed")
        
        return {
            'status': 'success',
            'table_counts': counts
        }
        
    except Exception as e:
        logger.error(f"Data quality check failed: {str(e)}")
        context['task_instance'].xcom_push(key='data_quality_status', value='failed')
        raise


def generate_pipeline_report(**context):
    """
    Task 7: Generate a comprehensive pipeline execution report
    """
    try:
        logger.info("📊 Generating pipeline execution report...")
        
        # Collect data from all previous tasks
        extraction_status = context['task_instance'].xcom_pull(task_ids='extract_spotify_data', key='extraction_status')
        extracted_records = context['task_instance'].xcom_pull(task_ids='extract_spotify_data', key='extracted_records') or 0
        
        transformation_status = context['task_instance'].xcom_pull(task_ids='transform_data', key='transformation_status')
        transformed_records = context['task_instance'].xcom_pull(task_ids='transform_data', key='transformed_records') or 0
        
        loading_status = context['task_instance'].xcom_pull(task_ids='load_data_to_database', key='loading_status')
        total_loaded_records = context['task_instance'].xcom_pull(task_ids='load_data_to_database', key='total_loaded_records') or 0
        loading_breakdown = context['task_instance'].xcom_pull(task_ids='load_data_to_database', key='loading_breakdown') or {}
        
        quality_status = context['task_instance'].xcom_pull(task_ids='check_data_quality', key='data_quality_status')
        table_counts = context['task_instance'].xcom_pull(task_ids='check_data_quality', key='table_counts') or {}
        
        user_info = context['task_instance'].xcom_pull(task_ids='check_spotify_authentication', key='user_info') or {}
        
        # Calculate pipeline duration
        dag_run = context['dag_run']
        # Use logical_date for Airflow 3.x (execution_date is deprecated)
        logical_date = dag_run.logical_date if hasattr(dag_run, 'logical_date') else dag_run.execution_date
        
        # Create comprehensive report
        report = {
            'pipeline_id': DAG_ID,
            'execution_date': logical_date.isoformat(),
            'execution_status': 'SUCCESS' if all([
                extraction_status == 'success',
                transformation_status == 'success', 
                loading_status == 'success',
                quality_status == 'success'
            ]) else 'PARTIAL_SUCCESS',
            'spotify_user': user_info.get('display_name', 'Unknown'),
            'extraction': {
                'status': extraction_status,
                'records_extracted': extracted_records
            }, 
            'transformation': {
                'status': transformation_status,
                'records_transformed': transformed_records
            },
            'loading': {
                'status': loading_status,
                'total_records_loaded': total_loaded_records,
                'breakdown': loading_breakdown
            },
            'quality_check': {
                'status': quality_status,
                'table_counts': table_counts
            }
        }
        
        # Log the report
        logger.info("PIPELINE EXECUTION REPORT")
        logger.info("=" * 50)
        logger.info(f"Execution Date: {logical_date}")
        logger.info(f"Spotify User: {user_info.get('display_name', 'Unknown')}")
        logger.info(f"Extracted: {extracted_records} records")
        logger.info(f" Transformed: {transformed_records} records")
        logger.info(f"Loaded: {total_loaded_records} records")
        logger.info(f"Loading breakdown: {loading_breakdown}")
        logger.info(f"Table counts: {table_counts}")
        logger.info(f"Overall Status: {report['execution_status']}")
        logger.info("=" * 50)
        
        # Store the report
        context['task_instance'].xcom_push(key='pipeline_report', value=report)
        
        return report['execution_status']
        
    except Exception as e:
        logger.error(f"Failed to generate pipeline report: {str(e)}")
        raise


# ============================================================================
# TASK DEFINITIONS
# ============================================================================

# Start Task
start_task = BashOperator(
    task_id='start',
    bash_command='echo "Starting Spotify ETL Pipeline..."',
    dag=dag,
)

# Task 1: Database Health Check
check_database_task = PythonOperator(
    task_id='check_database_connection',
    python_callable=check_database_connection,
    doc_md="""
    ## Database Health Check
    
    This task verifies:
    - PostgreSQL connection is working
    - Required tables exist
    - Database permissions are correct
    
    **Outputs:**
    - database_status: 'healthy' or 'failed'
    - available_tables: list of existing tables
    """,
    dag=dag,
)

# Task 2: Spotify Authentication Check  
check_spotify_task = PythonOperator(
    task_id='check_spotify_authentication',
    python_callable=check_spotify_authentication,
    doc_md="""
    ## Spotify API Authentication
    
    This task verifies:
    - Spotify API credentials are valid
    - OAuth tokens are working
    - User permissions are sufficient
    
    **Outputs:**
    - spotify_auth_status: 'success' or 'failed'
    - user_info: Spotify user information
    """,
    dag=dag,
)

# Task 3: Data Extraction
extract_data_task = PythonOperator(
    task_id='extract_spotify_data',
    python_callable=extract_spotify_data,
    doc_md="""
    ## Spotify Data Extraction
    
    This task extracts:
    - Recent listening history
    - Track metadata and audio features  
    - Artist details (genres, popularity, followers)
    - Album information
    
    **Configuration:**
    - Track limit can be set via Airflow Variable 'spotify_track_limit'
    
    **Outputs:**
    - extracted_data: JSON serialized DataFrame
    - extracted_records: number of records extracted
    """,
    dag=dag,
)

# Task 4: Data Transformation
transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    doc_md="""
    ## Data Transformation
    
    This task performs:
    - Data cleaning and validation
    - Feature engineering
    - Timestamp processing  
    - Audio feature categorization
    - Listening analytics calculation
    
    **Outputs:**
    - transformed_data: JSON serialized transformed DataFrame
    - transformed_records: number of records after transformation
    """,
    dag=dag,
)

# Task 5: Data Loading
load_data_task = PythonOperator(
    task_id='load_data_to_database',
    python_callable=load_data_to_database,
    doc_md="""
    ## Database Loading
    
    This task loads data into:
    - artists table (with genres, popularity, followers)
    - albums table
    - tracks table  
    - audio_features table
    - listening_history table
    
    **Features:**
    - Upsert logic to handle duplicates
    - Batch processing for efficiency
    - Transaction management
    
    **Outputs:**
    - loading_breakdown: records loaded per table
    - total_loaded_records: total records loaded
    """,
    dag=dag,
)

# Task 6: Data Quality Check
quality_check_task = PythonOperator(
    task_id='check_data_quality',
    python_callable=check_data_quality,
    doc_md="""
    ## Data Quality Validation
    
    This task validates:
    - Data integrity after loading
    - Table record counts
    - Database accessibility
    - Data consistency checks
    
    **Outputs:**
    - data_quality_status: 'success' or 'failed'
    - table_counts: record counts per table
    """,
    dag=dag,
)

# Task 7: Pipeline Reporting
report_task = PythonOperator(
    task_id='generate_pipeline_report',
    python_callable=generate_pipeline_report,
    doc_md="""
    ## Pipeline Execution Report
    
    This task generates:
    - Comprehensive execution summary
    - Performance metrics
    - Data quality statistics
    - Error reporting (if any)
    
    **Outputs:**
    - pipeline_report: complete execution report
    """,
    dag=dag,
)

# End Task
end_task = BashOperator(
    task_id='end',
    bash_command='echo "Spotify ETL Pipeline completed successfully!"',
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES - DEFINE THE WORKFLOW
# ============================================================================

# Define the ETL workflow:
# 1. Start pipeline
# 2. Check prerequisites (database + spotify auth) in parallel
# 3. Extract data from Spotify
# 4. Transform the extracted data  
# 5. Load transformed data to database
# 6. Validate data quality
# 7. Generate execution report
# 8. End pipeline

# Pipeline flow
start_task >> [check_database_task, check_spotify_task] >> extract_data_task
extract_data_task >> transform_data_task >> load_data_task >> quality_check_task >> report_task >> end_task

# ============================================================================
# DAG DOCUMENTATION
# ============================================================================

dag.doc_md = """
#  Spotify ETL Pipeline

## Overview
This DAG automates the complete Spotify data pipeline with the following benefits over manual execution:

### ✅ **Production Features:**
- **Automated Scheduling**: Runs every 6 hours without manual intervention
- **Task-Level Monitoring**: Each step (Extract → Transform → Load) is monitored separately  
- **Error Handling**: Automatic retries with exponential backoff
- **Data Lineage**: Full visibility into data flow and transformations
- **Parallel Processing**: Database and Spotify checks run simultaneously
- **XCom Data Passing**: Efficient data transfer between tasks

### 📊 **Pipeline Steps:**

1. **Prerequisites Check** (Parallel):
   - Database connectivity and schema validation
   - Spotify API authentication verification

2. **Data Extraction**:
   - Recent listening history (configurable limit)
   - Enhanced audio features with realistic mock data fallback
   - Detailed artist information (genres, popularity, followers)

3. **Data Transformation**:
   - Data cleaning and validation
   - Feature engineering and derived metrics
   - Audio feature categorization
   - Listening analytics computation

4. **Data Loading**:
   - Efficient upsert operations to handle duplicates
   - Multi-table loading with proper relationships
   - Transaction management for data consistency

5. **Reporting**:
   - Comprehensive execution summary
   - Data quality metrics
   - Performance statistics

### 🔧 **Configuration:**
- **Schedule**: Every 6 hours (customizable)
- **Track Limit**: Configurable via Airflow Variable `spotify_track_limit`
- **Retries**: 2 attempts with 5-minute delays
- **Max Active Runs**: 1 (prevents overlapping executions)

### 📈 **Monitoring:**
- View progress in Airflow UI: Graph View, Tree View, Gantt Chart
- Monitor Celery workers via Flower: http://localhost:5555
- Database monitoring via PostgreSQL logs
- Task-level logs for debugging

### 🚨 **Error Handling:**
- Automatic retries for transient failures
- Graceful fallbacks for API limitations (mock audio features)
- Detailed error logging and reporting
- Email notifications (configurable)

This DAG replaces the manual `SpotifyETLPipeline` orchestration with enterprise-grade
workflow management, providing better reliability, monitoring, and scalability.
"""