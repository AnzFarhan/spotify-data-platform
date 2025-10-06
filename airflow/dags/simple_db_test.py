"""
Simple Database Test DAG
A minimal DAG to test database connectivity with the fixed configuration.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
import psycopg2
import os

# Suppress deprecation warnings
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

# DAG Configuration
default_args = {
    'owner': 'farhan',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Create DAG
dag = DAG(
    'simple_db_test',
    default_args=default_args,
    description='Simple database connectivity test',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['test', 'database'],
)

@task
def test_database_connection():
    """Test database connection with new configuration"""
    try:
        # Database connection parameters from environment
        db_params = {
            'host': os.getenv('POSTGRES_HOST', 'postgres'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'airflow'),
            'user': os.getenv('POSTGRES_USER', 'airflow'),
            'password': os.getenv('POSTGRES_PASSWORD', 'airflow')
        }
        
        print(f"🔗 Connecting to database with config: {db_params['host']}:{db_params['port']}/{db_params['database']} as {db_params['user']}")
        
        # Test connection
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Execute test queries
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"✅ PostgreSQL Version: {version}")
        
        cursor.execute("SELECT current_database(), current_user;")
        db_name, user = cursor.fetchone()
        print(f"✅ Connected to database: {db_name}")
        print(f"✅ Connected as user: {user}")
        
        cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';")
        table_count = cursor.fetchone()[0]
        print(f"✅ Found {table_count} tables in public schema")
        
        cursor.close()
        conn.close()
        
        return {
            'status': 'SUCCESS',
            'database': db_name,
            'user': user,
            'table_count': table_count
        }
        
    except Exception as e:
        print(f"❌ Database connection failed: {str(e)}")
        raise

# Create task
test_db = test_database_connection()

# Set task in DAG
test_db