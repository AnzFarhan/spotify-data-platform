"""
Day 2: Database setup script
Creates all tables and indexes
"""
import os
import sys
from pathlib import Path
import psycopg2
from dotenv import load_dotenv

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
load_dotenv()

def connect_to_database():
    """Connect to PostgreSQL database"""
    try:
        connection = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            database=os.getenv('POSTGRES_DB', 'spotify_data'),
            user=os.getenv('POSTGRES_USER', 'postgres'),
            password=os.getenv('POSTGRES_PASSWORD', 'admin0125')
        )
        print("✅ Connected to PostgreSQL database")
        return connection
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None

def create_tables():
    """Create database tables from SQL file"""
    print("🗄️ Creating database tables...")
    
    connection = connect_to_database()
    if not connection:
        return False
    
    try:
        cursor = connection.cursor()
        
        # Read SQL file
        sql_file = project_root.parent / "sql" / "create_tables.sql"
        with open(sql_file, 'r') as f:
            sql_commands = f.read()
        
        # Execute SQL commands
        cursor.execute(sql_commands)
        connection.commit()
        
        print("✅ Database tables created successfully!")
        
        # Verify tables were created
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        
        tables = cursor.fetchall()
        print(f"📋 Created {len(tables)} tables:")
        for table in tables:
            print(f"  - {table[0]}")
        
        cursor.close()
        connection.close()
        return True
        
    except Exception as e:
        print(f"❌ Failed to create tables: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Setting up Spotify Data Platform Database")
    print("=" * 50)
    
    success = create_tables()
    
    if success:
        print("\n🎉 Database setup complete!")
        print("👉 Next: Test API and database connections")
    else:
        print("\n⚠️ Database setup failed. Check your configuration.")
    
    sys.exit(0 if success else 1)