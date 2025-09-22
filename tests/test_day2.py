"""
Day 2 Tests - API and Database connections
"""
import sys
import os
from pathlib import Path
import psycopg2
import spotipy
from spotipy.oauth2 import SpotifyOAuth

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
from config.database import DatabaseConfig
from config.spotify import SpotifyConfig

# Load environment variables
load_dotenv()

def test_database_connection():
    """Test PostgreSQL database connection"""
    print("🗄️ Testing database connection...")
    
    try:
        db_config = DatabaseConfig()
        
        connection = psycopg2.connect(
            host=db_config.host,
            port=db_config.port,
            database=db_config.database,
            user=db_config.user,
            password=db_config.password
        )
        
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        db_version = cursor.fetchone()
        
        print(f"✅ Database connection successful!")
        print(f"   PostgreSQL version: {db_version[0][:30]}...")
        
        # Test tables exist
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'public';
        """)
        table_count = cursor.fetchone()[0]
        print(f"   Found {table_count} tables")
        
        cursor.close()
        connection.close()
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def test_spotify_api():
    """Test Spotify API connection"""
    print("🎵 Testing Spotify API connection...")
    
    try:
        spotify_config = SpotifyConfig()
        
        # Create OAuth object
        sp_oauth = SpotifyOAuth(
            client_id=spotify_config.client_id,
            client_secret=spotify_config.client_secret,
            redirect_uri=spotify_config.redirect_uri,
            scope="user-read-recently-played"
        )
        
        # Check if we have a cached token
        token_info = sp_oauth.get_cached_token()
        
        if not token_info:
            print("🔐 Authentication required!")
            print(f"👉 Please visit: {sp_oauth.get_authorize_url()}")
            print("   Complete authentication and run this test again")
            return False
        
        # Test API call
        sp = spotipy.Spotify(auth=token_info['access_token'])
        user = sp.current_user()
        
        print(f"✅ Spotify API connection successful!")
        print(f"   User: {user.get('display_name', 'Unknown')}")
        print(f"   Country: {user.get('country', 'Unknown')}")
        
        return True
        
    except Exception as e:
        print(f"❌ Spotify API connection failed: {e}")
        print("💡 Make sure you've set up your API credentials correctly")
        return False

def run_day2_tests():
    """Run all Day 2 tests"""
    print("🚀 Running Day 2 Connection Tests")
    print("=" * 40)
    
    # Test database
    print("\n1️⃣ Database Connection Test")
    print("-" * 30)
    db_success = test_database_connection()
    
    # Test Spotify API
    print("\n2️⃣ Spotify API Connection Test")
    print("-" * 30)
    api_success = test_spotify_api()
    
    # Results
    print(f"\n📊 Day 2 Test Results")
    print("=" * 25)
    print(f"Database: {'✅ PASS' if db_success else '❌ FAIL'}")
    print(f"Spotify API: {'✅ PASS' if api_success else '❌ FAIL'}")
    
    all_passed = db_success and api_success
    
    if all_passed:
        print("\n🎉 All Day 2 tests passed!")
        print("✅ Ready to start building ETL pipeline tomorrow!")
    else:
        print("\n⚠️ Some tests failed. Please fix before Day 3.")
        
        if not db_success:
            print("🔧 Database issues: Check PostgreSQL installation and .env file")
        if not api_success:
            print("🔧 API issues: Complete Spotify authentication")
    
    return all_passed

if __name__ == "__main__":
    success = run_day2_tests()
    sys.exit(0 if success else 1)