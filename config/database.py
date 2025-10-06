"""
Simple database configuration - Day 1 version
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class DatabaseConfig:
    """Basic database configuration"""
    
    def __init__(self):
        self.host = os.getenv('POSTGRES_HOST', 'localhost')
        self.port = os.getenv('POSTGRES_PORT', '5432')
        self.database = os.getenv('POSTGRES_DB', 'spotify_data')
        self.user = os.getenv('POSTGRES_USER', 'postgres')
        self.password = os.getenv('POSTGRES_PASSWORD', 'admin0125')
    
    def get_connection_string(self):
        """Get database connection string"""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def test_config(self):
        """Test if configuration is complete"""
        missing = []
        if not self.host: missing.append('POSTGRES_HOST')
        if not self.database: missing.append('POSTGRES_DB')
        if not self.user: missing.append('POSTGRES_USER')
        if not self.password: missing.append('POSTGRES_PASSWORD')
        
        if missing:
            print(f"❌ Missing database config: {', '.join(missing)}")
            return False
        
        print("✅ Database configuration looks good!")
        return True

if __name__ == "__main__":
    # Test the configuration
    config = DatabaseConfig()
    config.test_config()
    print(f"Connection string: {config.get_connection_string()}")