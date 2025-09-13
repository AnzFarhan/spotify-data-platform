"""
Simple Spotify API configuration - Day 1 version
"""
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class SpotifyConfig:
    """Basic Spotify API configuration"""
    
    def __init__(self):
        self.client_id = os.getenv('SPOTIFY_CLIENT_ID')
        self.client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
        self.redirect_uri = 'http://localhost:8888/callback'
    
    def test_config(self):
        """Test if Spotify configuration is complete"""
        missing = []
        if not self.client_id: missing.append('SPOTIFY_CLIENT_ID')
        if not self.client_secret: missing.append('SPOTIFY_CLIENT_SECRET')
        
        if missing:
            print(f"❌ Missing Spotify config: {', '.join(missing)}")
            print("👉 Go to https://developer.spotify.com to get your API keys")
            return False
        
        print("✅ Spotify configuration looks good!")
        print(f"Client ID: {self.client_id[:8]}...")
        return True

if __name__ == "__main__":
    # Test the configuration
    config = SpotifyConfig()
    config.test_config()