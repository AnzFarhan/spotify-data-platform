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
        self.redirect_uri = os.getenv('SPOTIFY_REDIRECT_URI', 'https://datapotipy.com/callback')
        
        # Required scopes for the different API features
        self.scopes = ' '.join([
            'user-read-recently-played',    # For getting recently played tracks
            'user-read-private',            # For getting user details
            'playlist-read-private',        # For reading playlists
            'user-library-read',            # For reading saved tracks
            'user-top-read'                 # For reading top tracks
        ])
    
    def test_config(self):
        """Test if Spotify configuration is complete"""
        missing = []
        if not self.client_id: missing.append('4cf1ea44e49e4ab48c806f3fd6938b74')
        if not self.client_secret: missing.append('e5f24a4570e84274826ea27f3bfeb890')
        
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