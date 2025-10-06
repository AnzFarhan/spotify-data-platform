"""
Day 2: Basic Spotify data extractor
Simple version to test API functionality
"""
import os
from dotenv import load_dotenv
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from datetime import datetime
import certifi
import requests

# Load environment variables
load_dotenv()

def get_spotify_client():
    """Create authenticated Spotify client"""
    try:
        sp = spotipy.Spotify(
            auth_manager=SpotifyOAuth(
                client_id=os.getenv('SPOTIPY_CLIENT_ID'),
                client_secret=os.getenv('SPOTIPY_CLIENT_SECRET'),
                redirect_uri=os.getenv('SPOTIPY_REDIRECT_URI'),
                scope="user-read-recently-played",
            )
        )
        return sp
    except Exception as e:
        print(f"❌ Failed to create Spotify client: {e}")
        return None

def get_recent_tracks(sp, limit=20):
    """Get recently played tracks"""
    try:
        print(f"🎵 Getting {limit} recent tracks...")
        results = sp.current_user_recently_played(limit=limit)
        
        tracks_data = []
        for item in results['items']:
            track = item['track']
            played_at = item['played_at']
            
            track_info = {
                'track_id': track['id'],
                'track_name': track['name'],
                'artist_name': track['artists'][0]['name'],
                'artist_id': track['artists'][0]['id'],
                'album_name': track['album']['name'],
                'album_id': track['album']['id'],
                'played_at': played_at,
                'duration_ms': track['duration_ms'],
                'popularity': track['popularity'],
                'explicit': track['explicit']
            }
            tracks_data.append(track_info)
        
        df = pd.DataFrame(tracks_data)
        print(f"✅ Retrieved {len(tracks_data)} tracks")
        return df
        
    except Exception as e:
        print(f"❌ Failed to get recent tracks: {e}")
        return None

def save_to_csv(df, filename="recent_tracks.csv"):
    """Save DataFrame to CSV file"""
    if df is not None and not df.empty:
        df.to_csv(filename, index=False)
        print(f"✅ Saved {len(df)} tracks to {filename}")
        return True
    else:
        print("❌ No data to save")
        return False

def main():
    """Main function to test Spotify data extraction"""
    print("🚀 Testing Spotify Data Extractor")
    print("=" * 50)
    
    # Get Spotify client
    sp = get_spotify_client()
    if not sp:
        print("❌ Failed to initialize Spotify client")
        return False
    
    # Get recent tracks
    df = get_recent_tracks(sp, limit=10)
    if df is None:
        return False
    
    # Save to CSV
    success = save_to_csv(df, "day2_test_tracks.csv")
    
    if success:
        print("\n📊 Sample data:")
        print(df[['track_name', 'artist_name', 'played_at']].head())
        print("\n🎉 Data extraction complete!")
        return True
    return False

if __name__ == "__main__":
    import os
    os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
    success = main()
    exit(0 if success else 1)
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

class SpotifyExtractor:
    """Simple Spotify data extractor for Day 2"""
    
    def __init__(self):
        self.client_id = os.getenv('SPOTIPY_CLIENT_ID')
        self.client_secret = os.getenv('SPOTIPY_CLIENT_SECRET')
        self.redirect_uri = os.getenv('SPOTIPY_REDIRECT_URI')
        self.scope = "user-read-recently-played"
        
        self.sp = None
        self._setup_spotify()
    
    def _setup_spotify(self):
        """Set up Spotify client"""
        try:
            import certifi
            import requests
            session = requests.Session()
            session.verify = certifi.where()
            
            sp_oauth = SpotifyOAuth(
                client_id=self.client_id,
                client_secret=self.client_secret,
                redirect_uri=self.redirect_uri,
                scope=self.scope,
                requests_session=session
            )
            
            token_info = sp_oauth.get_cached_token()
            if not token_info:
                print("🔐 Need to authenticate with Spotify...")
                auth_url = sp_oauth.get_authorize_url()
                print(f"👉 Go to: {auth_url}")
                response = input("Paste the redirect URL here: ")
                code = sp_oauth.parse_response_code(response)
                token_info = sp_oauth.get_access_token(code)
            
            self.sp = spotipy.Spotify(auth=token_info['access_token'], requests_session=False)
            print("✅ Spotify client ready!")
            
        except Exception as e:
            print(f"❌ Spotify setup failed: {e}")
    
    def get_recent_tracks(self, limit=20):
        """Get recently played tracks"""
        if not self.sp:
            print("❌ Spotify client not available")
            return None
        
        try:
            print(f"🎵 Getting {limit} recent tracks...")
            
            results = self.sp.current_user_recently_played(limit=limit)
            
            tracks_data = []
            for item in results['items']:
                track = item['track']
                played_at = item['played_at']
                
                track_info = {
                    'track_id': track['id'],
                    'track_name': track['name'],
                    'artist_name': track['artists'][0]['name'],
                    'artist_id': track['artists'][0]['id'],
                    'album_name': track['album']['name'],
                    'album_id': track['album']['id'],
                    'played_at': played_at,
                    'duration_ms': track['duration_ms'],
                    'popularity': track['popularity'],
                    'explicit': track['explicit']
                }
                tracks_data.append(track_info)
            
            print(f"✅ Retrieved {len(tracks_data)} tracks")
            return pd.DataFrame(tracks_data)
            
        except Exception as e:
            print(f"❌ Failed to get recent tracks: {e}")
            return None
    
    def save_to_csv(self, df, filename="recent_tracks.csv"):
        """Save DataFrame to CSV file"""
        if df is not None and not df.empty:
            df.to_csv(filename, index=False)
            print(f"✅ Saved {len(df)} tracks to {filename}")
        else:
            print("❌ No data to save")

def test_extractor():
    """Test the Spotify extractor"""
    print("🧪 Testing Spotify Extractor")
    print("=" * 30)
    
    extractor = SpotifyExtractor()
    
    if extractor.sp:
        # Get recent tracks
        df = extractor.get_recent_tracks(limit=10)
        
        if df is not None and not df.empty:
            print(f"✅ Successfully extracted {len(df)} tracks")
            print("\n📊 Sample data:")
            print(df[['track_name', 'artist_name', 'played_at']].head())
            
            # Save to CSV
            extractor.save_to_csv(df, "day2_test_tracks.csv")
            
            return True
        else:
            print("❌ No tracks retrieved")
            return False
    else:
        print("❌ Extractor setup failed")
        return False

if __name__ == "__main__":
    test_extractor()