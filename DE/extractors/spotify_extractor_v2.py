"""
Day 3: Enhanced Spotify Data Extractor
Features: Error handling, rate limiting, data validation, logging
"""
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union
import os
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SpotifyExtractorV2:
    """Enhanced Spotify data extractor with production features"""
    
    def __init__(self):
        self.client_id = os.getenv('SPOTIFY_CLIENT_ID')
        self.client_secret = os.getenv('SPOTIFY_CLIENT_SECRET')
        self.redirect_uri = os.getenv('SPOTIFY_REDIRECT_URI', 'https://datapotipy.com/callback')
        
        # Enhanced scopes for more data
        self.scope = "user-read-recently-played user-read-playback-state user-top-read user-library-read playlist-read-private"
        
        self.sp = None
        self.max_retries = 3
        self.retry_delay = 1  # seconds
        
        self._setup_spotify()
    
    def _setup_spotify(self):
        """Set up Spotify client with error handling"""
        try:
            sp_oauth = SpotifyOAuth(
                client_id=self.client_id,
                client_secret=self.client_secret,
                redirect_uri=self.redirect_uri,
                scope=self.scope,
                cache_path='.spotify_cache'
            )
            
            token_info = sp_oauth.get_cached_token()
            if not token_info or sp_oauth.is_token_expired(token_info):
                if token_info:
                    # Refresh expired token
                    token_info = sp_oauth.refresh_access_token(token_info['refresh_token'])
                else:
                    # Need new authentication
                    logger.warning("Authentication required")
                    auth_url = sp_oauth.get_authorize_url()
                    logger.info(f"Visit: {auth_url}")
                    response = input("Paste redirect URL: ")
                    code = sp_oauth.parse_response_code(response)
                    token_info = sp_oauth.get_access_token(code)
            
            self.sp = spotipy.Spotify(auth=token_info['access_token'])
            logger.info("✅ Spotify client initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Spotify setup failed: {e}")
            raise
    
    def _make_api_call(self, api_func, *args, **kwargs):
        """Make API call with retry logic and rate limiting"""
        for attempt in range(self.max_retries):
            try:
                # Rate limiting - Spotify allows 100 requests per minute
                time.sleep(0.6)  # ~100 requests per minute
                
                result = api_func(*args, **kwargs)
                return result
                
            except spotipy.exceptions.SpotifyException as e:
                if e.http_status == 429:  # Rate limited
                    retry_after = int(e.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                elif e.http_status == 401:  # Unauthorized
                    logger.error("Token expired. Please re-authenticate.")
                    raise
                else:
                    logger.warning(f"API error (attempt {attempt + 1}): {e}")
                    if attempt == self.max_retries - 1:
                        raise
                    time.sleep(self.retry_delay * (2 ** attempt))  # Exponential backoff
                    
            except Exception as e:
                logger.warning(f"Unexpected error (attempt {attempt + 1}): {e}")
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(self.retry_delay)
        
        raise Exception(f"Failed after {self.max_retries} attempts")
    
    def extract_recent_tracks(self, limit: int = 50, after: Optional[int] = None) -> pd.DataFrame:
        """Extract recently played tracks with enhanced data"""
        logger.info(f"Extracting {limit} recent tracks...")
        
        try:
            # Get recently played tracks
            if after:
                results = self._make_api_call(
                    self.sp.current_user_recently_played, 
                    limit=limit, 
                    after=after
                )
            else:
                results = self._make_api_call(
                    self.sp.current_user_recently_played, 
                    limit=limit
                )
            
            if not results or not results.get('items'):
                logger.warning("No recent tracks found")
                return pd.DataFrame()
            
            tracks_data = []
            track_ids = []
            
            for item in results['items']:
                track = item['track']
                
                # Basic track info
                track_info = {
                    'track_id': track['id'],
                    'track_name': track['name'],
                    'artist_id': track['artists'][0]['id'],
                    'artist_name': track['artists'][0]['name'],
                    'album_id': track['album']['id'],
                    'album_name': track['album']['name'],
                    'played_at': item['played_at'],
                    'duration_ms': track['duration_ms'],
                    'popularity': track['popularity'],
                    'explicit': track['explicit'],
                    'preview_url': track.get('preview_url'),
                    'release_date': track['album'].get('release_date'),
                    'album_type': track['album'].get('album_type'),
                    'total_tracks': track['album'].get('total_tracks')
                }
                
                tracks_data.append(track_info)
                track_ids.append(track['id'])
            
            df = pd.DataFrame(tracks_data)
            
            # Get audio features for all tracks
            logger.info("Getting audio features...")
            audio_features = self.extract_audio_features(track_ids)
            
            # Merge with audio features
            if not audio_features.empty:
                df = df.merge(audio_features, on='track_id', how='left')
            
            logger.info(f"✅ Extracted {len(df)} tracks with full metadata")
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to extract recent tracks: {e}")
            return pd.DataFrame()
    
    def extract_audio_features(self, track_ids: List[str]) -> pd.DataFrame:
        """Extract audio features for tracks"""
        if not track_ids:
            return pd.DataFrame()
        
        logger.info(f"Getting audio features for {len(track_ids)} tracks...")
        
        try:
            all_features = []
            
            # Process in batches of 100 (API limit)
            for i in range(0, len(track_ids), 100):
                batch_ids = track_ids[i:i+100]
                
                features = self._make_api_call(self.sp.audio_features, batch_ids)
                
                if features:
                    # Filter out None responses
                    valid_features = [f for f in features if f is not None]
                    all_features.extend(valid_features)
            
            if not all_features:
                logger.warning("No audio features retrieved")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(all_features)
            
            # Keep only relevant columns
            feature_columns = [
                'id', 'danceability', 'energy', 'key', 'loudness', 'mode',
                'speechiness', 'acousticness', 'instrumentalness', 'liveness',
                'valence', 'tempo', 'time_signature'
            ]
            
            df = df[feature_columns].rename(columns={'id': 'track_id'})
            
            logger.info(f"✅ Retrieved audio features for {len(df)} tracks")
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to extract audio features: {e}")
            return pd.DataFrame()
    
    def extract_user_info(self) -> Dict:
        """Extract current user information"""
        try:
            user = self._make_api_call(self.sp.current_user)
            
            user_info = {
                'user_id': user['id'],
                'display_name': user.get('display_name'),
                'email': user.get('email'),
                'country': user.get('country'),
                'followers': user.get('followers', {}).get('total', 0),
                'product': user.get('product')  # free, premium, etc.
            }
            
            logger.info(f"✅ Retrieved user info for: {user_info['display_name']}")
            return user_info
            
        except Exception as e:
            logger.error(f"❌ Failed to extract user info: {e}")
            return {}
    
    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean extracted data"""
        if df.empty:
            return df
        
        initial_count = len(df)
        
        # Remove duplicates based on track_id and played_at
        if 'played_at' in df.columns:
            df = df.drop_duplicates(subset=['track_id', 'played_at'])
        else:
            df = df.drop_duplicates(subset=['track_id'])
        
        # Remove rows with missing essential data
        essential_columns = ['track_id', 'track_name', 'artist_name']
        df = df.dropna(subset=essential_columns)
        
        final_count = len(df)
        
        if final_count != initial_count:
            logger.info(f"Data validation: {initial_count} → {final_count} rows")
        
        return df

def test_enhanced_extractor():
    """Test the enhanced extractor"""
    print("🧪 Testing Enhanced Spotify Extractor")
    print("=" * 40)
    
    try:
        extractor = SpotifyExtractorV2()
        
        # Test user info extraction
        print("\n👤 Testing user info extraction...")
        user_info = extractor.extract_user_info()
        if user_info:
            print(f"✅ User: {user_info.get('display_name', 'Unknown')}")
            print(f"   Country: {user_info.get('country', 'Unknown')}")
            print(f"   Product: {user_info.get('product', 'Unknown')}")
        
        # Test track extraction
        print("\n🎵 Testing track extraction...")
        df = extractor.extract_recent_tracks(limit=20)
        
        if not df.empty:
            print(f"✅ Extracted {len(df)} tracks")
            print(f"   Columns: {len(df.columns)}")
            print("\n📊 Sample data:")
            sample_cols = ['track_name', 'artist_name', 'energy', 'valence', 'played_at']
            available_cols = [col for col in sample_cols if col in df.columns]
            print(df[available_cols].head(3).to_string(index=False))
            
            # Save test data
            df.to_csv('day3_enhanced_extraction_test.csv', index=False)
            print(f"\n💾 Saved test data to: day3_enhanced_extraction_test.csv")
            
            return True
        else:
            print("❌ No data extracted")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    test_enhanced_extractor()