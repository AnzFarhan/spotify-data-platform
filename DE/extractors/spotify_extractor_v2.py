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
import certifi
import requests
from dotenv import load_dotenv

# Configure SSL certificates
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
requests.utils.DEFAULT_CA_BUNDLE_PATH = certifi.where()

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
        
        # Enhanced scopes for more data - reduced to essential scopes
        self.scope = "user-read-recently-played user-read-private"
        
        self.sp = None
        self.max_retries = 3
        self.retry_delay = 1  # seconds
        
        self._setup_spotify()
    
    def _setup_spotify(self):
        """Set up Spotify client with error handling"""
        try:
            # Configure session with proper SSL certificates
            session = requests.Session()
            session.verify = certifi.where()
            
            # Clear any existing cached token
            if os.path.exists('.spotify_cache'):
                os.remove('.spotify_cache')
            
            # Create OAuth manager with all required scopes
            sp_oauth = SpotifyOAuth(
                client_id=self.client_id,
                client_secret=self.client_secret,
                redirect_uri=self.redirect_uri,
                scope=self.scope,
                cache_path='.spotify_cache',
                requests_session=session,
                open_browser=False,  # Prevent automatic browser opening
                show_dialog=True    # Force re-consent to ensure proper scopes
            )
            
            # Create Spotify client with OAuth manager
            self.sp = spotipy.Spotify(auth_manager=sp_oauth)
            
            # Test connection by getting current user
            user = self.sp.current_user()
            logger.info(f"✅ Connected as: {user.get('display_name', 'Unknown user')}")
            logger.info("✅ Spotify client initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Spotify setup failed: {e}")
            raise
    
    def _make_api_call(self, api_func, *args, **kwargs):
        """Make API call with retry logic and rate limiting"""
        for attempt in range(self.max_retries):
            try:
                # Ensure token is fresh before each API call
                if hasattr(self.sp, 'auth_manager'):
                    token_info = self.sp.auth_manager.get_cached_token()
                    if token_info and self.sp.auth_manager.is_token_expired(token_info):
                        logger.debug("Token expired, refreshing...")
                        token_info = self.sp.auth_manager.refresh_access_token(token_info['refresh_token'])
                        self.sp = spotipy.Spotify(auth=token_info['access_token'])
                
                result = api_func(*args, **kwargs)
                
                # Rate limiting - conservative approach
                time.sleep(0.1)  # Small delay between successful requests
                
                return result
                
            except spotipy.exceptions.SpotifyException as e:
                if e.http_status == 429:  # Rate limited
                    retry_after = int(e.headers.get('Retry-After', 60))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                elif e.http_status == 401:  # Token expired
                    if hasattr(self.sp, 'auth_manager') and attempt < self.max_retries - 1:
                        try:
                            logger.info("Token expired during request, refreshing...")
                            token_info = self.sp.auth_manager.refresh_access_token(token_info['refresh_token'])
                            self.sp = spotipy.Spotify(auth=token_info['access_token'])
                            continue
                        except Exception as refresh_error:
                            logger.error(f"Failed to refresh token: {refresh_error}")
                    raise
                elif e.http_status == 403:  # Forbidden - might need scope adjustment
                    logger.error(f"Access forbidden to endpoint. Scope issue? Error: {e}")
                    if attempt == self.max_retries - 1:
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
            
            # Try using tracks API endpoint instead of audio-features
            for track_id in track_ids:
                try:
                    track = self._make_api_call(self.sp.track, track_id)
                    if track:
                        features = {
                            'id': track['id'],
                            'popularity': track['popularity'],
                            'duration_ms': track['duration_ms'],
                            'explicit': int(track['explicit']),
                            'preview_url': 1 if track.get('preview_url') else 0,
                            'external_urls': track.get('external_urls', {}).get('spotify', ''),
                            'available_markets': len(track.get('available_markets', [])),
                            'track_href': track.get('href', ''),
                            'album_type': track['album'].get('album_type', ''),
                            'total_tracks': track['album'].get('total_tracks', 0)
                        }
                        all_features.append(features)
                        logger.debug(f"Got track data for {track_id}")
                    time.sleep(0.2)  # Small delay between requests
                except Exception as track_error:
                    logger.warning(f"Failed to get track data for {track_id}: {track_error}")
                    continue
                except Exception as e:
                    logger.warning(f"Failed to get features for track {track_id}: {e}")
                    continue
            
            if not all_features:
                logger.warning("No audio features retrieved")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(all_features)
            
            # Keep only relevant columns
            feature_columns = [
                'id', 'popularity', 'duration_ms', 'explicit', 'preview_url',
                'external_urls', 'available_markets', 'track_href',
                'album_type', 'total_tracks'
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
            
            # Create data directory if it doesn't exist
            import os
            data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data')
            os.makedirs(data_dir, exist_ok=True)
            
            # Save test data
            output_file = os.path.join(data_dir, 'day3_enhanced_extraction_test.csv')
            df.to_csv(output_file, index=False)
            print(f"\n💾 Saved test data to: {output_file}")
            
            return True
        else:
            print("❌ No data extracted")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    test_enhanced_extractor()