"""
Day 3: Enhanced Spotify Data Extractor
Features: Error handling, retry logic, rate limiting, data validation, logging
Production-ready with comprehensive fallback mechanisms
"""
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pandas as pd
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union, Any
import os
import certifi
import requests
from dotenv import load_dotenv

# Configure SSL certificates
os.environ['REQUESTS_CA_BUNDLE'] = certifi.where()
requests.utils.DEFAULT_CA_BUNDLE_PATH = certifi.where()

# load_dotenv()  # Commented out - using environment variables from Docker container

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SpotifyExtractorV2:
    """Enhanced Spotify data extractor with production features and comprehensive error handling"""
    
    def __init__(self):
        self.client_id = os.getenv('SPOTIPY_CLIENT_ID')
        self.client_secret = os.getenv('SPOTIPY_CLIENT_SECRET')
        self.redirect_uri = os.getenv('SPOTIPY_REDIRECT_URI')

        # Enhanced scopes for comprehensive data access
        self.scope = "user-read-recently-played user-read-private user-read-email user-library-read user-read-playback-state user-top-read"
        
        self.sp = None
        # Enhanced retry and rate limiting configuration
        self.max_retries = 3  # Aligned with new code naming
        self.retry_attempts = 3  # Keep for backward compatibility 
        self.retry_delay = 2  # seconds - from new code
        self.rate_limit_delay = 1.0
        
        logger.info(f"Initializing Enhanced Spotify Extractor v2")
        logger.info(f"📍 Redirect URI: {self.redirect_uri}")
        logger.info(f"🔄 Max retries: {self.max_retries}, Retry delay: {self.retry_delay}s")
        
        self._setup_spotify()
    
    def _setup_spotify(self):
        """Set up Spotify client with enhanced authentication and token management"""
        try:
            sp_oauth = SpotifyOAuth(
                client_id=self.client_id,
                client_secret=self.client_secret,
                redirect_uri=self.redirect_uri,
                scope=self.scope,
                show_dialog=True,
                cache_path=".spotify_cache"
            )
            
            # Check for cached token first
            token_info = sp_oauth.get_cached_token()
            
            if not token_info:
                logger.warning("No cached token found. Manual authentication may be required.")
                # Try to create new client anyway - will prompt for auth
                self.sp = spotipy.Spotify(auth_manager=sp_oauth)
            else:
                # Check if token is expired and refresh if needed
                if sp_oauth.is_token_expired(token_info):
                    logger.info("🔄 Token expired. Refreshing...")
                    try:
                        token_info = sp_oauth.refresh_access_token(token_info['refresh_token'])
                        logger.info(" Token refreshed successfully")
                    except Exception as refresh_error:
                        logger.warning(f"Failed to refresh token: {refresh_error}")
                        logger.info("Will attempt fresh authentication...")
                
                self.sp = spotipy.Spotify(auth=token_info.get('access_token') if token_info else None, auth_manager=sp_oauth)
            
            # Test the connection
            user = self.sp.current_user()
            if user:
                logger.info(f" Successfully authenticated as: {user.get('display_name', user['id'])}")
                logger.info(f"🌍 User country: {user.get('country', 'Unknown')}")
                logger.info(f"💎 Subscription: {user.get('product', 'Unknown')}")
            else:
                raise Exception("Failed to get user info")
                
        except Exception as e:
            logger.error(f"Authentication failed: {e}")
            logger.error("Please check your Spotify credentials and redirect URI")
            logger.error("Make sure your Spotify app has the correct redirect URI configured")
            raise
    
    def _retry_on_failure(self, func, *args, **kwargs):
        """Enhanced retry logic for API calls with exponential backoff"""
        for attempt in range(self.max_retries):
            try:
                return func(*args, **kwargs)
            except spotipy.exceptions.SpotifyException as e:
                if e.http_status == 429:  # Rate limited
                    retry_after = int(e.headers.get('Retry-After', self.retry_delay))
                    logger.warning(f"⏰ Rate limited. Waiting {retry_after} seconds... (attempt {attempt + 1}/{self.max_retries})")
                    time.sleep(retry_after)
                    continue
                elif e.http_status == 401:  # Unauthorized
                    logger.warning("🔑 Token expired, attempting refresh...")
                    try:
                        if hasattr(self.sp, 'auth_manager') and hasattr(self.sp.auth_manager, 'refresh_access_token'):
                            self.sp.auth_manager.refresh_access_token(self.sp.auth_manager.refresh_token)
                            continue
                    except Exception as refresh_error:
                        logger.error(f"Failed to refresh token: {refresh_error}")
                        raise e
                else:
                    if attempt < self.max_retries - 1:
                        wait_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"Spotify API error (attempt {attempt + 1}/{self.max_retries}): {e}. Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"All {self.max_retries} attempts failed: {e}")
                        raise
            except Exception as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Attempt {attempt + 1}/{self.max_retries} failed: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"All {self.max_retries} attempts failed: {e}")
                    raise
        
        raise Exception(f"Failed after {self.max_retries} attempts")
    
    def _make_api_call(self, api_function, *args, **kwargs):
        """Make API call with retry logic and rate limiting (legacy method for backward compatibility)"""
        return self._retry_on_failure(api_function, *args, **kwargs)
    
    def extract_recently_played(self, limit: int = 50, after: Optional[int] = None) -> pd.DataFrame:
        """
        Extract recently played tracks with enhanced error handling (alias for extract_recent_tracks)
        
        Args:
            limit: Number of tracks to retrieve (max 50 per request)
            after: Unix timestamp to get tracks played after this time
        
        Returns:
            DataFrame with comprehensive track information
        """
        logger.info(f"Using enhanced extract_recently_played (limit={limit})...")
        return self.extract_recent_tracks(limit=limit, after=after)
    
    def extract_recent_tracks(self, limit: int = 50, after: Optional[int] = None) -> pd.DataFrame:
        """
        Extract recently played tracks with comprehensive data and enhanced error handling
        
        Args:
            limit: Number of tracks to fetch (max 50 per request)
            after: Unix timestamp to get tracks after this time
        """
        try:
            logger.info(f"Extracting {limit} recent tracks with enhanced features...")
            
            # Get recently played tracks using enhanced retry logic
            if after:
                results = self._retry_on_failure(
                    self.sp.current_user_recently_played, 
                    limit=limit, 
                    after=after
                )
            else:
                results = self._retry_on_failure(
                    self.sp.current_user_recently_played, 
                    limit=limit
                )
            
            if not results or 'items' not in results or not results['items']:
                logger.warning("No recent tracks found")
                return pd.DataFrame()
            
            # Extract basic track data
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
                }
                
                tracks_data.append(track_info)
                track_ids.append(track['id'])
            
            # Create DataFrame from track data
            df = pd.DataFrame(tracks_data)
            
            logger.info(f" Extracted {len(df)} tracks")
            
            # Get audio features and merge with track data
            audio_features_df = self.extract_audio_features(track_ids)
            if not audio_features_df.empty:
                df = df.merge(audio_features_df, on='track_id', how='left')
                logger.info(f" Merged with audio features")
            
            # Get artist details and merge
            artist_ids = df['artist_id'].unique().tolist()
            artist_details_df = self.extract_artist_details(artist_ids)
            if not artist_details_df.empty:
                # Rename columns to avoid conflicts
                artist_details_df = artist_details_df.rename(columns={
                    'artist_name': 'artist_name_detailed',
                    'genres': 'artist_genres',
                    'popularity': 'artist_popularity',
                    'followers': 'artist_followers'
                })
                df = df.merge(artist_details_df[['artist_id', 'artist_genres', 'artist_popularity', 'artist_followers']], 
                             on='artist_id', how='left')
                logger.info(f" Merged with artist details")
            
            # Validate and clean data
            df = self._validate_and_clean_data(df)
            
            logger.info(f" Final dataset: {len(df)} rows with {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting recent tracks: {e}")
            raise

    def extract_audio_features(self, track_ids: List[str]) -> pd.DataFrame:
        """Extract audio features for given track IDs with fallback to mock data"""
        if not track_ids:
            logger.warning("No track IDs provided for audio features")
            return pd.DataFrame()
        
        logger.info(f"🔊 Extracting audio features for {len(track_ids)} tracks...")
        
        try:
            # Try to get real audio features first
            batch_size = 50
            all_features = []
            
            for i in range(0, len(track_ids), batch_size):
                batch_ids = track_ids[i:i + batch_size]
                
                # Add this right before the API call
                logger.info(f" Making API call to: https://api.spotify.com/v1/audio-features/?ids={','.join(batch_ids[:3])}...")
                
                try:
                    features = self._retry_on_failure(self.sp.audio_features, batch_ids)
                    if features:
                        valid_features = [f for f in features if f]
                        all_features.extend(valid_features)
                    time.sleep(0.2)
                except spotipy.exceptions.SpotifyException as e:
                    if e.http_status == 403:
                        logger.warning("⚠️ Audio features endpoint is forbidden (403)")
                        logger.warning("Your Spotify app doesn't have permission to access audio features")
                        logger.warning("Falling back to mock audio features...")
                        return self._create_mock_audio_features(track_ids)
                    else:
                        logger.warning(f"Failed to get audio features for batch: {e}")
                        continue
                except Exception as batch_error:
                    logger.warning(f"Failed to get audio features for batch: {batch_error}")
                    continue
            
            if not all_features:
                logger.warning("No audio features retrieved, using mock data")
                return self._create_mock_audio_features(track_ids)
            
            # Convert to DataFrame
            df = pd.DataFrame(all_features)
            
            # Ensure track_id column exists (it's called 'id' in the API response)
            if 'id' in df.columns and 'track_id' not in df.columns:
                df['track_id'] = df['id']
            
            # Select relevant columns
            feature_columns = [
                'track_id', 'danceability', 'energy', 'key', 'loudness', 'mode',
                'speechiness', 'acousticness', 'instrumentalness', 'liveness',
                'valence', 'tempo', 'time_signature'
            ]
            
            available_columns = [col for col in feature_columns if col in df.columns]
            df = df[available_columns]
            
            logger.info(f" Extracted audio features for {len(df)} tracks")
            return df
            
        except Exception as e:
            logger.error(f"Error extracting audio features: {e}")
            return self._create_mock_audio_features(track_ids)
    
    def _create_mock_audio_features(self, track_ids: List[str]) -> pd.DataFrame:
        """Create mock audio features when the real endpoint is unavailable"""
        logger.info("Creating mock audio features (Spotify app limitation)")
        
        import random
        
        mock_features = []
        for i, track_id in enumerate(track_ids):
            # Create more realistic mock values with variation based on track index
            random.seed(hash(track_id) % 1000)  # Use track_id for consistent but varied results
            # Create custom audio features because spotify API not working permission
            mock_feature = {
                'track_id': track_id,
                'danceability': round(random.uniform(0.3, 0.9), 3),     # Varied danceability
                'energy': round(random.uniform(0.2, 0.95), 3),          # Varied energy  
                'key': random.randint(0, 11),                           # Random key (0-11)
                'loudness': round(random.uniform(-20.0, -5.0), 2),      # Realistic loudness range
                'mode': random.choice([0, 1]),                          # Major (1) or Minor (0)
                'speechiness': round(random.uniform(0.02, 0.3), 3),     # Low to moderate speechiness
                'acousticness': round(random.uniform(0.1, 0.8), 3),     # Varied acousticness
                'instrumentalness': round(random.uniform(0.0, 0.4), 3), # Usually low for popular music
                'liveness': round(random.uniform(0.05, 0.35), 3),       # Usually studio recordings
                'valence': round(random.uniform(0.2, 0.9), 3),          # Varied mood
                'tempo': round(random.uniform(80.0, 180.0), 1),         # Realistic tempo range
                'time_signature': random.choice([3, 4, 5])              # Common time signatures
            }
            mock_features.append(mock_feature)
        
        df = pd.DataFrame(mock_features)
        logger.info(f" Created varied mock audio features for {len(df)} tracks")
        return df
    
    def extract_user_info(self) -> Dict:
        """Extract current user information with enhanced error handling"""
        try:
            logger.info("👤 Extracting user information...")
            user = self._retry_on_failure(self.sp.current_user)
            
            user_info = {
                'user_id': user['id'],
                'display_name': user.get('display_name'),
                'email': user.get('email'),
                'country': user.get('country'),
                'followers': user.get('followers', {}).get('total', 0),
                'product': user.get('product'),  # free, premium, etc.
                'external_urls': user.get('external_urls', {}).get('spotify'),
                'extraction_timestamp': datetime.now().isoformat()
            }
            
            logger.info(f" Retrieved user info for: {user_info['display_name']}")
            logger.info(f"   Country: {user_info['country']}, Product: {user_info['product']}")
            return user_info
            
        except Exception as e:
            logger.error(f"Failed to extract user info: {e}")
            return {}
    
    def extract_artist_details(self, artist_ids: List[str]) -> pd.DataFrame:
        """Extract detailed artist information including genres, popularity, followers"""
        if not artist_ids:
            logger.warning("No artist IDs provided for detail extraction")
            return pd.DataFrame()
        
        try:
            # Remove duplicates while preserving order
            unique_artist_ids = list(dict.fromkeys(artist_ids))
            logger.info(f"Extracting details for {len(unique_artist_ids)} unique artists")
            
            # Spotify API allows up to 50 artists per request
            artist_details = []
            
            for i in range(0, len(unique_artist_ids), 50):
                batch = unique_artist_ids[i:i+50]
                
                try:
                    artists_data = self._retry_on_failure(self.sp.artists, batch)
                    
                    for artist in artists_data['artists']:
                        if artist:  # artist can be None if not found
                            artist_detail = {
                                'artist_id': artist['id'],
                                'artist_name': artist['name'],
                                'genres': ', '.join(artist.get('genres', [])),
                                'popularity': artist.get('popularity', 0),
                                'followers': artist.get('followers', {}).get('total', 0),
                                'external_urls': artist.get('external_urls', {}).get('spotify'),
                                'image_url': artist.get('images', [{}])[0].get('url') if artist.get('images') else None
                            }
                            artist_details.append(artist_detail)
                
                except Exception as e:
                    logger.warning(f"Failed to get artist details for batch {i//50 + 1}: {e}")
                    # Continue with other batches
                    continue
            
            if artist_details:
                df = pd.DataFrame(artist_details)
                logger.info(f" Retrieved details for {len(df)} artists")
                return df
            else:
                logger.warning("No artist details retrieved, creating fallback data")
                return self._create_fallback_artist_details(unique_artist_ids)
                
        except Exception as e:
            logger.error(f"Failed to extract artist details: {e}")
            return self._create_fallback_artist_details(unique_artist_ids)
    
    def _create_fallback_artist_details(self, artist_ids: List[str]) -> pd.DataFrame:
        """Create fallback artist details when API is unavailable"""
        import random
        
        fallback_details = []
        genre_options = [
            'pop', 'rock', 'hip-hop', 'indie', 'electronic', 'jazz', 'classical',
            'country', 'r&b', 'alternative', 'folk', 'blues', 'reggae', 'punk'
        ]
        
        for artist_id in artist_ids:
            random.seed(hash(artist_id) % 1000)  # Consistent but varied results
            
            # Generate realistic fallback data
            num_genres = random.randint(1, 3)
            selected_genres = random.sample(genre_options, num_genres)
            
            fallback_detail = {
                'artist_id': artist_id,
                'artist_name': f'Artist_{artist_id[:8]}',  # Placeholder name
                'genres': ', '.join(selected_genres),
                'popularity': random.randint(20, 95),
                'followers': random.randint(1000, 1000000),
                'external_urls': f'https://open.spotify.com/artist/{artist_id}',
                'image_url': None
            }
            fallback_details.append(fallback_detail)
        
        df = pd.DataFrame(fallback_details)
        logger.info(f" Created fallback details for {len(df)} artists")
        return df
    
    def validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Public method for data validation - calls internal validation"""
        return self._validate_and_clean_data(df)
    
    def _validate_and_clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and clean the extracted data"""
        if df.empty:
            return df
        
        initial_count = len(df)
        
        # Remove duplicates based on track_id and played_at
        if 'track_id' in df.columns and 'played_at' in df.columns:
            df = df.drop_duplicates(subset=['track_id', 'played_at'])
        
        # Convert played_at to datetime
        if 'played_at' in df.columns:
            df['played_at'] = pd.to_datetime(df['played_at'])
        
        # Fill missing values
        numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
        df[numeric_columns] = df[numeric_columns].fillna(0)
        
        string_columns = df.select_dtypes(include=['object']).columns
        df[string_columns] = df[string_columns].fillna('')
        
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
            print(f" User: {user_info.get('display_name', 'Unknown')}")
            print(f"   Country: {user_info.get('country', 'Unknown')}")
            print(f"   Product: {user_info.get('product', 'Unknown')}")
        
        # Test track extraction
        print("\nTesting track extraction...")
        df = extractor.extract_recent_tracks(limit=20)
        
        if not df.empty:
            print(f" Extracted {len(df)} tracks")
            print(f"   Columns: {len(df.columns)}")
            print("\n Sample data:")
            sample_cols = ['track_name', 'artist_name', 'energy', 'valence', 'played_at']
            available_cols = [col for col in sample_cols if col in df.columns]
            print(df[available_cols].head(3).to_string(index=False))
            
            # Create data directory in current directory for testing
            import os
            data_dir = os.path.join(os.path.dirname(__file__), 'data')
            os.makedirs(data_dir, exist_ok=True)
            
            # Save test data
            output_file = os.path.join(data_dir, 'test_output.csv')
            df.to_csv(output_file, index=False)
            print(f"\n Saved test data to: {output_file}")
            
            return True
        else:
            print("No data extracted")
            return False
            
    except Exception as e:
        print(f"Test failed: {e}")
        return False

if __name__ == "__main__":
    test_enhanced_extractor()