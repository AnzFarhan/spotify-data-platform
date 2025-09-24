"""
Enhanced Data Transformation Pipeline for Spotify Data
Features: Advanced cleaning, normalization, feature engineering, and analytics
Optimized for spotify_extractor_v2.py data structure with comprehensive analytics
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
import re
from typing import Dict, List, Optional, Union, Any, Tuple
import logging
from collections import Counter
import warnings

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

warnings.filterwarnings('ignore', category=UserWarning)

# Import the enhanced transformer for full functionality
class SpotifyDataTransformer:
    """
    Enhanced transformer for Spotify data with comprehensive feature engineering.
    Specifically optimized for data structure from spotify_extractor_v2.py extractor.
    Provides backward compatibility while offering advanced analytics capabilities.
    """
    """
    Enhanced transformer for Spotify data with comprehensive feature engineering.
    Specifically optimized for data structure from spotify_extractor_v2.py extractor.
    """
    
    def __init__(self):
        """Initialize the transformer with Spotify-specific configurations"""
        # Core audio feature columns from Spotify Web API
        self.audio_feature_columns = [
            'danceability', 'energy', 'key', 'loudness', 'mode',
            'speechiness', 'acousticness', 'instrumentalness', 
            'liveness', 'valence', 'tempo', 'time_signature'
        ]
        
        # Track metadata columns from spotify_extractor_v2.py
        self.track_metadata_columns = [
            'track_id', 'track_name', 'artist_id', 'artist_name',
            'album_id', 'album_name', 'played_at', 'duration_ms',
            'popularity', 'explicit', 'preview_url', 'release_date',
            'album_type', 'total_tracks'
        ]
        
        # Valid ranges for audio features (for validation and outlier detection)
        self.feature_ranges = {
            'danceability': (0.0, 1.0),
            'energy': (0.0, 1.0),
            'key': (0, 11),
            'loudness': (-60.0, 0.0),
            'mode': (0, 1),
            'speechiness': (0.0, 1.0),
            'acousticness': (0.0, 1.0),
            'instrumentalness': (0.0, 1.0),
            'liveness': (0.0, 1.0),
            'valence': (0.0, 1.0),
            'tempo': (0.0, 300.0),
            'time_signature': (3, 7)
        }
        
        # Musical key mappings for better interpretation
        self.key_mappings = {
            0: 'C', 1: 'C#/Db', 2: 'D', 3: 'D#/Eb', 4: 'E', 5: 'F',
            6: 'F#/Gb', 7: 'G', 8: 'G#/Ab', 9: 'A', 10: 'A#/Bb', 11: 'B'
        }
        
        # Mode mappings
        self.mode_mappings = {0: 'Minor', 1: 'Major'}
        
        # Album type categories for standardization
        self.album_types = ['album', 'single', 'compilation']
        
        # Genre inference patterns (based on audio features)
        self.genre_inference_patterns = {
            'Electronic/Dance': {'danceability': 0.7, 'energy': 0.7, 'acousticness': 0.2},
            'Hip Hop/Rap': {'speechiness': 0.33, 'energy': 0.6, 'danceability': 0.6},
            'Classical/Instrumental': {'instrumentalness': 0.8, 'acousticness': 0.7},
            'Jazz': {'acousticness': 0.5, 'instrumentalness': 0.3, 'energy': 0.4},
            'Rock': {'energy': 0.7, 'loudness': -10, 'acousticness': 0.3},
            'Pop': {'danceability': 0.6, 'energy': 0.6, 'valence': 0.5},
            'Ambient/Chill': {'energy': 0.3, 'valence': 0.4, 'acousticness': 0.5}
        }
    
    def transform_spotify_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Main transformation pipeline for Spotify data from spotify_extractor_v2.py
        
        Args:
            df: Raw DataFrame from spotify_extractor_v2.py
            
        Returns:
            Fully transformed and enhanced DataFrame
        """
        logger.info(f"🚀 Starting enhanced transformation for {len(df)} tracks...")
        
        # Make a copy to avoid modifying original data
        transformed_df = df.copy()
        
        # Core transformation steps
        transformed_df = self._clean_and_validate_data(transformed_df)
        transformed_df = self._normalize_timestamps(transformed_df)
        transformed_df = self._process_audio_features(transformed_df)
        transformed_df = self._create_derived_features(transformed_df)
        transformed_df = self._add_listening_analytics(transformed_df)
        transformed_df = self._categorize_and_classify(transformed_df)
        transformed_df = self._handle_missing_values(transformed_df)
        
        logger.info(f"✅ Enhanced transformation completed. Output: {len(transformed_df)} records with {len(transformed_df.columns)} features")
        return transformed_df
    
    def _clean_and_validate_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate core data fields"""
        logger.info("🧹 Cleaning and validating data fields...")
        
        # Clean text fields
        text_columns = ['track_name', 'artist_name', 'album_name']
        for col in text_columns:
            if col in df.columns:
                # Handle None values and clean text
                df[col] = df[col].fillna('Unknown').astype(str)
                df[col] = df[col].str.strip()
                df[col] = df[col].str.replace(r'\s+', ' ', regex=True)  # Multiple spaces
                df[col] = df[col].str.replace(r'[^\w\s\-\'\.\(\)&]', '', regex=True)  # Special chars
        
        # Validate and clean IDs
        id_columns = ['track_id', 'artist_id', 'album_id']
        for col in id_columns:
            if col in df.columns:
                df[col] = df[col].fillna('unknown').astype(str)
                df[col] = df[col].str.strip()
        
        # Clean duration_ms
        if 'duration_ms' in df.columns:
            df['duration_ms'] = pd.to_numeric(df['duration_ms'], errors='coerce')
            df['duration_ms'] = df['duration_ms'].fillna(180000)  # Default 3 minutes
            df['duration_ms'] = np.clip(df['duration_ms'], 1000, 3600000)  # 1s to 1h
        
        # Clean popularity
        if 'popularity' in df.columns:
            df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce')
            df['popularity'] = df['popularity'].fillna(0)
            df['popularity'] = np.clip(df['popularity'], 0, 100)
        
        # Handle boolean fields
        if 'explicit' in df.columns:
            df['explicit'] = df['explicit'].fillna(False).astype(bool)
        
        return df
    
    def _normalize_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize and enhance timestamp data"""
        logger.info("⏰ Processing timestamps and temporal features...")
        
        if 'played_at' in df.columns:
            # Parse played_at timestamps
            df['played_at'] = pd.to_datetime(df['played_at'], utc=True, errors='coerce')
            df['played_at'] = df['played_at'].fillna(datetime.now(timezone.utc))
            
            # Create temporal features
            df['play_hour'] = df['played_at'].dt.hour
            df['play_day_of_week'] = df['played_at'].dt.dayofweek  # 0=Monday
            df['play_month'] = df['played_at'].dt.month
            df['play_year'] = df['played_at'].dt.year
            df['play_date'] = df['played_at'].dt.date
            
            # Time of day categories
            df['time_of_day'] = pd.cut(df['play_hour'], 
                                     bins=[0, 6, 12, 18, 24], 
                                     labels=['Night', 'Morning', 'Afternoon', 'Evening'],
                                     include_lowest=True)
        
        # Process release_date if available
        if 'release_date' in df.columns:
            df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
            df['release_year'] = df['release_date'].dt.year
            df['release_decade'] = (df['release_year'] // 10) * 10
            
            # Calculate track age
            current_year = datetime.now().year
            df['track_age_years'] = current_year - df['release_year'].fillna(current_year)
            df['track_age_years'] = np.clip(df['track_age_years'], 0, 100)
        
        return df
    
    def _process_audio_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process and validate audio features with advanced transformations"""
        logger.info("🎵 Processing audio features...")
        
        # Validate audio features ranges
        for feature, (min_val, max_val) in self.feature_ranges.items():
            if feature in df.columns:
                df[feature] = pd.to_numeric(df[feature], errors='coerce')
                
                # Handle outliers
                df[feature] = np.clip(df[feature], min_val, max_val)
                
                # Fill missing values with median
                median_val = df[feature].median()
                if pd.isna(median_val):  # If all are NaN, use midpoint
                    median_val = (min_val + max_val) / 2
                df[feature] = df[feature].fillna(median_val)
        
        # Create normalized audio feature groups
        if all(col in df.columns for col in ['danceability', 'energy', 'valence']):
            df['mood_score'] = (df['danceability'] + df['energy'] + df['valence']) / 3
            df['mood_category'] = pd.cut(df['mood_score'],
                                       bins=[0, 0.3, 0.7, 1.0],
                                       labels=['Sad/Calm', 'Neutral', 'Happy/Energetic'])
        
        # Acoustic vs Electronic spectrum
        if 'acousticness' in df.columns:
            df['acoustic_electronic'] = pd.cut(df['acousticness'],
                                              bins=[0, 0.33, 0.67, 1.0],
                                              labels=['Electronic', 'Mixed', 'Acoustic'])
        
        # Vocal content analysis
        if all(col in df.columns for col in ['speechiness', 'instrumentalness']):
            df['vocal_content'] = np.where(df['speechiness'] > 0.33, 'Speech/Rap',
                                 np.where(df['instrumentalness'] > 0.5, 'Instrumental', 'Vocal'))
        
        # Musical key and mode interpretations
        if 'key' in df.columns:
            df['key_name'] = df['key'].map(self.key_mappings).fillna('Unknown')
        
        if 'mode' in df.columns:
            df['mode_name'] = df['mode'].map(self.mode_mappings).fillna('Unknown')
        
        return df
    
    def _create_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create advanced derived features from base audio features"""
        logger.info("🔧 Creating derived features...")
        
        # Duration-based features
        if 'duration_ms' in df.columns:
            df['duration_minutes'] = df['duration_ms'] / 60000
            df['duration_category'] = pd.cut(df['duration_minutes'],
                                           bins=[0, 2, 4, 6, float('inf')],
                                           labels=['Short', 'Medium', 'Long', 'Very Long'])
        
        # Popularity-based features
        if 'popularity' in df.columns:
            df['popularity_tier'] = pd.cut(df['popularity'],
                                         bins=[0, 20, 50, 80, 100],
                                         labels=['Niche', 'Moderate', 'Popular', 'Hit'])
        
        # Album context features
        if 'total_tracks' in df.columns:
            df['album_size_category'] = pd.cut(df['total_tracks'].fillna(1),
                                             bins=[0, 3, 8, 15, float('inf')],
                                             labels=['Single', 'EP', 'Album', 'Extended'])
        
        # Audio feature combinations for genre inference
        audio_features = [col for col in self.audio_feature_columns if col in df.columns]
        if len(audio_features) >= 6:  # Minimum features for analysis
            # Energy-Valence quadrants (mood mapping)
            if all(col in df.columns for col in ['energy', 'valence']):
                df['energy_valence_quad'] = np.where(
                    (df['energy'] >= 0.5) & (df['valence'] >= 0.5), 'High Energy Happy',
                    np.where((df['energy'] >= 0.5) & (df['valence'] < 0.5), 'High Energy Sad',
                    np.where((df['energy'] < 0.5) & (df['valence'] >= 0.5), 'Low Energy Happy',
                    'Low Energy Sad')))
            
            # Danceability-Energy for activity mapping
            if all(col in df.columns for col in ['danceability', 'energy']):
                df['activity_score'] = (df['danceability'] + df['energy']) / 2
                df['activity_level'] = pd.cut(df['activity_score'],
                                            bins=[0, 0.3, 0.7, 1.0],
                                            labels=['Chill', 'Moderate', 'Active'])
        
        return df
    
    def _add_listening_analytics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add listening behavior analytics"""
        logger.info("📊 Adding listening analytics...")
        
        # Listening frequency by artist/album
        if 'artist_name' in df.columns:
            artist_counts = df['artist_name'].value_counts()
            df['artist_play_frequency'] = df['artist_name'].map(artist_counts)
            df['is_favorite_artist'] = df['artist_play_frequency'] > df['artist_play_frequency'].quantile(0.8)
        
        if 'album_name' in df.columns:
            album_counts = df['album_name'].value_counts()
            df['album_play_frequency'] = df['album_name'].map(album_counts)
        
        # Listening patterns by time
        if 'played_at' in df.columns:
            # Sort by played_at for sequence analysis
            df = df.sort_values('played_at')
            
            # Calculate time between plays
            df['time_since_last_play'] = df['played_at'].diff().dt.total_seconds() / 60  # minutes
            df['time_since_last_play'] = df['time_since_last_play'].fillna(0)
            
            # Listening session detection (gap > 30 minutes = new session)
            df['new_session'] = df['time_since_last_play'] > 30
            df['session_id'] = df['new_session'].cumsum()
        
        return df
    
    def _categorize_and_classify(self, df: pd.DataFrame) -> pd.DataFrame:
        """Advanced categorization and genre inference"""
        logger.info("🎯 Applying categorization and classification...")
        
        # Infer genre based on audio features
        if len([col for col in self.audio_feature_columns if col in df.columns]) >= 8:
            df['inferred_genre'] = df.apply(self._infer_genre_from_features, axis=1)
        
        # Explicit content analysis
        if 'explicit' in df.columns:
            explicit_rate = df.groupby('artist_name')['explicit'].mean() if 'artist_name' in df.columns else pd.Series()
            if not explicit_rate.empty:
                df['artist_explicit_rate'] = df['artist_name'].map(explicit_rate).fillna(0)
        
        # Recency classification
        if 'release_year' in df.columns:
            current_year = datetime.now().year
            df['release_era'] = pd.cut(df['release_year'].fillna(2000),
                                     bins=[1900, 1980, 1990, 2000, 2010, 2020, current_year + 1],
                                     labels=['Classic', '80s', '90s', '2000s', '2010s', 'Recent'])
        
        return df
    
    def _infer_genre_from_features(self, row: pd.Series) -> str:
        """Infer genre based on audio feature patterns"""
        scores = {}
        
        for genre, thresholds in self.genre_inference_patterns.items():
            score = 0
            total_features = len(thresholds)
            
            for feature, threshold in thresholds.items():
                if feature in row and not pd.isna(row[feature]):
                    # Calculate similarity to threshold (closer = higher score)
                    if feature == 'loudness':  # Special case for loudness (negative values)
                        similarity = 1 - abs(row[feature] - threshold) / 30  # Normalize by typical range
                    else:
                        similarity = 1 - abs(row[feature] - threshold)
                    score += max(0, similarity)
            
            scores[genre] = score / total_features if total_features > 0 else 0
        
        # Return genre with highest score, or 'Mixed' if no clear winner
        if scores:
            best_genre = max(scores.keys(), key=lambda x: scores[x])
            return best_genre if scores[best_genre] > 0.3 else 'Mixed'
        
        return 'Unknown'
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Final pass for handling any remaining missing values"""
        logger.info("🔍 Final missing value handling...")
        
        try:
            # Handle remaining missing values with appropriate defaults
            defaults = {
                'album_type': 'album',
                'total_tracks': 1,
                'inferred_genre': 'Unknown',
                'mood_category': 'Neutral',
                'activity_level': 'Moderate',
                'duration_category': 'Medium',
                'popularity_tier': 'Moderate'
            }
            
            # Handle preview_url separately since it should remain None when missing
            if 'preview_url' in df.columns:
                # preview_url can legitimately be None/NaN, so we leave it as is
                pass  # Keep preview URLs as None where missing - not all tracks have previews
            
            for col, default_val in defaults.items():
                if col in df.columns:
                    df[col] = df[col].fillna(default_val)
            
            # Handle any remaining NaN values in other columns
            for col in df.columns:
                if col != 'preview_url' and df[col].isnull().any():
                    if df[col].dtype == 'object':
                        df[col] = df[col].fillna('Unknown')
                    elif df[col].dtype in ['int64', 'float64']:
                        df[col] = df[col].fillna(0)
                    elif df[col].dtype == 'bool':
                        df[col] = df[col].fillna(False)
            
            # Report on data quality
            missing_counts = df.isnull().sum()
            if missing_counts.sum() > 0:
                logger.info(f"Final missing values (expected for preview_url): {missing_counts[missing_counts > 0].to_dict()}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error in missing value handling: {str(e)}")
            raise
    
    def get_transformation_summary(self, original_df: pd.DataFrame, transformed_df: pd.DataFrame) -> Dict[str, Any]:
        """Generate a comprehensive summary of the transformation process"""
        summary = {
            'input_records': len(original_df),
            'output_records': len(transformed_df),
            'input_columns': len(original_df.columns),
            'output_columns': len(transformed_df.columns),
            'new_features_created': len(transformed_df.columns) - len(original_df.columns),
            'audio_features_processed': len([col for col in self.audio_feature_columns if col in transformed_df.columns]),
            'data_quality': {
                'missing_values_remaining': transformed_df.isnull().sum().sum(),
                'complete_records': len(transformed_df.dropna()),
                'completion_rate': len(transformed_df.dropna()) / len(transformed_df) * 100
            },
            'feature_breakdown': {
                'temporal_features': len([col for col in transformed_df.columns if 'play_' in col or 'time_' in col or 'session' in col]),
                'derived_audio_features': len([col for col in transformed_df.columns if any(term in col for term in ['mood', 'activity', 'quad'])]),
                'categorical_features': len([col for col in transformed_df.columns if transformed_df[col].dtype == 'category']),
                'analytical_features': len([col for col in transformed_df.columns if 'frequency' in col or 'rate' in col])
            }
        }
        
        return summary
    
    def transform(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Legacy method for pipeline compatibility.
        Returns tuple of (transformed_data, quality_report)
        """
        transformed_df = self.transform_spotify_data(df)
        
        # Generate quality report
        quality_report = {
            'input_records': len(df),
            'output_records': len(transformed_df),
            'input_columns': len(df.columns),
            'output_columns': len(transformed_df.columns),
            'new_features_created': len(transformed_df.columns) - len(df.columns),
            'missing_values_remaining': transformed_df.isnull().sum().sum(),
            'completion_rate': len(transformed_df.dropna()) / len(transformed_df) * 100 if len(transformed_df) > 0 else 0
        }
        
        return transformed_df, quality_report


def main():
    """Test the enhanced transformer with sample data"""
    print("🧪 Testing Enhanced Spotify Data Transformer...")
    
    # Create sample data that matches spotify_extractor_v2.py structure
    sample_data = {
        'track_id': ['1', '2', '3'],
        'track_name': ['Test Song 1', 'Test Song 2', 'Test Song 3'],
        'artist_id': ['artist1', 'artist2', 'artist1'],
        'artist_name': ['Artist One', 'Artist Two', 'Artist One'],
        'album_id': ['album1', 'album2', 'album1'],
        'album_name': ['Test Album', 'Another Album', 'Test Album'],
        'played_at': ['2024-01-15T10:30:00Z', '2024-01-15T11:00:00Z', '2024-01-15T14:30:00Z'],
        'duration_ms': [180000, 210000, 195000],
        'popularity': [75, 60, 80],
        'explicit': [False, True, False],
        'danceability': [0.8, 0.6, 0.7],
        'energy': [0.9, 0.5, 0.8],
        'key': [5, 2, 7],
        'loudness': [-8.5, -12.0, -6.8],
        'mode': [1, 0, 1],
        'speechiness': [0.05, 0.4, 0.08],
        'acousticness': [0.1, 0.8, 0.2],
        'instrumentalness': [0.0, 0.0, 0.0],
        'liveness': [0.12, 0.3, 0.15],
        'valence': [0.85, 0.4, 0.9],
        'tempo': [128.0, 85.0, 140.0],
        'time_signature': [4, 4, 4]
    }
    
    df = pd.DataFrame(sample_data)
    transformer = SpotifyDataTransformer()
    
    print(f"📊 Input: {len(df)} records, {len(df.columns)} columns")
    transformed_df = transformer.transform_spotify_data(df)
    print(f"✅ Output: {len(transformed_df)} records, {len(transformed_df.columns)} columns")
    
    # Show summary
    summary = transformer.get_transformation_summary(df, transformed_df)
    print("\n📈 Transformation Summary:")
    for key, value in summary.items():
        if isinstance(value, dict):
            print(f"  {key}:")
            for sub_key, sub_value in value.items():
                print(f"    {sub_key}: {sub_value}")
        else:
            print(f"  {key}: {value}")
    
    print(f"\n🔍 New columns created: {list(set(transformed_df.columns) - set(df.columns))}")


if __name__ == "__main__":
    main()