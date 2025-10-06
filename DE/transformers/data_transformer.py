"""
Day 3: Data Transformation Pipeline
Features: Data cleaning, validation, normalization, feature engineering
Production-ready with comprehensive quality control
"""
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import logging
from typing import Dict, List, Optional, Tuple
import re

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SpotifyDataTransformer:
    """Transform and clean Spotify data for database loading with enhanced validation"""
    
    def __init__(self):
        self.genre_mappings = self._load_genre_mappings()
        # Add required columns for validation (from new code)
        self.required_track_columns = [
            'track_id', 'track_name', 'artist_id', 'album_id',
            'duration_ms', 'popularity', 'explicit'
        ]
        self.required_artist_columns = ['artist_id', 'name']
        logger.info(" SpotifyDataTransformer initialized with enhanced validation")
    
    def clean_tracks_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and validate tracks data (compatibility method for DAG integration)
        
        Args:
            df: Raw tracks DataFrame
        
        Returns:
            Cleaned tracks DataFrame
        """
        if df.empty:
            logger.warning("Empty DataFrame received for tracks cleaning")
            return df
        
        logger.info(f" Cleaning {len(df)} track records...")
        
        try:
            # Use existing comprehensive transformation pipeline
            transformed_df, _ = self.transform(df)
            
            # Ensure required columns exist with proper fallbacks
            if 'artist_name' in transformed_df.columns and 'name' not in transformed_df.columns:
                transformed_df['name'] = transformed_df['artist_name']
            
            logger.info(f" Track data cleaning completed: {len(transformed_df)} records")
            return transformed_df
            
        except Exception as e:
            logger.error(f" Error in clean_tracks_data: {e}")
            # Fallback to basic cleaning
            return self.clean_text_fields(df)
    
    def clean_audio_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and validate audio features data (compatibility method for DAG integration)
        
        Args:
            df: Raw audio features DataFrame
        
        Returns:
            Cleaned audio features DataFrame
        """
        if df.empty:
            logger.warning("Empty DataFrame received for audio features cleaning")
            return df
        
        logger.info(f"🎵 Cleaning {len(df)} audio feature records...")
        
        try:
            # Create a copy and apply existing normalization
            cleaned_df = df.copy()
            
            # Rename 'id' to 'track_id' if needed
            if 'id' in cleaned_df.columns and 'track_id' not in cleaned_df.columns:
                cleaned_df = cleaned_df.rename(columns={'id': 'track_id'})
            
            # Apply existing audio feature normalization
            cleaned_df = self.normalize_audio_features(cleaned_df)
            
            # Remove duplicates
            if 'track_id' in cleaned_df.columns:
                cleaned_df = cleaned_df.drop_duplicates(subset=['track_id'])
            
            logger.info(f" Audio features cleaning completed: {len(cleaned_df)} records")
            return cleaned_df
            
        except Exception as e:
            logger.error(f" Error in clean_audio_features: {e}")
            return df
    
    def clean_artist_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and validate artist data (compatibility method for DAG integration)
        
        Args:
            df: Raw artist DataFrame
        
        Returns:
            Cleaned artist DataFrame
        """
        if df.empty:
            logger.warning("Empty DataFrame received for artist cleaning")
            return df
        
        logger.info(f"👤 Cleaning {len(df)} artist records...")
        
        try:
            cleaned_df = df.copy()
            
            # Remove duplicates
            if 'artist_id' in cleaned_df.columns:
                cleaned_df = cleaned_df.drop_duplicates(subset=['artist_id'])
            
            # Clean text fields using existing method
            cleaned_df = self.clean_text_fields(cleaned_df)
            
            # Handle numeric fields
            if 'popularity' in cleaned_df.columns:
                cleaned_df['popularity'] = pd.to_numeric(cleaned_df['popularity'], errors='coerce')
                cleaned_df['popularity'] = cleaned_df['popularity'].fillna(0).astype(int).clip(0, 100)
            
            if 'followers' in cleaned_df.columns:
                cleaned_df['followers'] = pd.to_numeric(cleaned_df['followers'], errors='coerce')
                cleaned_df['followers'] = cleaned_df['followers'].fillna(0).astype(int)
            
            # Ensure artist_name maps to name column for compatibility
            if 'artist_name' in cleaned_df.columns and 'name' not in cleaned_df.columns:
                cleaned_df['name'] = cleaned_df['artist_name']
            elif 'name' in cleaned_df.columns and 'artist_name' not in cleaned_df.columns:
                cleaned_df['artist_name'] = cleaned_df['name']
            
            # Handle genres
            if 'genres' in cleaned_df.columns:
                # Ensure genres is properly formatted
                cleaned_df['genres'] = cleaned_df['genres'].fillna('')
                
            logger.info(f" Artist data cleaning completed: {len(cleaned_df)} records")
            return cleaned_df
            
        except Exception as e:
            logger.error(f" Error in clean_artist_data: {e}")
            return df
    
    def validate_data(self, df: pd.DataFrame, required_columns: List[str]) -> bool:
        """
        Validate that DataFrame has required columns and valid data
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
        
        Returns:
            True if valid, False otherwise
        """
        if df.empty:
            logger.warning(" Empty DataFrame")
            return False
        
        # Check required columns
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            logger.error(f" Missing required columns: {missing_columns}")
            return False
        
        # Check for null values in required columns
        for col in required_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    logger.warning(f" Column '{col}' has {null_count} null values")
        
        logger.info(f" Data validation passed for {len(df)} records")
        return True
    
    def _load_genre_mappings(self) -> Dict[str, str]:
        """Load genre category mappings"""
        return {
            'pop': 'Pop',
            'rock': 'Rock', 
            'hip hop': 'Hip Hop',
            'electronic': 'Electronic',
            'indie': 'Indie',
            'jazz': 'Jazz',
            'classical': 'Classical',
            'country': 'Country',
            'r&b': 'R&B',
            'folk': 'Folk',
            'k-pop': 'K-Pop',
            'metal': 'Metal',
            'punk': 'Punk',
            'reggae': 'Reggae',
            'blues': 'Blues',
            'latin': 'Latin',
            'world': 'World',
            'soundtrack': 'Soundtrack'
        }
    
    def clean_text_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize text fields"""
        logger.info("Cleaning text fields...")
        
        text_columns = ['track_name', 'artist_name', 'album_name']
        df_cleaned = df.copy()
        
        for col in text_columns:
            if col in df_cleaned.columns:
                # Handle None values first
                df_cleaned[col] = df_cleaned[col].fillna('')
                
                # Convert to string type
                df_cleaned[col] = df_cleaned[col].astype(str)
                
                # Remove extra whitespace
                df_cleaned[col] = df_cleaned[col].str.strip()
                
                # Remove special characters that cause database issues
                df_cleaned[col] = df_cleaned[col].str.replace(r'[^\w\s\-\'\(\)\&]', '', regex=True)
                
                # Limit length to prevent database issues
                max_length = 200 if col != 'track_name' else 300
                df_cleaned[col] = df_cleaned[col].str[:max_length]
                
                # Handle empty strings
                df_cleaned[col] = df_cleaned[col].replace('', np.nan)
        
        return df_cleaned
    
    def normalize_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize timestamp formats"""
        logger.info("Normalizing timestamps...")
        
        if 'played_at' in df.columns:
            # Convert to datetime
            df['played_at'] = pd.to_datetime(df['played_at'], utc=True)
            
            # Create additional time-based features
            df['played_date'] = df['played_at'].dt.date
            df['played_hour'] = df['played_at'].dt.hour
            df['played_day_of_week'] = df['played_at'].dt.day_name()
            df['played_month'] = df['played_at'].dt.month
        
        # Handle release dates
        if 'release_date' in df.columns:
            # Spotify release dates come in different formats
            df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
            
            # Extract year for analysis
            df['release_year'] = df['release_date'].dt.year
        
        return df
    
    def normalize_audio_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize audio feature values"""
        logger.info("Normalizing audio features...")
        
        # Audio features that should be between 0 and 1
        feature_columns = [
            'danceability', 'energy', 'speechiness', 'acousticness',
            'instrumentalness', 'liveness', 'valence'
        ]
        
        for col in feature_columns:
            if col in df.columns:
                # Ensure values are between 0 and 1
                df[col] = df[col].clip(0, 1)
                
                # Round to 3 decimal places
                df[col] = df[col].round(3)
        
        # Handle tempo (can be any positive value)
        if 'tempo' in df.columns:
            df['tempo'] = df['tempo'].clip(0, 300)  # Reasonable tempo range
            df['tempo'] = df['tempo'].round(2)
        
        # Handle loudness (typically negative values)
        if 'loudness' in df.columns:
            df['loudness'] = df['loudness'].clip(-60, 0)  # Typical range
            df['loudness'] = df['loudness'].round(2)
        
        return df

    # Create derived features for analysis
    def create_derived_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create derived features for analysis"""
        logger.info("Creating derived features...")
        
        # Mood categories based on valence and energy
        if 'valence' in df.columns and 'energy' in df.columns:
            def get_mood(row):
                valence, energy = row['valence'], row['energy']
                if pd.isna(valence) or pd.isna(energy):
                    return 'Unknown'
                
                if valence > 0.6 and energy > 0.6:
                    return 'Happy/Energetic'
                elif valence > 0.6 and energy <= 0.6:
                    return 'Happy/Calm'
                elif valence <= 0.4 and energy > 0.6:
                    return 'Angry/Intense'
                elif valence <= 0.4 and energy <= 0.4:
                    return 'Sad/Melancholic'
                else:
                    return 'Neutral'
            
            df['mood_category'] = df.apply(get_mood, axis=1)
        
        # Duration categories
        if 'duration_ms' in df.columns:
            def get_duration_category(ms):
                if pd.isna(ms):
                    return 'Unknown'
                
                minutes = ms / 60000
                if minutes < 2:
                    return 'Very Short'
                elif minutes < 3:
                    return 'Short'
                elif minutes < 4:
                    return 'Medium'
                elif minutes < 6:
                    return 'Long'
                else:
                    return 'Very Long'
            
            df['duration_category'] = df['duration_ms'].apply(get_duration_category)
            df['duration_minutes'] = (df['duration_ms'] / 60000).round(2)
        
        # Popularity categories
        if 'popularity' in df.columns:
            def get_popularity_category(pop):
                if pd.isna(pop):
                    return 'Unknown'
                
                if pop >= 80:
                    return 'Viral'
                elif pop >= 60:
                    return 'Popular'
                elif pop >= 40:
                    return 'Moderate'
                elif pop >= 20:
                    return 'Niche'
                else:
                    return 'Obscure'
            
            df['popularity_category'] = df['popularity'].apply(get_popularity_category)
        
        return df
    
    # For handling missing values 
    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle missing values appropriately"""
        logger.info("Handling missing values...")
        
        # Fill numeric features with median values
        numeric_columns = ['popularity', 'duration_ms', 'tempo', 'loudness']
        for col in numeric_columns:
            if col in df.columns:
                median_value = df[col].median()
                df[col] = df[col].fillna(median_value)
        
        # Fill audio features with neutral values
        audio_features = ['danceability', 'energy', 'valence', 'acousticness', 
                         'instrumentalness', 'liveness', 'speechiness']
        for col in audio_features:
            if col in df.columns:
                df[col] = df[col].fillna(0.5)  # Neutral value
        
        # Fill categorical with 'Unknown'
        categorical_columns = ['album_type', 'mood_category', 'duration_category']
        for col in categorical_columns:
            if col in df.columns:
                df[col] = df[col].fillna('Unknown')
        
        return df
    
    # Remove duplicates intelligently
    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate records intelligently"""
        logger.info("Removing duplicates...")
        
        initial_count = len(df) 
        
        # For listening history, duplicates are based on track_id + played_at
        if 'played_at' in df.columns:
            # Keep the first occurrence of each track at each timestamp
            df = df.drop_duplicates(subset=['track_id', 'played_at'], keep='first')
        else:
            # For other data, just track_id
            df = df.drop_duplicates(subset=['track_id'], keep='first')
        
        final_count = len(df)
        
        if final_count != initial_count:
            logger.info(f"Removed {initial_count - final_count} duplicate records")
        
        return df
    
    def validate_data_quality(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Validate data quality and return quality metrics"""
        logger.info("Validating data quality...")
        
        quality_report = {
            'total_rows': len(df),
            'missing_values': {},
            'data_types': {},
            'value_ranges': {}
        }
        
        if df.empty:
            return df, quality_report
        
        # Check missing values
        for col in df.columns:
            missing_count = df[col].isnull().sum()
            if missing_count > 0:
                quality_report['missing_values'][col] = {
                    'count': missing_count,
                    'percentage': (missing_count / len(df)) * 100
                }
        
        # Check data types
        quality_report['data_types'] = df.dtypes.to_dict()
        
        # Check value ranges for numeric columns
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            quality_report['value_ranges'][col] = {
                'min': df[col].min(),
                'max': df[col].max(),
                'mean': df[col].mean()
            }
        
        return df, quality_report
    
    def transform(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Complete transformation pipeline"""
        logger.info(f"Starting transformation of {len(df)} rows...")
        
        if df.empty:
            return df, {}
        
        try:
            # Ensure required columns exist
            required_columns = ['track_id', 'track_name']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Apply all transformations
            df = self.clean_text_fields(df)
            df = self.normalize_timestamps(df)
            df = self.normalize_audio_features(df)
            df = self.create_derived_features(df)
            df = self.handle_missing_values(df)
            df = self.remove_duplicates(df)
            
            # Final validation
            df, quality_report = self.validate_data_quality(df)
            
            logger.info(f" Transformation complete: {len(df)} rows")
            
            return df, quality_report
            
        except Exception as e:
            logger.error(f"Error during transformation: {str(e)}")
            raise

# Method for Testing for all method functions "Clean Text Fields", 
# "Normalize Timestamps", "Normalize Audio Features", "Create Derived Features", 
# "Handle Missing Values", "Remove Duplicates", "Validate Data Quality", 
# and the full "Transform" pipeline.
def test_transformer():
    """Test the enhanced data transformer with validation capabilities"""
    print("🧪 Testing Enhanced Data Transformer")
    print("=" * 40)
    
    # Create sample data for testing various edge cases
    sample_data = {
        'track_id': ['1', '2', '3', '1'],  # Include duplicate
        'track_name': ['  Song One  ', 'Song Two!@#', '', 'Song One'],
        'artist_id': ['A1', 'A2', 'A3', 'A1'],
        'artist_name': ['Artist A', 'Artist B & C', 'Artist C', 'Artist A'],
        'album_id': ['AL1', 'AL2', 'AL3', 'AL1'],
        'album_name': ['Album 1', 'Album 2', 'Album 3', 'Album 1'],
        'played_at': ['2024-01-15T10:30:00Z', '2024-01-15T11:00:00Z', '2024-01-15T12:00:00Z', '2024-01-15T10:30:00Z'],
        'duration_ms': [210000, 180000, 240000, 210000],
        'popularity': [75, 60, np.nan, 75],
        'energy': [0.8, 0.4, 0.9, 0.8],
        'valence': [0.7, 0.3, 0.8, 0.7],
        'tempo': [120.5, 95.2, 140.0, 120.5],
        'danceability': [0.6, 0.3, 0.9, 0.6],
        'explicit': [True, False, True, True]
    }
    
    df = pd.DataFrame(sample_data)
    transformer = SpotifyDataTransformer()
    
    print(f" Input data: {len(df)} rows")
    
    # Test 1: Individual cleaning methods (new)
    print("\n Testing individual cleaning methods...")
    
    # Test track cleaning
    cleaned_tracks = transformer.clean_tracks_data(df)
    print(f"    Track cleaning: {len(df)} → {len(cleaned_tracks)} records")
    
    # Test audio features cleaning  
    audio_features_df = df[['track_id', 'energy', 'valence', 'tempo', 'danceability']].copy()
    cleaned_audio = transformer.clean_audio_features(audio_features_df)
    print(f"    Audio features cleaning: {len(audio_features_df)} → {len(cleaned_audio)} records")
    
    # Test artist cleaning
    artist_df = df[['artist_id', 'artist_name', 'popularity']].drop_duplicates().copy()
    artist_df['name'] = artist_df['artist_name']  # Add name column
    cleaned_artists = transformer.clean_artist_data(artist_df)
    print(f"    Artist cleaning: {len(artist_df)} → {len(cleaned_artists)} records")
    
    # Test 2: Data validation (new)
    print("\n Testing data validation...")
    tracks_valid = transformer.validate_data(cleaned_tracks, transformer.required_track_columns)
    print(f"    Track validation: {'PASSED' if tracks_valid else 'FAILED'}")
    
    artists_valid = transformer.validate_data(cleaned_artists, transformer.required_artist_columns)
    print(f"    Artist validation: {'PASSED' if artists_valid else 'FAILED'}")
    
    # Test 3: Full transformation pipeline (existing)
    print("\n Testing full transformation pipeline...")
    transformed_df, quality_report = transformer.transform(df)
    
    print(f"    Output data: {len(transformed_df)} rows")
    print(f"    New columns: {len(transformed_df.columns)}")
    
    # Show enhanced results
    print("\n Sample enhanced data:")
    display_cols = ['track_name', 'mood_category', 'duration_category', 'popularity_category']
    available_cols = [col for col in display_cols if col in transformed_df.columns]
    if available_cols:
        print(transformed_df[available_cols].head(3).to_string(index=False))
    
    # Enhanced quality report
    print("\n Enhanced Quality Report:")
    print(f"   Total rows: {quality_report['total_rows']}")
    if quality_report['missing_values']:
        print("   Missing values:")
        for col, info in quality_report['missing_values'].items():
            print(f"     {col}: {info['count']} ({info['percentage']:.1f}%)")
    else:
        print("    No missing values found")
    
    # Save enhanced test results
    output_file = 'enhanced_day3_transformation_test.csv'
    transformed_df.to_csv(output_file, index=False)
    print(f"\n💾 Saved enhanced test results to: {output_file}")
    
    print("\n All enhanced transformer tests completed successfully!")
    return True

if __name__ == "__main__":
    test_transformer()