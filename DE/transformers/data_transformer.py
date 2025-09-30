"""
Data Transformation Pipeline
Features: Data cleaning, normalization, feature engineering
"""
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import logging
from typing import Dict, List, Optional, Tuple
import re

logger = logging.getLogger(__name__)

class SpotifyDataTransformer:
    """Transform and clean Spotify data for database loading"""
    
    def __init__(self):
        self.genre_mappings = self._load_genre_mappings()
    
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
            
            logger.info(f"✅ Transformation complete: {len(df)} rows")
            
            return df, quality_report
            
        except Exception as e:
            logger.error(f"Error during transformation: {str(e)}")
            raise

# Method for Testing for all method functions "Clean Text Fields", 
# "Normalize Timestamps", "Normalize Audio Features", "Create Derived Features", 
# "Handle Missing Values", "Remove Duplicates", "Validate Data Quality", 
# and the full "Transform" pipeline.
def test_transformer():
    """Test the data transformer"""
    print("🧪 Testing Data Transformer")
    print("=" * 30)
    
    # Create sample data for testing various edge cases
    sample_data = {
        'track_id': ['1', '2', '3', '1'],  # Include duplicate
        'track_name': ['  Song One  ', 'Song Two!@#', '', 'Song One'],
        'artist_name': ['Artist A', 'Artist B & C', 'Artist C', 'Artist A'],
        'album_name': ['Album 1', 'Album 2', 'Album 3', 'Album 1'],
        'played_at': ['2024-01-15T10:30:00Z', '2024-01-15T11:00:00Z', '2024-01-15T12:00:00Z', '2024-01-15T10:30:00Z'],
        'duration_ms': [210000, 180000, 240000, 210000],
        'popularity': [75, 60, np.nan, 75],
        'energy': [0.8, 0.4, 0.9, 0.8],
        'valence': [0.7, 0.3, 0.8, 0.7],
        'tempo': [120.5, 95.2, 140.0, 120.5],
        'danceability': [0.6, 0.3, 0.9, 0.6]
    }
    
    df = pd.DataFrame(sample_data)
    print(f"📊 Input data: {len(df)} rows")
    
    # Transform the data
    transformer = SpotifyDataTransformer()
    transformed_df, quality_report = transformer.transform(df)
    
    print(f"📊 Output data: {len(transformed_df)} rows")
    print(f"📋 New columns: {len(transformed_df.columns)}")
    
    # Show sample results
    print("\n📈 Sample transformed data:")
    display_cols = ['track_name', 'mood_category', 'duration_category', 'popularity_category']
    available_cols = [col for col in display_cols if col in transformed_df.columns]
    if available_cols:
        print(transformed_df[available_cols].head().to_string(index=False))
    
    # Show quality report
    print("\n📊 Quality Report:")
    print(f"Total rows: {quality_report['total_rows']}")
    if quality_report['missing_values']:
        print("Missing values:")
        for col, info in quality_report['missing_values'].items():
            print(f"  {col}: {info['count']} ({info['percentage']:.1f}%)")
    else:
        print("No missing values found")
    
    # Save test results
    transformed_df.to_csv('day3_transformation_test.csv', index=False)
    print(f"\n💾 Saved transformed data to: day3_transformation_test.csv")
    
    return True

if __name__ == "__main__":
    test_transformer()