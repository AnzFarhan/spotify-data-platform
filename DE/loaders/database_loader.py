"""
Day 3: Database Loader
Features: Batch loading, upsert logic, error handling, transaction management
Production-ready with comprehensive data validation and dependency management
"""
import pandas as pd
import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text
import logging
from typing import Dict, List, Optional, Tuple
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SpotifyDatabaseLoader:
    """Load transformed Spotify data into PostgreSQL database with enhanced batch processing"""
    
    def __init__(self):
        self.connection_string = self._build_connection_string()
        self.engine = None
        self.connection = None
        # Add batch size configuration from new code
        self.batch_size = int(os.getenv('DB_BATCH_SIZE', '1000'))
        self.connection_params = self._build_connection_params()  # For compatibility
        logger.info(f"🚀 SpotifyDatabaseLoader initialized with batch size: {self.batch_size}")
        self._setup_database()
    
    def _build_connection_params(self) -> Dict[str, str]:
        """Build connection parameters dictionary for compatibility"""
        return {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': os.getenv('POSTGRES_PORT', '5432'),
            'database': os.getenv('POSTGRES_DB', 'spotify_data'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', '')
        }
    
    def _build_connection_string(self) -> str:
        """Build database connection string"""
        host = os.getenv('POSTGRES_HOST', 'localhost')
        port = os.getenv('POSTGRES_PORT', '5432')
        database = os.getenv('POSTGRES_DB', 'spotify_data')
        user = os.getenv('POSTGRES_USER', 'postgres')
        password = os.getenv('POSTGRES_PASSWORD', '')
        
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    def _setup_database(self):
        """Set up database connections"""
        try:
            # Create SQLAlchemy engine with proper configuration
            self.engine = create_engine(
                self.connection_string, 
                pool_size=10, 
                max_overflow=20,
                pool_pre_ping=True,  # Enable connection health checks
                echo=False  # Set to True for SQL debugging
            )
            
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logger.info(" Database connection established")
            
        except Exception as e:
            logger.error(f" Database setup failed: {e}")
            raise
    
    def get_connection(self):
        """Get raw psycopg2 connection for advanced operations"""
        try:
            conn = psycopg2.connect(self.connection_string)
            return conn
        except Exception as e:
            logger.error(f" Failed to get database connection: {e}")
            raise
    
    def load_artists(self, df: pd.DataFrame) -> int:
        """
        Load artist data with enhanced upsert logic and batch processing
        
        Args:
            df: DataFrame with artist data
        
        Returns:
            Number of records loaded
        """
        if df.empty or 'artist_id' not in df.columns:
            logger.warning(" No artist data to load")
            return 0
        
        logger.info(f" Loading {len(df)} artists with enhanced processing...")
        
        try:
            # Check if we have detailed artist information (from new extractor)
            has_detailed_info = all(col in df.columns for col in ['artist_genres', 'artist_popularity', 'artist_followers'])
            
            # Prepare artist data with available columns
            if has_detailed_info:
                # Use detailed artist information
                artist_columns = ['artist_id', 'artist_name', 'artist_genres', 'artist_popularity', 'artist_followers']
                available_columns = [col for col in artist_columns if col in df.columns]
                
                # Get unique artists with detailed info
                artists_df = df[available_columns].drop_duplicates(subset=['artist_id'])
                artists_df = artists_df.rename(columns={
                    'artist_name': 'name',
                    'artist_genres': 'genres',
                    'artist_popularity': 'popularity',
                    'artist_followers': 'followers'
                })
                
                # Enhanced genre processing - handle both list and string formats
                if 'genres' in artists_df.columns:
                    def process_genres(x):
                        if pd.isna(x) or x == '' or x == '[]':
                            return None
                        if isinstance(x, list):
                            return f"{{{','.join(x)}}}"
                        elif isinstance(x, str):
                            # Handle comma-separated string
                            if x.startswith('[') and x.endswith(']'):
                                # Remove brackets and process
                                x = x[1:-1].replace("'", "").replace('"', '')
                            return f"{{{x.replace(', ', ',')}}}"
                        return None
                    
                    artists_df['genres'] = artists_df['genres'].apply(process_genres)
                
                logger.info(f" Using detailed artist information (genres, popularity, followers)")
                
            else:
                # Use basic artist information
                artist_columns = ['artist_id', 'artist_name']
                available_columns = [col for col in artist_columns if col in df.columns]
                
                artists_df = df[available_columns].drop_duplicates(subset=['artist_id'])
                artists_df = artists_df.rename(columns={'artist_name': 'name'})
                
                # Add missing columns with defaults
                artists_df['genres'] = None
                artists_df['popularity'] = None
                artists_df['followers'] = None

                logger.info(" Using basic artist information (no detailed data available)")

            # Ensure required columns exist
            if 'artist_id' not in artists_df.columns:
                logger.error("Missing required artist_id column")
                return 0
            
            if 'name' not in artists_df.columns:
                artists_df['name'] = 'Unknown Artist'
            
            # Add timestamps
            artists_df['created_at'] = datetime.now(timezone.utc)
                
            # Enhanced batch processing with execute_values for better performance
            if len(artists_df) > self.batch_size:
                logger.info(f"🔄 Processing {len(artists_df)} artists in batches of {self.batch_size}")
                return self._load_artists_batch(artists_df, has_detailed_info)
            else:
                # Use existing upsert method for smaller datasets
                update_columns = ['name', 'created_at']
                if has_detailed_info:
                    update_columns.extend(['genres', 'popularity', 'followers'])
                
                rows_affected = self._upsert_data(
                    artists_df, 
                    'artists', 
                    conflict_columns=['artist_id'],
                    update_columns=update_columns
                )
                
                logger.info(f" Loaded {rows_affected} artists")
                return rows_affected
                
        except Exception as e:
            logger.error(f" Failed to load artists: {e}")
            return 0
            
    def _load_artists_batch(self, artists_df: pd.DataFrame, has_detailed_info: bool) -> int:
        """Load artists using batch processing with execute_values for better performance"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            total_loaded = 0
            
            # Process in batches
            for i in range(0, len(artists_df), self.batch_size):
                batch_df = artists_df.iloc[i:i + self.batch_size]
                logger.info(f"📋 Processing batch {i//self.batch_size + 1}: {len(batch_df)} artists")
                
                # Prepare data for execute_values
                data = []
                for _, row in batch_df.iterrows():
                    if has_detailed_info:
                        values = (
                            row.get('artist_id'),
                            row.get('name', 'Unknown Artist'),
                            row.get('genres'),
                            row.get('popularity'),
                            row.get('followers'),
                            datetime.now(timezone.utc)
                        )
                    else:
                        values = (
                            row.get('artist_id'),
                            row.get('name', 'Unknown Artist'),
                            None,  # genres
                            None,  # popularity
                            None,  # followers
                            datetime.now(timezone.utc)
                        )
                    data.append(values)
                
                # Enhanced upsert query with execute_values
                if has_detailed_info:
                    query = """
                        INSERT INTO artists (artist_id, name, genres, popularity, followers, created_at)
                        VALUES %s
                        ON CONFLICT (artist_id) 
                        DO UPDATE SET
                            name = EXCLUDED.name,
                            genres = EXCLUDED.genres,
                            popularity = EXCLUDED.popularity,
                            followers = EXCLUDED.followers,
                            created_at = EXCLUDED.created_at
                    """
                else:
                    query = """
                        INSERT INTO artists (artist_id, name, genres, popularity, followers, created_at)
                        VALUES %s
                        ON CONFLICT (artist_id) 
                        DO UPDATE SET
                            name = EXCLUDED.name,
                            created_at = EXCLUDED.created_at
                    """
                
                execute_values(cursor, query, data)
                batch_loaded = cursor.rowcount
                total_loaded += batch_loaded
                
                logger.info(f"    Batch {i//self.batch_size + 1}: {batch_loaded} artists loaded")
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f" Total artists loaded with batch processing: {total_loaded}")
            return total_loaded
            
        except Exception as e:
            logger.error(f" Batch artist loading failed: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return 0
    
    def load_albums(self, df: pd.DataFrame) -> int:
        """Load album data with upsert logic"""
        if df.empty or 'album_id' not in df.columns:
            logger.warning("No album data to load")
            return 0
        
        logger.info(f"Loading albums...")
        
        try:
            # Prepare album data
            album_columns = ['album_id', 'album_name', 'artist_id', 'release_date', 'total_tracks', 'album_type']
            available_columns = [col for col in album_columns if col in df.columns]
            
            # Get unique albums
            albums_df = df[available_columns].drop_duplicates(subset=['album_id'])
            albums_df = albums_df.rename(columns={'album_name': 'name'})
            
            # Add missing columns with defaults
            if 'name' not in albums_df.columns:
                albums_df['name'] = 'Unknown Album'
            
            albums_df['created_at'] = datetime.now(timezone.utc)
            
            # Use upsert logic
            rows_affected = self._upsert_data(
                albums_df,
                'albums',
                conflict_columns=['album_id'],
                update_columns=['name', 'release_date', 'total_tracks', 'album_type', 'created_at']
            )
            
            logger.info(f" Loaded {rows_affected} albums")
            return rows_affected
            
        except Exception as e:
            logger.error(f" Failed to load albums: {e}")
            return 0
    
    def load_tracks(self, df: pd.DataFrame) -> int:
        """Load track data with upsert logic"""
        if df.empty or 'track_id' not in df.columns:
            logger.warning("No track data to load")
            return 0
        
        logger.info(f"Loading tracks...")
        
        try:
            # Prepare track data
            track_columns = [
                'track_id', 'track_name', 'album_id', 'artist_id', 
                'duration_ms', 'explicit', 'popularity', 'preview_url'
            ]
            available_columns = [col for col in track_columns if col in df.columns]
            
            # Get unique tracks
            tracks_df = df[available_columns].drop_duplicates(subset=['track_id'])
            tracks_df = tracks_df.rename(columns={'track_name': 'name'})
            
            # Add missing columns with defaults
            if 'name' not in tracks_df.columns:
                tracks_df['name'] = 'Unknown Track'
            
            tracks_df['created_at'] = datetime.now(timezone.utc)
            
            # Use upsert logic
            rows_affected = self._upsert_data(
                tracks_df,
                'tracks',
                conflict_columns=['track_id'],
                update_columns=['name', 'duration_ms', 'explicit', 'popularity', 'preview_url', 'created_at']
            )
            
            logger.info(f" Loaded {rows_affected} tracks")
            return rows_affected
            
        except Exception as e:
            logger.error(f" Failed to load tracks: {e}")
            return 0
    
    def load_audio_features(self, df: pd.DataFrame) -> int:
        """Load audio features data"""
        if df.empty or 'track_id' not in df.columns:
            logger.warning("No audio features data to load")
            return 0
        
        logger.info(f"Loading audio features...")
        
        try:
            # Audio feature columns
            feature_columns = [
                'track_id', 'danceability', 'energy', 'key', 'loudness', 'mode',
                'speechiness', 'acousticness', 'instrumentalness', 'liveness',
                'valence', 'tempo', 'time_signature'
            ]
            
            available_columns = [col for col in feature_columns if col in df.columns]
            
            if len(available_columns) < 2:  # At least track_id + 1 feature
                logger.warning("Insufficient audio features data")
                return 0
            
            # Get unique audio features
            features_df = df[available_columns].drop_duplicates(subset=['track_id'])
            features_df['created_at'] = datetime.now(timezone.utc)
            
            # Use upsert logic
            rows_affected = self._upsert_data(
                features_df,
                'audio_features',
                conflict_columns=['track_id'],
                update_columns=[col for col in available_columns if col != 'track_id'] + ['created_at']
            )
            
            logger.info(f" Loaded {rows_affected} audio features")
            return rows_affected
            
        except Exception as e:
            logger.error(f" Failed to load audio features: {e}")
            return 0
    
    def load_listening_history(self, df: pd.DataFrame) -> int:
        """Load listening history data"""
        if df.empty or 'track_id' not in df.columns or 'played_at' not in df.columns:
            logger.warning("No listening history data to load")
            return 0
        
        logger.info(f"Loading {len(df)} listening history records...")
        
        try:
            # Prepare listening history data
            history_columns = ['track_id', 'played_at']
            available_columns = [col for col in history_columns if col in df.columns]
            
            history_df = df[available_columns].copy()
            history_df['created_at'] = datetime.now(timezone.utc)
            
            # For listening history, we typically want to append new records
            # Check for existing records to avoid duplicates
            existing_check_query = """
            SELECT COUNT(*) as count FROM listening_history 
            WHERE track_id = %s AND played_at = %s
            """
            
            conn = self.get_connection()
            cursor = conn.cursor()
            
            new_records = []
            for _, row in history_df.iterrows():
                cursor.execute(existing_check_query, (row['track_id'], row['played_at']))
                count = cursor.fetchone()[0]
                
                if count == 0:  # Record doesn't exist
                    new_records.append(row)
            
            cursor.close()
            conn.close()
            
            if new_records:
                new_df = pd.DataFrame(new_records)
                
                # Insert new records using SQLAlchemy engine properly
                rows_affected = len(new_df)
                new_df.to_sql('listening_history', self.engine, if_exists='append', index=False, method='multi')
                
                logger.info(f" Loaded {rows_affected} new listening history records")
            else:
                logger.info("No new listening history records to load")
                rows_affected = 0
            
            return rows_affected
            
        except Exception as e:
            logger.error(f" Failed to load listening history: {e}")
            return 0
    
    def _upsert_data(self, df: pd.DataFrame, table_name: str, 
                    conflict_columns: List[str], update_columns: List[str]) -> int:
        """Perform upsert (INSERT ... ON CONFLICT) operation"""
        if df.empty:
            return 0
        
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            
            # Prepare column names and placeholders
            columns = list(df.columns)
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join(columns)
            
            # Build ON CONFLICT clause
            conflict_str = ', '.join(conflict_columns)
            update_clauses = [f"{col} = EXCLUDED.{col}" for col in update_columns]
            update_str = ', '.join(update_clauses)
            
            # Build the upsert query
            query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_str})
            DO UPDATE SET {update_str}
            """
            
            # Execute batch insert
            data_tuples = [tuple(row) for row in df.values]
            cursor.executemany(query, data_tuples)
            
            rows_affected = cursor.rowcount
            conn.commit()
            
            return rows_affected
            
        except Exception as e:
            logger.error(f" Upsert failed for {table_name}: {e}")
            if conn:
                conn.rollback()
            return 0
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def load_complete_dataset(self, df: pd.DataFrame) -> Dict[str, int]:
        """Load complete dataset with proper order and dependencies"""
        logger.info("Starting complete dataset load...")
        
        results = {
            'artists': 0,
            'albums': 0,
            'tracks': 0,
            'audio_features': 0,
            'listening_history': 0
        }
        
        if df.empty:
            logger.warning("No data to load")
            return results
        
        try:
            # Load in proper order to respect foreign key constraints
            
            # 1. Load artists first (no dependencies)
            results['artists'] = self.load_artists(df)
            
            # 2. Load albums (depends on artists)
            results['albums'] = self.load_albums(df)
            
            # 3. Load tracks (depends on albums and artists)
            results['tracks'] = self.load_tracks(df)
            
            # 4. Load audio features (depends on tracks)
            results['audio_features'] = self.load_audio_features(df)
            
            # 5. Load listening history (depends on tracks)
            results['listening_history'] = self.load_listening_history(df)
            
            total_rows = sum(results.values())
            logger.info(f" Complete dataset load finished: {total_rows} total rows loaded")
            
            return results
            
        except Exception as e:
            logger.error(f" Complete dataset load failed: {e}")
            return results
    
    def get_load_statistics(self) -> Dict[str, int]:
        """Get current table row counts"""
        try:
            tables = ['artists', 'albums', 'tracks', 'audio_features', 'listening_history']
            stats = {}
            
            with self.engine.connect() as conn:
                for table in tables:
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                    stats[table] = result.fetchone()[0]
            
            return stats
            
        except Exception as e:
            logger.error(f" Failed to get load statistics: {e}")
            return {}

# Alias for compatibility with new code
DatabaseLoader = SpotifyDatabaseLoader

def test_database_loader():
    """Test the database loader"""
    print(" Testing Database Loader")
    print("=" * 30)
    
    # Create sample data
    sample_data = {
        'track_id': ['track1', 'track2', 'track3'],
        'track_name': ['Test Song 1', 'Test Song 2', 'Test Song 3'],
        'artist_id': ['artist1', 'artist2', 'artist1'],
        'artist_name': ['Test Artist 1', 'Test Artist 2', 'Test Artist 1'],
        'album_id': ['album1', 'album2', 'album1'],
        'album_name': ['Test Album 1', 'Test Album 2', 'Test Album 1'],
        'played_at': ['2024-01-15T10:00:00Z', '2024-01-15T11:00:00Z', '2024-01-15T12:00:00Z'],
        'duration_ms': [210000, 180000, 240000],
        'popularity': [75, 60, 80],
        'explicit': [False, False, True],
        'danceability': [0.8, 0.4, 0.9],
        'energy': [0.7, 0.3, 0.8],
        'valence': [0.6, 0.2, 0.9],
        'tempo': [120.0, 95.0, 140.0]
    }
    
    df = pd.DataFrame(sample_data)
    print(f" Sample data created: {len(df)} rows")
    
    try:
        # Initialize loader
        loader = SpotifyDatabaseLoader()
        
        # Get initial statistics
        print("\n Initial database statistics:")
        initial_stats = loader.get_load_statistics()
        for table, count in initial_stats.items():
            print(f"  {table}: {count} rows")
        
        # Load the complete dataset
        print(f"\n Loading test data...")
        results = loader.load_complete_dataset(df)
        
        print(f"\n Load results:")
        for table, count in results.items():
            print(f"  {table}: {count} rows loaded")
        
        # Get final statistics
        print(f"\n Final database statistics:")
        final_stats = loader.get_load_statistics()
        for table, count in final_stats.items():
            change = count - initial_stats.get(table, 0)
            change_str = f" (+{change})" if change > 0 else ""
            print(f"  {table}: {count} rows{change_str}")
        
        print("\n Database loader test completed successfully!")
        return True
        
    except Exception as e:
        print(f" Database loader test failed: {e}")
        return False

if __name__ == "__main__":
    test_database_loader()
    