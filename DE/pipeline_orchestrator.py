"""
Day 3: ETL Pipeline Orchestrator
Combines extraction, transformation, and loading with error handling and monitoring
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
import pandas as pd
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from DE.extractors.spotify_extractor_v2 import SpotifyExtractorV2
from DE.transformers.data_transformer import SpotifyDataTransformer
from DE.loaders.database_loader import SpotifyDatabaseLoader

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SpotifyETLPipeline:
    """Complete ETL Pipeline for Spotify data"""
    
    def __init__(self):
        self.extractor = None
        self.transformer = None
        self.loader = None
        self.pipeline_stats = {
            'start_time': None,
            'end_time': None,
            'duration_seconds': 0,
            'extraction_stats': {},
            'transformation_stats': {},
            'loading_stats': {},
            'total_records_processed': 0,
            'success': False,
            'errors': []
        }
        
        self._initialize_components()
    
    def _initialize_components(self):
        """Initialize ETL components"""
        try:
            logger.info("🚀 Initializing ETL pipeline components...")
            
            # Create logs directory if it doesn't exist
            logs_dir = Path('logs')
            logs_dir.mkdir(exist_ok=True)
            
            self.extractor = SpotifyExtractorV2()
            self.transformer = SpotifyDataTransformer()
            self.loader = SpotifyDatabaseLoader()
            
            logger.info("✅ All ETL components initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize ETL components: {e}")
            raise
    
    def extract_data(self, limit: int = 50, after_timestamp: Optional[int] = None) -> pd.DataFrame:
        """Extract data with error handling and monitoring"""
        logger.info(f"📥 Starting data extraction (limit={limit})...")
        
        try:
            self.pipeline_stats['extraction_stats']['start_time'] = datetime.utcnow()
            
            # Extract user info
            user_info = self.extractor.extract_user_info()
            if user_info:
                logger.info(f"👤 User: {user_info.get('display_name', 'Unknown')}")
            
            # Extract recent tracks with audio features
            df = self.extractor.extract_recent_tracks(limit=limit, after=after_timestamp)
            
            if df.empty:
                logger.warning("⚠️ No data extracted")
                return pd.DataFrame()
            
            # Validate extracted data
            df = self.extractor.validate_data(df)
            
            self.pipeline_stats['extraction_stats'].update({
                'end_time': datetime.utcnow(),
                'records_extracted': len(df),
                'columns_extracted': len(df.columns),
                'success': True
            })
            
            logger.info(f"✅ Extraction complete: {len(df)} records, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            error_msg = f"Data extraction failed: {e}"
            logger.error(f"❌ {error_msg}")
            self.pipeline_stats['errors'].append(error_msg)
            self.pipeline_stats['extraction_stats']['success'] = False
            return pd.DataFrame()
    
    def transform_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """Transform data with error handling and quality reporting"""
        logger.info(f"🔄 Starting data transformation...")
        
        try:
            self.pipeline_stats['transformation_stats']['start_time'] = datetime.utcnow()
            
            if df.empty:
                logger.warning("⚠️ No data to transform")
                return df, {}
            
            # Apply transformations
            transformed_df, quality_report = self.transformer.transform(df)
            
            self.pipeline_stats['transformation_stats'].update({
                'end_time': datetime.utcnow(),
                'input_records': len(df),
                'output_records': len(transformed_df),
                'quality_report': quality_report,
                'success': True
            })
            
            logger.info(f"✅ Transformation complete: {len(df)} → {len(transformed_df)} records")
            
            # Log quality issues if any
            if quality_report.get('missing_values'):
                logger.info("📊 Data quality summary:")
                for col, info in quality_report['missing_values'].items():
                    logger.info(f"  {col}: {info['percentage']:.1f}% missing")
            
            return transformed_df, quality_report
            
        except Exception as e:
            error_msg = f"Data transformation failed: {e}"
            logger.error(f"❌ {error_msg}")
            self.pipeline_stats['errors'].append(error_msg)
            self.pipeline_stats['transformation_stats']['success'] = False
            return pd.DataFrame(), {}
    
    def load_data(self, df: pd.DataFrame) -> Dict[str, int]:
        """Load data with error handling and monitoring"""
        logger.info(f"📤 Starting data loading...")
        
        try:
            self.pipeline_stats['loading_stats']['start_time'] = datetime.utcnow()
            
            if df.empty:
                logger.warning("⚠️ No data to load")
                return {}
            
            # Load complete dataset
            loading_results = self.loader.load_complete_dataset(df)
            
            total_loaded = sum(loading_results.values())
            
            self.pipeline_stats['loading_stats'].update({
                'end_time': datetime.utcnow(),
                'records_loaded': total_loaded,
                'loading_breakdown': loading_results,
                'success': True
            })
            
            logger.info(f"✅ Loading complete: {total_loaded} total records loaded")
            logger.info("📊 Loading breakdown:")
            for table, count in loading_results.items():
                if count > 0:
                    logger.info(f"  {table}: {count} records")
            
            return loading_results
            
        except Exception as e:
            error_msg = f"Data loading failed: {e}"
            logger.error(f"❌ {error_msg}")
            self.pipeline_stats['errors'].append(error_msg)
            self.pipeline_stats['loading_stats']['success'] = False
            return {}
    
    def run_pipeline(self, limit: int = 50, after_timestamp: Optional[int] = None) -> Dict:
        """Run complete ETL pipeline"""
        logger.info("🚀 Starting complete ETL pipeline...")
        
        self.pipeline_stats['start_time'] = datetime.utcnow()
        
        try:
            # Step 1: Extract
            df = self.extract_data(limit=limit, after_timestamp=after_timestamp)
            
            if df.empty:
                logger.warning("⚠️ Pipeline stopped: No data extracted")
                return self._finalize_stats()
            
            # Step 2: Transform
            transformed_df, quality_report = self.transform_data(df)
            
            if transformed_df.empty:
                logger.warning("⚠️ Pipeline stopped: No data after transformation")
                return self._finalize_stats()
            
            # Step 3: Load
            loading_results = self.load_data(transformed_df)
            
            # Calculate total records processed
            self.pipeline_stats['total_records_processed'] = len(transformed_df)
            self.pipeline_stats['success'] = True
            
            logger.info("🎉 ETL Pipeline completed successfully!")
            
        except Exception as e:
            error_msg = f"Pipeline execution failed: {e}"
            logger.error(f"❌ {error_msg}")
            self.pipeline_stats['errors'].append(error_msg)
            self.pipeline_stats['success'] = False
        
        return self._finalize_stats()
    
    def _finalize_stats(self) -> Dict:
        """Finalize pipeline statistics"""
        self.pipeline_stats['end_time'] = datetime.utcnow()
        
        if self.pipeline_stats['start_time']:
            duration = self.pipeline_stats['end_time'] - self.pipeline_stats['start_time']
            self.pipeline_stats['duration_seconds'] = duration.total_seconds()
        
        return self.pipeline_stats.copy()
    
    def get_database_summary(self) -> Dict:
        """Get summary of current database state"""
        try:
            return self.loader.get_load_statistics()
        except Exception as e:
            logger.error(f"❌ Failed to get database summary: {e}")
            return {}
    
    def run_incremental_pipeline(self, hours_back: int = 1) -> Dict:
        """Run pipeline for incremental updates"""
        logger.info(f"🔄 Running incremental pipeline (last {hours_back} hours)...")
        
        # Calculate timestamp for incremental load
        cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
        after_timestamp = int(cutoff_time.timestamp() * 1000)  # Spotify uses milliseconds
        
        return self.run_pipeline(limit=50, after_timestamp=after_timestamp)

def test_complete_pipeline():
    """Test the complete ETL pipeline"""
    print("🧪 Testing Complete ETL Pipeline")
    print("=" * 50)
    
    try:
        # Initialize pipeline
        pipeline = SpotifyETLPipeline()
        
        # Get initial database state
        print("📊 Initial database state:")
        initial_stats = pipeline.get_database_summary()
        for table, count in initial_stats.items():
            print(f"  {table}: {count} records")
        
        # Run the pipeline
        print(f"\n🚀 Running ETL pipeline...")
        results = pipeline.run_pipeline(limit=20)  # Small test run
        
        # Display results
        print(f"\n📊 Pipeline Results:")
        print(f"Success: {results['success']}")
        print(f"Duration: {results['duration_seconds']:.2f} seconds")
        print(f"Total records processed: {results['total_records_processed']}")
        
        if results.get('errors'):
            print(f"Errors: {len(results['errors'])}")
            for error in results['errors']:
                print(f"  - {error}")
        
        # Show extraction stats
        if results.get('extraction_stats'):
            ext_stats = results['extraction_stats']
            print(f"\n📥 Extraction:")
            print(f"  Records: {ext_stats.get('records_extracted', 0)}")
            print(f"  Columns: {ext_stats.get('columns_extracted', 0)}")
        
        # Show transformation stats
        if results.get('transformation_stats'):
            trans_stats = results['transformation_stats']
            print(f"\n🔄 Transformation:")
            print(f"  Input: {trans_stats.get('input_records', 0)}")
            print(f"  Output: {trans_stats.get('output_records', 0)}")
        
        # Show loading stats
        if results.get('loading_stats'):
            load_stats = results['loading_stats']
            if load_stats.get('loading_breakdown'):
                print(f"\n📤 Loading:")
                for table, count in load_stats['loading_breakdown'].items():
                    if count > 0:
                        print(f"  {table}: {count} records")
        
        # Get final database state
        print(f"\n📈 Final database state:")
        final_stats = pipeline.get_database_summary()
        for table, count in final_stats.items():
            change = count - initial_stats.get(table, 0)
            change_str = f" (+{change})" if change > 0 else ""
            print(f"  {table}: {count} records{change_str}")
        
        print(f"\n✅ Pipeline test completed!")
        return results['success']
        
    except Exception as e:
        print(f"❌ Pipeline test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_complete_pipeline()
    sys.exit(0 if success else 1)