"""
Day 3 Tests - Complete ETL Pipeline Testing
"""
import sys
from pathlib import Path
import pandas as pd
import logging

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging for tests
logging.basicConfig(level=logging.INFO)

def test_enhanced_extractor():
    """Test enhanced Spotify extractor"""
    print("🎵 Testing Enhanced Extractor...")
    
    try:
        from DE.extractors.spotify_extractor_v2 import SpotifyExtractorV2
        
        extractor = SpotifyExtractorV2()
        
        # Test user info
        user_info = extractor.extract_user_info()
        if user_info and user_info.get('user_id'):
            print(f"  ✅ User info: {user_info.get('display_name', 'Unknown')}")
        else:
            print("  ⚠️ User info extraction - authentication may be required")
        
        # Test track extraction (small sample)
        df = extractor.extract_recent_tracks(limit=5)
        if not df.empty:
            print(f"  ✅ Track extraction: {len(df)} tracks, {len(df.columns)} columns")
        else:
            print("  ⚠️ No tracks extracted - may need authentication")
        
        return not df.empty
        
    except Exception as e:
        print(f"  ❌ Enhanced extractor test failed: {e}")
        return False

def test_data_transformer():
    """Test data transformation pipeline"""
    print("🔄 Testing Data Transformer...")
    
    try:
        from DE.transformers.data_transformer import SpotifyDataTransformer
        
        # Create sample data
        sample_data = {
            'track_id': ['1', '2', '3'],
            'track_name': ['  Song 1  ', 'Song 2!', 'Song 3'],
            'artist_name': ['Artist A', 'Artist B', 'Artist C'],
            'played_at': ['2024-01-15T10:00:00Z', '2024-01-15T11:00:00Z', '2024-01-15T12:00:00Z'],
            'energy': [0.8, 0.4, 0.9],
            'valence': [0.7, 0.3, 0.8],
            'duration_ms': [210000, 180000, 240000]
        }
        
        df = pd.DataFrame(sample_data)
        transformer = SpotifyDataTransformer()
        
        # Transform data
        transformed_df, quality_report = transformer.transform(df)
        
        if not transformed_df.empty and len(transformed_df.columns) > len(df.columns):
            print(f"  ✅ Transformation: {len(df)} → {len(transformed_df)} rows, added features")
            
            # Check if new features were created
            new_features = ['mood_category', 'duration_category']
            found_features = [f for f in new_features if f in transformed_df.columns]
            print(f"  ✅ New features: {found_features}")
            
        else:
            print("  ❌ Transformation failed or no new features created")
            return False
        
        return True
        
    except Exception as e:
        print(f"  ❌ Data transformer test failed: {e}")
        return False

def test_database_loader():
    """Test database loader"""
    print("📤 Testing Database Loader...")
    
    try:
        from DE.loaders.database_loader import SpotifyDatabaseLoader
        
        loader = SpotifyDatabaseLoader()
        
        # Test connection and get stats
        stats = loader.get_load_statistics()
        
        if stats:
            print(f"  ✅ Database connection successful")
            print(f"  📊 Current data: {sum(stats.values())} total records")
            for table, count in stats.items():
                if count > 0:
                    print(f"      {table}: {count}")
        else:
            print("  ❌ Database connection failed")
            return False
        
        return True
        
    except Exception as e:
        print(f"  ❌ Database loader test failed: {e}")
        return False

def test_pipeline_orchestrator():
    """Test pipeline orchestrator"""
    print("⚙️ Testing Pipeline Orchestrator...")
    
    try:
        from DE.pipeline_orchestrator import SpotifyETLPipeline
        
        # Initialize pipeline
        pipeline = SpotifyETLPipeline()
        
        # Test initialization
        if pipeline.extractor and pipeline.transformer and pipeline.loader:
            print("  ✅ Pipeline components initialized")
        else:
            print("  ❌ Pipeline initialization failed")
            return False
        
        # Test database summary
        summary = pipeline.get_database_summary()
        if summary:
            print(f"  ✅ Database summary retrieved: {len(summary)} tables")
        else:
            print("  ⚠️ Could not retrieve database summary")
        
        print("  ✅ Pipeline orchestrator initialized successfully")
        return True
        
    except Exception as e:
        print(f"  ❌ Pipeline orchestrator test failed: {e}")
        return False

def run_integration_test():
    """Run a small integration test of the complete pipeline"""
    print("🔗 Running Integration Test...")
    
    try:
        from DE.pipeline_orchestrator import SpotifyETLPipeline
        
        pipeline = SpotifyETLPipeline()
        
        print("  🚀 Running mini pipeline (5 tracks)...")
        
        # Run pipeline with very small limit for testing
        results = pipeline.run_pipeline(limit=5)
        
        if results['success']:
            print("  ✅ Integration test passed!")
            print(f"     Duration: {results['duration_seconds']:.2f}s")
            print(f"     Records: {results['total_records_processed']}")
            
            if results.get('loading_stats', {}).get('loading_breakdown'):
                print("     Loaded data:")
                for table, count in results['loading_stats']['loading_breakdown'].items():
                    if count > 0:
                        print(f"       {table}: {count}")
        else:
            print("  ⚠️ Integration test completed with issues")
            if results.get('errors'):
                for error in results['errors']:
                    print(f"       Error: {error}")
        
        return results['success']
        
    except Exception as e:
        print(f"  ❌ Integration test failed: {e}")
        return False

def run_all_day3_tests():
    """Run all Day 3 tests"""
    print("🧪 Running Day 3 ETL Pipeline Tests")
    print("=" * 50)
    
    tests = [
        ("Enhanced Extractor", test_enhanced_extractor),
        ("Data Transformer", test_data_transformer),
        ("Database Loader", test_database_loader),
        ("Pipeline Orchestrator", test_pipeline_orchestrator),
        ("Integration Test", run_integration_test)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n📋 {test_name}")
        print("-" * 30)
        try:
            result = test_func()
            results.append(result)
            status = "✅ PASSED" if result else "⚠️ ISSUES"
            print(f"Result: {status}")
        except Exception as e:
            print(f"❌ TEST ERROR: {e}")
            results.append(False)
    
    # Summary
    print(f"\n📊 Day 3 Test Results")
    print("=" * 30)
    
    passed = sum(results)
    total = len(results)
    
    for i, (test_name, _) in enumerate(tests):
        status = "✅" if results[i] else "❌"
        print(f"{status} {test_name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All Day 3 tests passed!")
        print("✅ ETL Pipeline is ready for production!")
        print("\n👉 Next steps:")
        print("   - Schedule regular pipeline runs")
        print("   - Set up monitoring and alerts")
        print("   - Begin analytics dashboard development")
    else:
        print(f"\n⚠️ {total - passed} tests need attention")
        print("🔧 Please fix issues before proceeding to analytics")
    
    return passed == total

if __name__ == "__main__":
    success = run_all_day3_tests()
    sys.exit(0 if success else 1)

