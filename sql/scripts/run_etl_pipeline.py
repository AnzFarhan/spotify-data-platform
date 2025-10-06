"""
Day 3: ETL Pipeline Runner Script
Easy-to-use script for running the complete pipeline
"""
import sys
import argparse
from pathlib import Path
from datetime import datetime

# Add project root to path (go up two levels from sql/scripts/)
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
from DE.pipeline_orchestrator import SpotifyETLPipeline

load_dotenv()

def run_full_pipeline(limit: int = 50):
    """Run the complete ETL pipeline"""
    print("🚀 Running Complete Spotify ETL Pipeline")
    print("=" * 50)
    print(f"Timestamp: {datetime.now()}")
    print(f"Track limit: {limit}")
    print()
    
    try:
        # Initialize pipeline
        pipeline = SpotifyETLPipeline()
        
        # Get initial database state
        print("📊 Initial Database State:")
        initial_stats = pipeline.get_database_summary()
        for table, count in initial_stats.items():
            print(f"  {table}: {count:,} records")
        print()
        
        # Run the pipeline
        print("🔄 Executing ETL Pipeline...")
        results = pipeline.run_pipeline(limit=limit)
        
        # Display results
        print("\n📋 Pipeline Execution Results:")
        print(f"  Success: {'✅ YES' if results['success'] else '❌ NO'}")
        print(f"  Duration: {results['duration_seconds']:.2f} seconds")
        print(f"  Records Processed: {results['total_records_processed']:,}")
        
        if results.get('errors'):
            print(f"  Errors: {len(results['errors'])}")
            for error in results['errors']:
                print(f"    - {error}")
        
        # Detailed statistics
        print(f"\n📈 Detailed Statistics:")
        
        # Extraction
        if results.get('extraction_stats'):
            ext = results['extraction_stats']
            print(f"  Extraction:")
            print(f"    Records: {ext.get('records_extracted', 0):,}")
            print(f"    Columns: {ext.get('columns_extracted', 0)}")
        
        # Transformation  
        if results.get('transformation_stats'):
            trans = results['transformation_stats']
            print(f"  Transformation:")
            print(f"    Input: {trans.get('input_records', 0):,}")
            print(f"    Output: {trans.get('output_records', 0):,}")
        
        # Loading
        if results.get('loading_stats', {}).get('loading_breakdown'):
            print(f"  Loading:")
            for table, count in results['loading_stats']['loading_breakdown'].items():
                if count > 0:
                    print(f"    {table}: {count:,} records")
        
        # Final database state
        print(f"\n📊 Final Database State:")
        final_stats = pipeline.get_database_summary()
        for table, count in final_stats.items():
            change = count - initial_stats.get(table, 0)
            change_str = f" (+{change:,})" if change > 0 else ""
            print(f"  {table}: {count:,} records{change_str}")
        
        if results['success']:
            print(f"\n🎉 ETL Pipeline completed successfully!")
        else:
            print(f"\n⚠️ ETL Pipeline completed with issues")
        
        return results['success']
        
    except Exception as e:
        print(f"\n❌ Pipeline failed with error: {e}")
        return False

def run_incremental_pipeline(hours: int = 1):
    """Run incremental pipeline for recent data"""
    print(f"🔄 Running Incremental ETL Pipeline (Last {hours} hours)")
    print("=" * 50)
    
    try:
        pipeline = SpotifyETLPipeline()
        results = pipeline.run_incremental_pipeline(hours_back=hours)
        
        print(f"\n📊 Incremental Pipeline Results:")
        print(f"  Success: {'✅ YES' if results['success'] else '❌ NO'}")
        print(f"  Records: {results['total_records_processed']:,}")
        print(f"  Duration: {results['duration_seconds']:.2f} seconds")
        
        return results['success']
        
    except Exception as e:
        print(f"❌ Incremental pipeline failed: {e}")
        return False

def main():
    """Main function with command line arguments"""
    parser = argparse.ArgumentParser(description='Run Spotify ETL Pipeline')
    parser.add_argument('--mode', choices=['full', 'incremental'], default='full',
                       help='Pipeline mode (default: full)')
    parser.add_argument('--limit', type=int, default=50,
                       help='Number of tracks to extract (default: 50)')
    parser.add_argument('--hours', type=int, default=1,
                       help='Hours back for incremental mode (default: 1)')
    
    args = parser.parse_args()
    
    if args.mode == 'full':
        success = run_full_pipeline(limit=args.limit)
    elif args.mode == 'incremental':
        success = run_incremental_pipeline(hours=args.hours)
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()