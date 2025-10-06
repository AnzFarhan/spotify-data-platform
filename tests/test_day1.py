"""
Day 1 Tests - Basic environment testing
"""
import sys
import os
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_python_packages():
    """Test that required packages are installed"""
    print("🐍 Testing Python packages...")
    
    required_packages = ['pandas', 'psycopg2', 'spotipy', 'dotenv']
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"  ✅ {package}")
        except ImportError:
            print(f"  ❌ {package} - not installed")
            return False
    
    print("✅ All required packages installed!")
    return True

def test_project_structure():
    """Test that project structure is correct"""
    print("📁 Testing project structure...")
    
    required_folders = [
        'DE',
        'DA', 
        'DS',
        'config',
        'sql',
        'tests'
    ]
    
    for folder in required_folders:
        folder_path = project_root / folder
        if folder_path.exists():
            print(f"  ✅ {folder}/")
        else:
            print(f"  ❌ {folder}/ - missing")
            return False
    
    print("✅ Project structure looks good!")
    return True

def test_config_files():
    """Test configuration files"""
    print("⚙️ Testing configuration...")
    
    # Test database config
    try:
        from config.database import DatabaseConfig
        db_config = DatabaseConfig()
        db_ok = db_config.test_config()
    except Exception as e:
        print(f"❌ Database config error: {e}")
        db_ok = False
    
    # Test Spotify config
    try:
        from config.spotify import SpotifyConfig
        spotify_config = SpotifyConfig()
        spotify_ok = spotify_config.test_config()
    except Exception as e:
        print(f"❌ Spotify config error: {e}")
        spotify_ok = False
    
    return db_ok and spotify_ok

def run_all_tests():
    """Run all Day 1 tests"""
    print("🚀 Running Day 1 Environment Tests")
    print("=" * 40)
    
    tests = [
        ("Python Packages", test_python_packages),
        ("Project Structure", test_project_structure),
        ("Configuration", test_config_files)
    ]
    
    results = []
    for test_name, test_func in tests:
        print(f"\n📋 {test_name}")
        print("-" * 30)
        result = test_func()
        results.append(result)
    
    print(f"\n📊 Results Summary")
    print("=" * 20)
    
    all_passed = all(results)
    if all_passed:
        print("🎉 All Day 1 tests passed!")
        print("✅ Environment setup complete!")
        print("\n👉 Next: Set up Spotify Developer account")
    else:
        print("⚠️ Some tests failed. Please fix before continuing.")
    
    return all_passed

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
