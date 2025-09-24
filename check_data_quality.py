"""Quick data quality check"""
import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

# Database connection
conn = psycopg2.connect(
    host=os.getenv('POSTGRES_HOST', 'localhost'),
    database=os.getenv('POSTGRES_DB', 'spotify_data'),
    user=os.getenv('POSTGRES_USER', 'postgres'),
    password=os.getenv('POSTGRES_PASSWORD', '')
)

print('🎵 Data Quality Check')
print('=' * 50)

cursor = conn.cursor()

# Check audio features variation
print('\n📊 Audio Features Sample:')
cursor.execute('SELECT track_id, danceability, energy, valence, tempo FROM audio_features LIMIT 5')
features = cursor.fetchall()

for track_id, dance, energy, val, tempo in features:
    print(f'  {track_id[:8]}... | Dance: {dance:.3f} | Energy: {energy:.3f} | Tempo: {tempo:.1f}')

# Check artist data
print('\n👨‍🎤 Artist Data Sample:')
cursor.execute('SELECT name, genres, popularity, followers FROM artists WHERE genres IS NOT NULL LIMIT 3')
artists = cursor.fetchall()

for name, genres, pop, followers in artists:
    print(f'  {name}: {genres} | Pop: {pop} | Followers: {followers:,}')

# Check counts
print('\n📈 Record Counts:')
cursor.execute('SELECT COUNT(*) FROM artists WHERE genres IS NOT NULL')
artists_with_genres = cursor.fetchone()[0]
cursor.execute('SELECT COUNT(*) FROM artists')
total_artists = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM audio_features')
total_features = cursor.fetchone()[0]

print(f'  Artists with genres: {artists_with_genres}/{total_artists}')
print(f'  Audio features: {total_features}')

conn.close()
print('\n✅ Data quality check completed!')