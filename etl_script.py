import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import mysql.connector
import re
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Spotify API setup
client_id = '383606752f144b33a8ac5db409ec8d96'
client_secret = '31936de7fbe349949a3e54df8cd9bea6'

sp = spotipy.Spotify(auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret))

# MySQL setup
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="spotify_db"
)

cursor = db.cursor()

def extract_track_id(url):
    """Extract Spotify track ID from URL."""
    pattern = r"track\/([a-zA-Z0-9]+)"
    match = re.search(pattern, url)
    return match.group(1) if match else None

def fetch_track_data(track_id):
    """Fetch track metadata from Spotify API."""
    track = sp.track(track_id)
    data = {
        "track_id": track["id"],
        "track_name": track["name"],
        "artist": track["artists"][0]["name"],
        "album": track["album"]["name"],
        "popularity": track["popularity"],
        "duration_ms": track["duration_ms"],
        "duration_min": track["duration_ms"] / 60000  # Convert to minutes
    }
    return data

def load_data_to_mysql(data):
    """Insert track data into MySQL."""
    insert_query = """
    INSERT INTO spotify_tracks (track_id, track_name, artist, album, popularity, duration_ms, duration_min)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        popularity = VALUES(popularity),
        duration_ms = VALUES(duration_ms),
        duration_min = VALUES(duration_min),
        fetched_at = CURRENT_TIMESTAMP
    """
    values = (
        data["track_id"], data["track_name"], data["artist"], data["album"],
        data["popularity"], data["duration_ms"], data["duration_min"]
    )
    cursor.execute(insert_query, values)
    db.commit()

def main():
    # Read track URLs from a file
    with open('track_urls.txt', 'r') as file:
        urls = file.readlines()

    all_data = []
    for url in urls:
        url = url.strip()
        track_id = extract_track_id(url)
        if track_id:
            try:
                data = fetch_track_data(track_id)
                load_data_to_mysql(data)
                all_data.append(data)
                print(f"Inserted: {data['track_name']} by {data['artist']}")
            except Exception as e:
                print(f"Error fetching data for {url}: {e}")
        else:
            print(f"Invalid URL: {url}")

    # Visualization - plot popularity vs duration
    df = pd.DataFrame(all_data)
    plt.scatter(df['popularity'], df['duration_min'])
    plt.title('Track Popularity vs Duration (minutes)')
    plt.xlabel('Popularity')
    plt.ylabel('Duration (minutes)')
    plt.show()

if __name__ == "__main__":
    main()
