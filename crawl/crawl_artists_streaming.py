import requests
import pandas as pd
from bs4 import BeautifulSoup

# Define URLs
URL = "https://kworb.net/spotify/artists.html"

# Define path to store list of artists name and stream
FILE_PATH = "/home/phu/Spotify ELT Pipeline Analytics/Spotify-ELT-Pipeline-Analytics/artists_stream.txt"


def get_artists_and_stream(url: str):
    """Get artists name and stream from URL."""
    try:
        # Read table from URL
        spotify_artists_table = pd.read_html(url)[0]
        if spotify_artists_table.empty:
            print("No data found in the table.")
            return []

        # Get artists name and streams
        artists_name = spotify_artists_table["Artist"].tolist()
        streams = spotify_artists_table["Streams"].tolist()  # Extract stream data

        # Combine both columns into a list of tuples
        artists_and_stream = list(zip(artists_name, streams))
        return artists_and_stream

    except Exception as e:
        print(f"Error while fetching data from URL: {e}")
        return []


def store_artists_and_stream(artists_and_stream, file_name=FILE_PATH):
    """Store artists name and stream into a file."""
    try:
        with open(file_name, 'a') as f:
            # Write header
            f.write("Artist,Stream\n")
            # Write data for each artist and stream
            for artist, stream in artists_and_stream:
                f.write(f"{artist},{stream}\n")
        print(f"Successfully saved artists and streams to {file_name}")
    except Exception as e:
        print(f"Error while writing to file: {e}")


def artists_crawler(path=FILE_PATH):
    """Main function."""
    artists_and_stream = get_artists_and_stream(URL)
    if artists_and_stream:
        store_artists_and_stream(artists_and_stream, path)


if __name__ == "__main__":
    print("Start")
    artists_crawler(FILE_PATH)
    print("Success")
