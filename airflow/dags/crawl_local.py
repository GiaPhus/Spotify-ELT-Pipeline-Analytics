import sys
import os
import time

from IOManager.MongoDBio import MongoIO
from crawl import main

file_artists = "/home/phu/Pictures/Spotify-ELT-Pipeline-Analytics/artists_stream.txt"

with open(file_artists, "r", encoding="utf-8") as f:
    all_artists = [line.strip() for line in f.readlines()]

print(f"Tổng số nghệ sĩ: {len(all_artists)}")

batch_size = 25
start_index = 2925
total_artists = 3000

for index in range(start_index, total_artists, batch_size):
    end_index = min(index + batch_size, total_artists)
    artists_test = all_artists[index:end_index]
    
    print(f"Đang crawl từ {index} đến {end_index - 1}: {artists_test}")

    with MongoIO() as client:
        main.spotify_crawler(client, artists_test, 0, len(artists_test))

    print(f"Đã crawl xong {len(artists_test)} nghệ sĩ, đang sleep 2 phút...")
    time.sleep(150)


# track dataset : https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset/code
# track feature : https://www.kaggle.com/datasets/tomigelo/spotify-audio-features/data
# track genre : https://www.kaggle.com/datasets/thedevastator/spotify-tracks-genre-dataset
# artists crawl : 



