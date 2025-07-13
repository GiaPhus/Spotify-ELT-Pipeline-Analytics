## 🧭 Introduction

The **Spotify Data Pipeline** project aims to build a scalable and automated workflow that collects, processes, and analyzes music data from the [Spotify Developer API](https://developer.spotify.com/).  
It starts by crawling artist names from [kworb.net's Spotify artist page](https://kworb.net/spotify/artists.html), which ranks the most listened-to artists globally.

These artist names are saved to a plain text file (`artists.txt`) and used as input to fetch detailed music metadata and audio features from the Spotify API.

The pipeline then continues through several stages — from ingestion and transformation to querying and visualization — leveraging a modern big data tech stack including MongoDB, Spark, HDFS, Trino, and Tableau.

This project is ideal for learning how to connect real-world APIs with distributed storage, processing, and analytics tools in a production-like environment.

![Spotify Banner](./images/Spotify_image.jpeg)