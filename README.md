# Spotify Data Pipeline

The **Spotify Data Pipeline** project aims to build a scalable and automated workflow that collects, processes, and analyzes music data from the [Spotify Developer API](https://developer.spotify.com/).  
It starts by crawling artist names from [kworb.net's Spotify artist page](https://kworb.net/spotify/artists.html), which ranks the most listened-to artists globally.

These artist names are saved to a plain text file (`artists.txt`) and used as input to fetch detailed music metadata and audio features from the Spotify API.

The pipeline then continues through several stages — from ingestion and transformation to querying and visualization — leveraging a modern big data tech stack including MongoDB, Spark, HDFS, Trino, and Tableau.

This project is ideal for learning how to connect real-world APIs with distributed storage, processing, and analytics tools in a production-like environment.

![Spotify Banner](./images/Spotify_image.jpg)


# 🎯 Project Goals

This project is built as a learning-focused data engineering pipeline with the primary goal of mastering how to collect and move data from multiple sources into a centralized system in a clean, reliable, and structured format.

The end objective is to provide high-quality datasets that enable **Data Scientists (DS)** and **Data Analysts (DA)** to generate insights through analysis and visualization.

Through this project, the developer aims to gain hands-on experience with:
- Designing a real-world data ingestion & transformation flow
- Handling semi-structured and API-based data
- Implementing ETL/ELT best practices
- Building a scalable pipeline that supports downstream analytics

# Pipeline Design

![Pipeline Design](./images/ddl.png)

1. We use **Docker** to containerize each service, and optionally integrate **Airflow** for orchestration and scheduling future enhancements.
2. Artist names are crawled from [kworb.net](https://kworb.net/spotify/listeners.html), then stored in a plain text file (`artists.txt`) as the pipeline's input source.
3. Using multi-threaded Python scripts, each artist name is passed into the **Spotify API**, retrieving data on artists, albums, tracks, and audio features.
4. The raw JSON responses from the API are stored in **MongoDB**, simulating a semi-structured NoSQL landing zone.
5. Data from MongoDB is extracted and loaded into **HDFS** as raw files.
6. From HDFS, **Apache Spark (PySpark)** is used to clean, normalize, and join data into structured datasets (silver & gold layers).
7. Transformed Spark DataFrames are written back into **HDFS** in `.parquet` format for efficient storage and querying.
8. **Trino**, connected through the **Hive Metastore**, is used to query parquet data directly from HDFS.
9. **Tableau** connects to Trino to build interactive dashboards, while **Jupyter Notebooks** are used independently for EDA and feature analysis.

# Data Layer Breakdown

1. General :

![Airflow DAG](./images/DAG.png)

2. Bronze Layer :

- **artists_raw**: Raw JSON data of artist metadata fetched from Spotify API.
- **albums_raw**: Raw album information per artist, including release date, label, and popularity.
- **tracks_raw**: Raw track data for each album, including track name, duration, and popularity.
- **audio_features_raw**: Technical audio features (e.g., danceability, energy, valence...) per track.
- **genres_raw**: Genre list per artist, often containing multiple genre tags.

All of these datasets are ingested directly from the **Spotify API**, and stored unprocessed in **MongoDB** as the raw landing zone.

---

3. Silver Layer :

- **silver_artists**: Cleaned and normalized artist data. Removed nulls, renamed columns, and flattened nested fields.
- **silver_album**: Album metadata with standardized date formats and cleaned column names.
- **tracks_data**: Flattened track table with clear schema (`track_id`, `artist_id`, `album_id`, etc.).
- **silver_feature_music**: Cleaned audio features dataset matched to track_id and artist.
- **silver_artists_genres**: Exploded genre list per artist into multiple rows for easier aggregation.

Data from MongoDB is exported to **HDFS** and cleaned using **PySpark** before being stored in Parquet format.

---

4. Gold Layer :

- **gold_track_metadata**: A fully joined dataset combining tracks, albums, artists, and genres. Aggregated genre tags per artist using `collect_set`, and ensured one record per track.
- **gold_feature_matrix**: Technical audio features enriched with track name and artist name, prepared for visualization and machine learning tasks.

This layer represents a **business-meaningful view** of the data, curated for downstream analytics.

---

5. Warehouse Layer :

- **warehouse_track_dashboard**: Combined dataset of track metadata and audio features, used for Tableau dashboards.
- **warehouse_track_recommendation**: Clean feature matrix prepared for building a content-based recommendation system.

All outputs are stored in **Parquet format on HDFS**, and can optionally be migrated into warehouse for further reporting or analytics.


# Set up environment

```bash
git clone https://github.com/GiaPhus/Spotify-ELT-Pipeline-Analytics.git
cd spotify-data-pipeline
```

---

### Create Environment Files

Create two required files: `.env` and `hadoop.env`

#### `.env`

```env
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
MONGO_URI=your_mongo_uri
```

#### `hadoop.env`

```env
CORE_CONF_fs_defaultFS=hdfs://namenode:8020
HDFS_CONF_dfs_replication=1
```

> You can find sample templates in `.env.example` and `hadoop.env.example`

---

### Set Up Docker for Airflow

This project uses [Apache Airflow's official Docker Compose setup](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

Run the following to initialize Airflow metadata and create the default user:

```bash
docker-compose up airflow-init
```

Once initialization completes, start all services in detached mode:

```bash
docker-compose up -d
```

---

