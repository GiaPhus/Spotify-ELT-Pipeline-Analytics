from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import StringType
from pyspark.sql.functions import split, explode, trim
from pyspark.sql.functions import collect_set, concat_ws


def gold_track_metadata(spark: SparkSession):
    tracks_df = spark.read.parquet("hdfs://namenode:8020/silver_layer/tracks_data.parquet")
    albums_df = spark.read.parquet("hdfs://namenode:8020/silver_layer/silver_album.parquet")
    artists_df = spark.read.parquet("hdfs://namenode:8020/silver_layer/silver_artists.parquet")
    artists_genres_df = spark.read.parquet("hdfs://namenode:8020/silver_layer/silver_artists_genres.parquet")
    artists_df = artists_df.selectExpr(
        "artist_id", 
        "id as artist_spotify_id", 
        "name as artist_name", 
        "popularity as popularity_artist", 
        "type as type_artist", 
        "uri as uri_artist", 
        "external_urls_artists", 
        "`followers number`", 
        "images_artists"
    )
    albums_df = albums_df.selectExpr(
        "album_id", 
        "artist_id as album_artists_id", 
        "id as album_spotify_id", 
        "album_name", 
        "album_type", 
        "release_date", 
        "label", 
        "popularity as popularity_album", 
        "image_album", 
        "external_urls_albums"
    )
    tracks_df = tracks_df.selectExpr(
        "track_id", 
        "album_id as track_album_id", 
        "artist_id as track_artist_id", 
        "name as track_name", 
        "duration_ms", 
        "explicit", 
        "external_urls_tracks"
    )
    artists_genres_df = artists_genres_df.selectExpr(
        "artist_id as artist_genres_id", 
        "genre"
    )
    artists_genres_df = artists_genres_df.groupBy("artist_genres_id") \
                                     .agg(concat_ws(", ", collect_set("genre")).alias("genres"))
    df = (
        tracks_df
        .join(albums_df, tracks_df.track_album_id == albums_df.album_id, how="inner")
        .join(artists_df, tracks_df.track_artist_id == artists_df.artist_id, how="inner")
        .join(artists_genres_df, tracks_df.track_artist_id == artists_genres_df.artist_genres_id, how="inner")
    )
    df = df.select(
        "track_id", "track_name", "external_urls_tracks",
        "track_artist_id", "artist_name", "`followers number`", "images_artists", "external_urls_artists",
        "track_album_id", "album_name", "release_date", "image_album", "external_urls_albums",
        "duration_ms", "explicit", "genres"
    )
    df.write.mode("overwrite").parquet("hdfs://namenode:8020/gold_layer/gold_track_metadata.parquet")

    

def gold_feature_matrix(spark: SparkSession):
    feature_path = "hdfs://namenode:8020/silver_layer/silver_feature_music.parquet"
    tracks_path = "hdfs://namenode:8020/silver_layer/tracks_data.parquet"
    output_path = "hdfs://namenode:8020/gold_layer/gold_feature_matrix.parquet"
    features_df = spark.read.parquet(feature_path).withColumnRenamed("track", "track_name")
    tracks_df = spark.read.parquet(tracks_path).selectExpr(
        "track_id", "name as track_name", "artist_id as track_artist_id"
    )
    artists_df = spark.read.parquet("hdfs://namenode:8020/silver_layer/silver_artists.parquet").selectExpr(
        "artist_id", "name as artist_name"
    )
    tracks_joined = tracks_df.join(
        artists_df,
        tracks_df["track_artist_id"] == artists_df["artist_id"],
        how="left"
    ).select("track_id", "track_name", "artist_name")
    df_joined = features_df.join(
        tracks_joined,
        on=(features_df["track_name"] == tracks_joined["track_name"]) &
        (features_df["artist"] == tracks_joined["artist_name"]),
        how="inner"
    )
    feature_cols = ['track_id', 'danceability', 'energy', 'loudness', 'speechiness',
                    'acousticness', 'instrumentalness', 'liveness', 'valence',
                    'tempo', 'duration_ms']
    final_df = df_joined.select(feature_cols).dropDuplicates(["track_id"])
    final_df.write.mode("overwrite").parquet(output_path)
    
def gold_track_search_index(spark: SparkSession):
    track_meta = spark.read.parquet("hdfs://namenode:8020/gold_layer/gold_track_metadata.parquet")
    feature = spark.read.parquet("hdfs://namenode:8020/gold_layer/gold_feature_matrix.parquet")

    search_index = (
        track_meta
        .join(feature, "track_id")
        .select("track_id", "track_name", "artist_name", "album_name", "release_date", "genres", "danceability", "energy", "valence")
    )
    search_index.write.mode("overwrite").parquet("hdfs://namenode:8020/gold_layer/gold_track_search_index.parquet")
