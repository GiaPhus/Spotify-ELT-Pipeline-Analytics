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


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set, concat_ws


def gold_track_metadata(spark: SparkSession):

    tracks_df = spark.read.parquet("hdfs://namenode:8020/silver_layer/tracks_data.parquet")
    albums_df = spark.read.parquet("hdfs://namenode:8020/silver_layer/silver_album.parquet")
    artists_df = spark.read.parquet("hdfs://namenode:8020/silver_layer/silver_artists.parquet")
    artists_genres_df = spark.read.parquet("hdfs://namenode:8020/silver_layer/silver_artists_genres.parquet")
    
    artists_genres_df = (
        artists_genres_df.groupBy("artist_id")
        .agg(concat_ws(", ", collect_set("artists_genres")).alias("genres"))
    )

    
    artists_df = artists_df.selectExpr(
        "artist_id",
        "id as artist_spotify_id",
        "name as artist_name",
        "popularity as popularity_artist",
        "`followers number`",
        "type as type_artist",
        "uri as uri_artist",
        "external_urls_artists",
        "images_artists"
    )
    
    albums_df = albums_df.selectExpr(
        "album_id",
        "artist_id as album_artist_id",
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
        "popularity as popularity_track",
        "uri as uri_track",
        "external_urls_tracks"
    )
    
    df = (
        tracks_df
        .join(albums_df, (tracks_df.track_album_id == albums_df.album_id) & 
                  (tracks_df.track_artist_id == albums_df.album_artist_id), how="inner")
        .join(artists_df, tracks_df.track_artist_id == artists_df.artist_id, how="inner")
        .join(artists_genres_df, tracks_df.track_artist_id == artists_genres_df.artist_id, how="inner")
    )
    
    result_df = df.select(
        "track_id", "track_name", "duration_ms", "explicit", "popularity_track", "uri_track",
        "album_name", "album_type", "release_date", "label", "popularity_album", "image_album",
        "artist_name", "popularity_artist", "followers number", "artist_spotify_id", "type_artist", "uri_artist","images_artists",
        "genres"
    ).dropDuplicates(["track_id"])

    result_df.write.mode("overwrite").parquet("hdfs://namenode:8020/gold_layer/gold_track_metadata.parquet")


    

def gold_feature_matrix(spark: SparkSession):
    feature_path = "hdfs://namenode:8020/silver_layer/silver_feature_music.parquet"
    tracks_path = "hdfs://namenode:8020/silver_layer/tracks_data.parquet"
    output_path = "hdfs://namenode:8020/gold_layer/gold_feature_matrix.parquet"
    features_df = spark.read.parquet(feature_path)
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
    df_joined = features_df.alias("f").join(
    tracks_joined.alias("t"),
    on=(col("f.track_id") == col("t.track_id")) &
    (col("f.artists") == col("t.artist_name")),
    how="inner"
)

    final_df = df_joined.select(
        col("t.track_id"), 
        col("f.danceability"), col("f.energy"), col("f.loudness"), col("f.speechiness"),
        col("f.acousticness"), col("f.instrumentalness"), col("f.liveness"), col("f.valence"),
        col("f.tempo"), col("f.duration_ms")
    ).dropDuplicates(["track_id"])

    final_df.write.mode("overwrite").parquet(output_path)