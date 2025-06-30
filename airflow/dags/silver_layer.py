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

def artists_silver_layer(spark: SparkSession, table_name : str = "artists_data"):
    hdfs_uri = f"hdfs://namenode:8020/bronze_layer/{table_name}.parquet"
    artists_data = spark.read.parquet(hdfs_uri)
    df = artists_data.withColumn("artist_id",col("id"))
    df_clean = df.drop_duplicates(["artist_id"])
    df_clean = df_clean.drop("_id")
    print("Record before clean duplicate",df.count())
    print("After",df_clean.count())
    df_clean = df_clean.withColumn("external_urls_artists",col("external_urls.spotify"))
    df_clean = df_clean.withColumn("followers number",col("followers.total"))
    df_clean = df_clean.drop("followers")
    df_clean = df_clean.drop("href")
    df_clean = df_clean.drop("external_urls")
    artist_genres = df_clean.select(
        col("artist_id"),col("name"),
        explode('genres').alias("artists_genres")
    )
    artist_genres.write.mode("overwrite").parquet("hdfs://namenode:8020/silver_layer/silver_artists_genres.parquet")
    df_clean = df_clean.drop("genres")
    df_clean = df_clean.withColumn("images_artists",col("images")[0]["url"]).drop("images")
    df_clean.write.mode("overwrite").parquet("hdfs://namenode:8020/silver_layer/silver_artists.parquet")
    print("Done artist table")
    
def feature_music_silver_layer(spark: SparkSession, table_name : str = "feature_music"):
    hdfs_uri = f"hdfs://namenode:8020/bronze_layer/{table_name}.parquet"
    df_feature = spark.read.parquet(hdfs_uri)
    df = df_feature.dropDuplicates()
    df.write.mode("overwrite").parquet("hdfs://namenode:8020/silver_layer/silver_feature_music.parquet")
    print("Done feature")

def albums_silver_layer(spark: SparkSession, table_name : str = "albums_data"):
    hdfs_uri = f"hdfs://namenode:8020/bronze_layer/{table_name}.parquet"
    albums_silver = spark.read.parquet(hdfs_uri)
    df = albums_silver.withColumn("album_id",col("id"))
    df_clean = df.drop_duplicates(["album_id"])
    df_clean = df_clean.drop("_id")
    df_clean = df_clean.withColumn("external_urls_albums",col("external_urls.spotify"))
    df_clean = df_clean.drop("href")
    df_clean = df_clean.drop("external_urls")
    df_clean = df_clean.withColumn("album_name",col("name"))
    df_clean = df_clean.drop("name")
    df_clean = df_clean.withColumn("uri",
                                   regexp_replace("uri","spotify:album:","open.spotify.com/album/"))
    df_clean = df_clean.drop("copyrights","external_ids","genres")
    df_clean = df_clean.withColumn("image_album",col("images")[0]["url"]).drop("images")
    df_clean.write.mode("overwrite").parquet("hdfs://namenode:8020/silver_layer/silver_album.parquet")
    print("Done album table")

def tracks_silver_layer(spark: SparkSession, table_name : str = "tracks_data"):
    hdfs_uri = f"hdfs://namenode:8020/bronze_layer/{table_name}.parquet"
    tracks_silver = spark.read.parquet(hdfs_uri)
    df = tracks_silver.withColumn("track_id",col("id"))
    df_clean = df.drop_duplicates(["track_id"])
    df_clean = df_clean.drop("_id")
    df_clean = df_clean.drop("external_ids")
    df_clean = df_clean.drop("href")
    df_clean = df_clean.withColumn("external_urls_tracks",col("external_urls.spotify"))
    df_clean = df_clean.drop("external_urls")
    df_clean = df_clean.withColumn("uri",
                                   regexp_replace("uri","spotify:track:","open.spotify.com/track/"))
    df_clean.write.mode("overwrite").parquet(f"hdfs://namenode:8020/silver_layer/{table_name}.parquet")
