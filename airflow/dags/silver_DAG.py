from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
import os
from IOManager.MongoDBio import MongoIO,auth_mongoDB
from IOManager.SparkIO import SparkIO
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode
from pyspark.sql.functions import regexp_replace



default_args ={
    'owner' : "Phu",
    'depends_on_past' : False,
    'start_date': datetime.today()   
} 

def artists_silver_layer(spark: SparkSession, table_name : str = "artists_data"):
    hdfs_uri = f"hdfs://namenode:8020/bronze_layer/{table_name}.parquet"
    artists_data = spark.read.parquet(hdfs_uri)
    df = artists_data.withColumn("artist_id",col("id"))
    df_clean = df.drop_duplicates(["artist_id"])
    df_clean = df_clean.drop("_id")
    print("Record before clean duplicate",df.count())
    print("After",df_clean.count())
    df_clean = df_clean.withColumn("external_urls_spotify",col("external_urls.spotify"))
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
    feature_silver = spark.read.parquet(hdfs_uri)
    df = feature_silver.withColumn("id",col("_id.$oid"))
    df = feature_silver.withColumn("track",col("song"))
    df = df.dropDuplicates()
    df_genres=spark.read.parquet("hdfs://namenode:8020/silver_layer/silver_artists_genres.parquet")
    artists_feature_genres = df.select(
        col("artist"),
        col("genre")
    ).filter((col("genre").isNotNull()) & (col("genre") != "")).distinct()
    artist_mapping = df_genres.select(
    col("artist_id"),
    col("name").alias("artist")
    )
    artists_feature_genres_mapped = artists_feature_genres.join(
        artist_mapping,
        on = "artist",
        how = "inner",       
    ).select("artist_id", col("genre").alias("artists_genres"))
    df_genres = spark.read.parquet("hdfs://namenode:8020/silver_layer/silver_artists_genres.parquet")
    updated_genres = df_genres.unionByName(artists_feature_genres_mapped).dropDuplicates(["artist_id", "artists_genres"])
    updated_genres.write.mode("overwrite").parquet("hdfs://namenode:8020/silver_layer/silver_artists_genres.parquet")
    print("Done feature")

def albums_silver_layer(spark: SparkSession, table_name : str = "albums_data"):
    hdfs_uri = f"hdfs://namenode:8020/bronze_layer/{table_name}.parquet"
    albums_silver = spark.read.parquet(hdfs_uri)
    df = albums_silver.withColumn("album_id",col("id"))
    df_clean = df.drop_duplicates(["album_id"])
    df_clean = df_clean.drop("_id")
    df_clean = df_clean.withColumn("external_urls_spotify",col("external_urls.spotify"))
    df_clean = df_clean.drop("href")
    df_clean = df_clean.drop("external_urls")
    df_clean = df_clean.withColumn("album_name",col("name"))
    df_clean = df_clean.drop("name")
    df_clean = df_clean.withColumn("uri",
                                   regexp_replace("uri","spotify:album:","open.spotify.com/album/"))
    df_clean = df_clean.drop("copyrights","external_id","genres")
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
    df_clean = df_clean.withColumn("external_urls_spotify",col("external_urls.spotify"))
    df_clean = df_clean.drop("external_urls")
    df_clean = df_clean.withColumn("uri",
                                   regexp_replace("uri","spotify:track:","open.spotify.com/track/"))
    df_clean.write.mode("overwrite").parquet(f"hdfs://namenode:8020/silver_layer/{table_name}.parquet")
