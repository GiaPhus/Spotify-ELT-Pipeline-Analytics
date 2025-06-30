from pyspark.sql import SparkSession
import os
import urllib
from IOManager.SparkIO import SparkIO
from pyspark.sql.types import *
from dotenv import load_dotenv
from pyspark import SparkConf
from pymongo import MongoClient

load_dotenv(dotenv_path="./.env")
def auth_mongoDB():
    user =  urllib.parse.quote_plus(os.getenv("MONGODB_USER", ""))
    password = urllib.parse.quote_plus(os.getenv("MONGODB_PASSWORD", ""))
    cluster = os.getenv("MONGODB_SRV", "").replace("mongodb+srv://", "").strip("/")
    uri = f"mongodb+srv://{user}:{password}@{cluster}/?retryWrites=true&w=majority"
    print("MongoDB URI:", uri)
    return uri
def get_schema(collections):
    artists_schema = StructType([
        StructField("_id",StringType(),True),
        StructField("external_urls",
                    StructType(
                [
                    StructField("spotify",StringType(),True)
                ])
        ),
        StructField("followers",
                    StructType(
                [
                    StructField("href",StringType(),True),
                    StructField("total",IntegerType(),True)
                ])
        ),
        StructField("genres",ArrayType(StringType()),True),
        StructField("id",StringType(),True),
        StructField("images",ArrayType(StructType([
            StructField("height",IntegerType(),True),
            StructField("url",StringType(),True),
            StructField("width",IntegerType(),True)
        ]))),
        StructField("name",StringType(),True),
        StructField("popularity",IntegerType(),True),
        StructField("type",StringType(),True),
        StructField("uri",StringType(),True),
       ])
    album_schema = StructType([
        StructField("_id", StringType(), True),
        StructField("album_type", StringType(), True),
        StructField("artist_id", StringType(), True),
        StructField(
            "copyrights",
            ArrayType(
                StructType([
                    StructField("text", StringType(), True),
                    StructField("type", StringType(), True)
                ])
            )
        ),
        StructField(
            "external_ids",
            StructType([
                StructField("amgid", StringType(), True),
                StructField("upc", StringType(), True)
            ])
        ),
        StructField(
            "external_urls",
            StructType([
                StructField("spotify", StringType(), True)
            ])
        ),
        StructField(
            "genres",
            ArrayType(StringType(), True)
        ),
        StructField("href", StringType(), True),
        StructField("id", StringType(), True),
        StructField(
            "images",
            ArrayType(
                StructType([
                    StructField("height", IntegerType(), True),
                    StructField("url", StringType(), True),
                    StructField("width", IntegerType(), True)
                ])
            )
        ),
        StructField("label", StringType(), True),
        StructField("name", StringType(), True),
        StructField("popularity", IntegerType(), True),
        StructField("release_date", StringType(), True),
        StructField("release_date_precision", StringType(), True),
        StructField("total_tracks", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("uri", StringType(), True),
    ])
    track_schema = StructType([
        StructField("_id", StringType(), True),
        StructField("artist_id", StringType(), True),
        StructField("album_id", StringType(), True),
        StructField("disc_number", IntegerType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("explicit", StringType(), True),
        StructField(
            "external_ids",
            StructType([
                StructField("isrc", StringType(), True)
            ])
        ),
        StructField(
            "external_urls",
            StructType([
                StructField("spotify", StringType(), True)
            ])
        ),
        StructField("href", StringType(), True),
        StructField("id", StringType(), True),
        StructField("is_local", BooleanType(), True),
        StructField("name", StringType(), True),
        StructField("popularity", IntegerType(), True),
        StructField("preview_url", StringType(), True),
        StructField("track_number", IntegerType(), True),
        StructField("type", StringType(), True),
        StructField("uri", StringType(), True)
    ])
    track_features_schema = StructType([
        StructField("_id", StringType(), True),
        StructField("track_id", StringType(), True),
        StructField("artists", StringType(), True),
        StructField("album_name", StringType(), True),
        StructField("track_name", StringType(), True),
        StructField("acousticness", DoubleType(), True),
        StructField("danceability", DoubleType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("energy", DoubleType(), True),
        StructField("explicit", StringType(), True),
        StructField("instrumentalness", DoubleType(), True),
        StructField("key", IntegerType(), True),
        StructField("liveness", DoubleType(), True),
        StructField("loudness", DoubleType(), True),
        StructField("mode", IntegerType(), True),
        StructField("popularity", IntegerType(), True),
        StructField("speechiness", DoubleType(), True),
        StructField("valence", DoubleType(), True),
        StructField("tempo", DoubleType(), True),
        StructField("time_signature", IntegerType(), True),
        StructField("track_genre", StringType(), True),
    ])
    if 'artists_data' in collections:
        return artists_schema
    elif 'albums_data' in collections:
        return album_schema
    elif 'feature_music' in collections:
        return track_features_schema
    else:
        return track_schema

def main() :
    uri = auth_mongoDB()
    database = os.getenv("MONGODB_DATABASE")
    print(database)
    client = MongoClient(uri)
    db = client[database]
    collections = db.list_collection_names()
    conf = (
    SparkConf()
    .setAppName("MongoDB_to_HDFS")
    .set("spark.mongodb.read.connection.uri", uri)
    .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1")
    .set("spark.driver.bindAddress", "127.0.0.1")
    .set("spark.driver.host", "127.0.0.1")
    .setMaster("local[*]")
)
    print(f"Database: {database}")
    for key,col in enumerate(collections) : 
        print(f"Collection {key}:{col}")

    with SparkIO(conf=conf) as spark :
        for collection in collections:
            try:
                df = spark.read.format("mongodb") \
                    .schema(get_schema(collection)) \
                    .option("database", database) \
                    .option("collection", collection) \
                    .load()
                count = df.count()
                print(f"Collection: {collection}, Row count: {count}")
                if count > 0:
                    cols = [c for c in df.columns if c != "_id"]
                    hdfs_path = f"hdfs://namenode:8020/bronze_layer/{collection}.parquet"
                    df.select(cols).write.mode("overwrite").parquet(hdfs_path)
                    print(f"Finished writing {collection} to HDFS at {hdfs_path}")
                else:
                    print(f"Skipped {collection} - No data found")
            except Exception as e:
                print(f"Error processing collection {collection}: {str(e)}")

        
if __name__ == "__main__":
    main()

