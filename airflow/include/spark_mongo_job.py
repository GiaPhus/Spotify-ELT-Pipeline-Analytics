from pyspark.sql import SparkSession
import os
import urllib
from dotenv import load_dotenv

load_dotenv()

def auth_connection():
    user = urllib.parse.quote_plus(os.getenv("MONGODB_USER", ""))
    password = urllib.parse.quote_plus(os.getenv("MONGODB_PASSWORD", ""))
    cluster = os.getenv("MONGODB_SRV", "").replace("mongodb+srv://", "").strip("/")
    uri = f"mongodb+srv://{user}:{password}@{cluster}/?retryWrites=true&w=majority"
    print("MongoDB URI:", uri)
    return uri

def main():
    uri = auth_connection()
    collection = "artists_data"

    spark = SparkSession.builder \
        .appName("MongoDb_test") \
        .master("spark://spark-master1:7077") \
        .config("spark.mongodb.read.connection.uri", uri) \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
        .getOrCreate()

    df = spark.read.format("mongodb") \
        .option("database", "spotify") \
        .option("collection", collection) \
        .load()

    df.show(5)
    spark.stop()

if __name__ == "__main__":
    main()
