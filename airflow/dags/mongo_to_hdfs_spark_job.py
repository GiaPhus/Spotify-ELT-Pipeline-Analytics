from IOManager.SparkIO import SparkIO
from pyspark.sql import SparkSession
import os
import urllib
from dotenv import load_dotenv
from pyspark import SparkConf
from IOManager.MongoDBio import auth_mongoDB

load_dotenv()
def main() :
    uri = auth_mongoDB()
    database = os.getenv("MONGODB_DATABASE")
    collection = os.getenv("MONGODB_COLLECTION")
    hdfs_path = f"hdfs://namenode:8020/bronze_layer/{collection}.parquet"
    conf = (
    SparkConf()
    .setAppName("MongoDB_to_HDFS")
    .set("spark.mongodb.read.connection.uri", uri)
    .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1")
    .setMaster("spark://spark-master1:7077")
)
    with SparkIO(conf=conf) as spark :
        df = spark.read.format("mongodb") \
        .option("database", database) \
        .option("collection", collection) \
        .load()
        cols = [c for c in df.columns if c != "_id"]
        df.select(cols).write.mode("overwrite").parquet(hdfs_path)
        print(f"Finished writing {collection} to HDFS at {hdfs_path}")
        
        
        
        
if __name__ == "__main__":
    main()


