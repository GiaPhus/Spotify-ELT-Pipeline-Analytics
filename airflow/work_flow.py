from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
import sys
# sys.path.append(os.path.abspath("/home/phu/Pictures/Spotify-ELT-Pipeline-Analytics/airflow/plugins"))

from dags.bronze.MongotoHDFS import IngestHadoop
from plugins.MongoDBio import MongoIO
from plugins.SparkIO import SparkIO
from pyspark import SparkConf

def auth_mongoDB():
    user = os.getenv("MONGODB_USER")
    password = os.getenv("MONGODB_PASSWORD")
    cluster = os.getenv("MONGODB_SRV").split("//")[-1]
    return f"mongodb+srv://{user}:{password}@{cluster}/?retryWrites=true&w=majority"

def process_collection(collection_name):
    uri = auth_mongoDB()
    conf = (
        SparkConf()
        .setAppName(f"ETL-{collection_name}")
        .set("spark.executor.memory", "4g")
        .set("spark.mongodb.read.connection.uri", uri)
        .set("spark.mongodb.write.connection.uri", uri)
        .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.2.1")
        .setMaster("local[*]"))
    with SparkIO(conf) as spark:
        with MongoIO() as client:
            IngestHadoop(spark, uri, client, collection_name)

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}
    
with DAG(
    dag_id="bronze_etl_all_collections",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["bronze"],
) as dag:

    with MongoIO() as client:
        db = client[os.getenv("MONGODB_DATABASE")]
        for collection_name in db.list_collection_names():
            PythonOperator(
                task_id=f"ingest_{collection_name}",
                python_callable=lambda name=collection_name: process_collection(name),
            )
