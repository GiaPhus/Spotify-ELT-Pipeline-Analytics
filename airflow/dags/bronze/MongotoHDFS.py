from datetime import datetime, timedelta
import os
from pyspark.sql import SparkSession
from airflow import DAG
from pyspark.sql.functions import col
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import sys
# sys.path.append(os.path.abspath("/home/phu/Pictures/Spotify-ELT-Pipeline-Analytics/airflow/plugins"))
from airflow.operators.dummy import DummyOperator
from IOManager.MongoDBio import MongoIO
from IOManager.SparkIO import SparkIO
from pyspark import SparkConf



def auth_mongoDB():
    user = os.getenv("MONGODB_USER")
    password = os.getenv("MONGODB_PASSWORD")
    cluster = os.getenv("MONGODB_SRV").split("//")[-1]
    uri = f"mongodb+srv://{user}:{password}@{cluster}/?retryWrites=true&w=majority"
    print("MongoDB URI:", uri)
    return uri

def getMongotoHDFS(spark:SparkSession,uri:str, database_name:str, table_name:str):
    hdfs_uri = f"hdfs://namenode:8020/bronze_layer/{table_name}.parquet"
    spark_data = (spark.read.format("mongodb")
              .option("uri", uri)
              .option('database', database_name)
              .option('collection', table_name)
              .load()
              .select([col for col in spark.read.format("mongodb").option("uri", uri).option('database', database_name).option('collection', table_name).load().columns if col != '_id'])
              )
    spark_data.write.parquet(hdfs_uri, mode="overwrite")
    print(f"Bronze: Successfully writing {table_name}.parquet")

def IngestHadoop(client, uri, spark: SparkSession,collection):
    """Extract data From MongoDB and Load to HDFS"""

    database_name = os.getenv("MONGODB_DATABASE")
    getMongotoHDFS.submit(spark, uri, database_name, collection)
    print("Done")


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
    start_dag = DummyOperator(task_id= "start_dag",dag=dag)
    end_dag = DummyOperator(task_id="end_dag",dag=dag)
    

    with MongoIO() as client:
        db = client[os.getenv("MONGODB_DATABASE")]
        for collection_name in db.list_collection_names():
            task = PythonOperator(
                task_id=f"ingest_{collection_name}",
                python_callable=lambda name=collection_name: process_collection(name),
            )

            start_dag >> task >> end_dag
