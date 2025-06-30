from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from silver_layer import (
    artists_silver_layer, feature_music_silver_layer,
    albums_silver_layer, tracks_silver_layer
)
from warehouse_layer import (warehouse_spotify_dashboard,warehouse_track_recommendation)
from gold_layer import (
    gold_track_metadata, gold_feature_matrix
)
from IOManager.SparkIO import SparkIO
from pyspark import SparkConf
from airflow.utils.task_group import TaskGroup

spark_conf = SparkConf() \
    .setAppName("SilverLayerProcessing") \
    .set("spark.driver.bindAddress", "127.0.0.1") \
    .set("spark.driver.host", "127.0.0.1") \
    .setMaster("local[*]")
    
def run_artists():
    with SparkIO(spark_conf) as spark:
        artists_silver_layer(spark)

def run_feature_music():
    with SparkIO(spark_conf) as spark:
        feature_music_silver_layer(spark)

def run_albums():
    with SparkIO(spark_conf) as spark:
        albums_silver_layer(spark)

def run_tracks():
    with SparkIO(spark_conf) as spark:
        tracks_silver_layer(spark)
    
def run_gold_metadata():
    with SparkIO(spark_conf) as spark:
        gold_track_metadata(spark)

def run_gold_feature_matrix():
    with SparkIO(spark_conf) as spark:
        gold_feature_matrix(spark)

def run_warehouse_dashboard():
    with SparkIO(spark_conf) as spark:
        warehouse_spotify_dashboard(spark)

def run_warehouse_track_recommendation():
    with SparkIO(spark_conf) as spark :
        warehouse_track_recommendation(spark)
default_args = {
    'owner': 'Phu',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    dag_id="Spotify_DAG",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["etl", "silver","gold"]
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    with TaskGroup("bronze_layer", tooltip="Ingest data from Mongo to HDFS") as bronze_layer:
        bronze_task = SparkSubmitOperator(
            task_id='bronze_mongo_to_hdfs',
            application='/opt/airflow/dags/bronze_layer.py',
            packages='org.mongodb.spark:mongo-spark-connector_2.12:10.1.1',
            verbose=True,
            conf={
            # "spark.master": "local[*]",
            "spark.executor.memory": "1g"
        },
            conn_id='spark_default_conn'
        )
        
    with TaskGroup("silver_layer") as silver_layer:
        silver_artists = PythonOperator(task_id="silver_artists", python_callable=run_artists)
        silver_albums = PythonOperator(task_id="silver_albums", python_callable=run_albums)
        silver_tracks = PythonOperator(task_id="silver_tracks", python_callable=run_tracks)
        silver_feature_music = PythonOperator(task_id="silver_feature_music", python_callable=run_feature_music)
        silver_done = DummyOperator(task_id="silver_done")

        [silver_artists, silver_albums, silver_tracks] >> silver_feature_music >> silver_done


    with TaskGroup("gold_layer") as gold_layer:
        gold_metadata = PythonOperator(task_id="gold_track_metadata", python_callable=run_gold_metadata)
        gold_feature = PythonOperator(task_id="gold_feature_matrix", python_callable=run_gold_feature_matrix)
        gold_done = DummyOperator(task_id="gold_done")
        [gold_metadata, gold_feature] >> gold_done



    with TaskGroup("warehouse_layer", tooltip="Final model for dashboard and ML") as warehouse_layer:
        warehouse_dashboard = PythonOperator(task_id="warehouse_dashboard", python_callable=run_warehouse_dashboard)
        warehouse_recommendation = PythonOperator(task_id="warehouse_recommendation", python_callable=run_warehouse_track_recommendation)

        gold_metadata >> warehouse_dashboard
        [gold_metadata, gold_feature] >> warehouse_recommendation

    start >> bronze_task
    bronze_task >> [silver_artists, silver_albums, silver_tracks]
    [silver_artists, silver_albums, silver_tracks] >> silver_feature_music

    [silver_artists, silver_albums, silver_tracks] >> gold_metadata
    [silver_artists, silver_feature_music] >> gold_feature

    gold_metadata >> warehouse_dashboard
    [gold_metadata, gold_feature] >> warehouse_recommendation

    warehouse_recommendation >> end
        
