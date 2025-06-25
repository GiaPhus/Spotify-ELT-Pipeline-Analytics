from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
# from airflow.dags.bronze_layer import run_bronze_layer
from pipeline.silver_layer import (
    artists_silver_layer, feature_music_silver_layer,
    albums_silver_layer, tracks_silver_layer
)
from pipeline.gold_layer import (
    gold_track_metadata, gold_feature_matrix, gold_track_search_index
)
from IOManager.SparkIO import SparkIO
from pyspark import SparkConf

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

def run_gold_search_index():
    with SparkIO(spark_conf) as spark:
        gold_track_search_index(spark)

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

    silver_artists = PythonOperator(
        task_id="silver_artists",
        python_callable=run_artists
    )

    silver_feature_music = PythonOperator(
        task_id="silver_feature_music",
        python_callable=run_feature_music
    )

    silver_albums = PythonOperator(
        task_id="silver_albums",
        python_callable=run_albums
    )

    silver_tracks = PythonOperator(
        task_id="silver_tracks",
        python_callable=run_tracks
    )
    gold_metadata = PythonOperator(task_id="gold_track_metadata", python_callable=run_gold_metadata)
    gold_feature = PythonOperator(task_id="gold_feature_matrix", python_callable=run_gold_feature_matrix)
    gold_search_index = PythonOperator(task_id="gold_track_search_index", python_callable=run_gold_search_index)

    start >> silver_artists >> silver_feature_music
    start >> silver_albums
    start >> silver_tracks
    [silver_artists, silver_albums, silver_tracks] >> gold_metadata
    [silver_artists, silver_feature_music] >> gold_feature
    [gold_metadata, gold_feature] >> gold_search_index >> end
