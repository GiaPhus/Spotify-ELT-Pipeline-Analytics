from airflow.decorators import dag, task
from datetime import datetime, timedelta
import os
from IOManager.MongoDBio import MongoIO
from crawl import main, crawl_artists_streaming
import time

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id="crawl_and_ingest_data_into_mongoDB",
    default_args=default_args,
    schedule_interval=timedelta(minutes=2),
    catchup=False
)
def spotify_crawl_dag():
    
    @task(task_id = "crawl_by_artists_file")
    def crawl_artist():
        data_dir = "/opt/airflow/data"
        art_path = os.path.join(data_dir, 'artists_stream.txt')
        log_path = os.path.join(data_dir, 'logs.txt')

        if not os.path.exists(art_path):
            crawl_artists_streaming.artists_crawler(art_path)

        with open(art_path, 'r') as read_file:
            data = read_file.read().splitlines()

        if not os.path.exists(log_path):
            with open(log_path, 'w') as file:
                start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                file.write(f"0 {start_time}\n")

        return data

    @task()
    def ingest_mongoDB(artists_stream, batch_size: int = 20):
        if not artists_stream:
            raise ValueError("No artists to crawl!")

        data_dir = "/opt/airflow/data"
        log_path = os.path.join(data_dir, 'logs.txt')

        with open(log_path, 'a+') as file:
            file.seek(0)
            log_data = file.readlines()[-1].strip().split()
            start_index = int(log_data[0]) if log_data else 0

            if start_index >= len(artists_stream):
                return

            end_index = start_index + batch_size

            with MongoIO() as client:
                try:
                    main.spotify_crawler(client, artists_stream, start_index, end_index)
                except Exception as e:
                    raise e

            track_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            file.write(f"{end_index} {track_time}\n")
        
        time.sleep(120)

    artists_list = crawl_artist()
    ingest_mongoDB(artists_list)

spotify_crawl_dag()
