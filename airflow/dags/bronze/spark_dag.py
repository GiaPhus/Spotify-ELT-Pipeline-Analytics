from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
import os
from IOManager.MongoDBio import MongoIO,auth_mongoDB
from IOManager.SparkIO import SparkIO
from pyspark.sql import SparkSession
from pyspark import SparkConf

default_args = {
    'owner': 'phu',
    'start_date': datetime(2025, 6, 2),
    'retries': 1,
}

with DAG(
    dag_id="Spark_DAG_HDFS",
    default_args=default_args,
    schedule_interval=None,
    tags=["test", "spark", "mongo"]
) as dag:
    start_task = DummyOperator(task_id="start_task")
    end_task = DummyOperator(task_id="end_task")
    spark_submit_task = SparkSubmitOperator(
        task_id='mongo_to_HDFS_DAG',
        application='/opt/airflow/dags/mongo_to_hdfs_spark_job.py',
        packages='org.mongodb.spark:mongo-spark-connector_2.12:10.1.1',
        verbose=True,
        conf={
            "spark.master": "local[*]",
            "spark.executor.memory": "1g"
        },
        conn_id='spark_default_conn'
    )

    start_task >> spark_submit_task >> end_task
