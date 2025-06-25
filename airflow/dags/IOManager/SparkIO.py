from pyspark.sql import SparkSession
from pyspark import SparkConf
from contextlib import contextmanager
from typing import Generator

@contextmanager
def SparkIO(conf: SparkConf = SparkConf()) -> Generator[SparkSession, None, None]:
    app_name = conf.get("spark.app.name")
    master = conf.get("spark.master")
    print(f'Create SparkSession app {app_name} with {master} mode')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    try:
        yield spark
    except Exception as e:
        print(f"Exception occurred in SparkIO context: {e}")
        raise
    finally:
        print(f'Stop SparkSession app {app_name}')
