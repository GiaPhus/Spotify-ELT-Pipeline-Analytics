from pyspark.sql import SparkSession

def warehouse_track_recommendation(spark: SparkSession):
    metadata_df = spark.read.parquet("hdfs://namenode:8020/gold_layer/gold_feature_matrix.parquet")
    metadata_df.write.mode("overwrite").parquet("hdfs://namenode:8020/warehouse_layer/warehouse_track_recommendation.parquet")
    print(" warehouse_spotify_dashboard written.")

def warehouse_spotify_dashboard(spark: SparkSession):
    metadata_df = spark.read.parquet("hdfs://namenode:8020/gold_layer/gold_track_metadata.parquet")
    feature_df = spark.read.parquet("hdfs://namenode:8020/gold_layer/gold_feature_matrix.parquet")
    
    feature_df=feature_df.drop("duration_ms")
    
    df = metadata_df.join(feature_df, on="track_id", how="left")
    
    df.write.mode("overwrite").parquet("hdfs://namenode:8020/warehouse_layer/warehouse_track_dashboard.parquet")
    print("Recommendation data written to warehouse.")
