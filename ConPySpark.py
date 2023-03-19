from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *

import os

# spark_version = '2.4.4'
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12-2.6.0:{}'.format(spark_version)

# kafka_2.12-2.6.0
spark = SparkSession \
    .builder \
    .appName("producer-id1") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "new_topic") \
    .option("startingOffsets", "earliest") \
    .load()

query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .outputMode("update") \
    .option("checkpointLocation", "checkpoint-location1") \
    .start()

query.awaitTermination()
