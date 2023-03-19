# from test.dbLoad import ConnectDatabase
import pyspark
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from os import *

spark = SparkSession.builder.appName("Kafka_Spark").getOrCreate()  # Spark 2.x

spark.sparkContext.setLogLevel("ERROR")

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ournewtopic").load()

df1 = df.selectExpr("CAST(value AS STRING)")

df2 = df1.withColumn("value_split", split(col("value"), ",")) \
    .withColumn("datevalue", to_timestamp(col("value_split").getItem(0), 'yyyy/MM/dd HH:mm:ss'))\
    .withColumn("ipaddress", col("value_split").getItem(1)) \
    .withColumn("Host", col("value_split").getItem(2)) \
    .withColumn("ReqURL", col("value_split").getItem(3)) \
    .withColumn("ResponseCode", col("value_split").getItem(4).cast("Integer")) \
    .drop("value_split","value")

df2.printSchema()


df2.writeStream.format("console").option("truncate", "false").outputMode("append").start()

# df2.writeStream.outputMode("update").foreach(ConnectDatabase()).start().awaitTermination()