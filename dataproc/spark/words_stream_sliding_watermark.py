from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
#from pyspark.sql import Window


import shutil
import os

"""
simple example of reading from Kafka
streams. window of 60 seconds,
with sliding window of 30 seconds
After 60 seconds, cache is flushed

RUN: ../../kafka/python producer_words.py 3
"""

def _make_spark_context():
    scala_version = '2.12'
    spark_version = '3.1.2'
    packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
    ]
    spark = SparkSession.builder \
             .master("local") \
             .appName("read from kafka")\
              .config("spark.jars.packages", ",".join(packages))\
            .getOrCreate()
    return spark

def main(out_dir, checkpoint_dir):
    spark = _make_spark_context()
    schema = StructType([
        StructField("word", StringType()),
        StructField("timestamp", TimestampType()),
        ])

    words = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "words") \
      .load() \
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
      .select(F.from_json('value', schema).alias("json")) \
            .withColumn("word", F.col("json.word"))\
            .withColumn("timestamp", F.col("json.timestamp")) 
    windowedCounts = words.withWatermark("timestamp", "240 seconds") \
            .groupBy(
            F.window("timestamp", "120 seconds", "60 seconds"),
            words.word
            ).count().writeStream\
            .trigger(processingTime='120 seconds')\
            .format("console")\
            .outputMode("update")\
            .start()\
            .awaitTermination()


if __name__ == '__main__':
    main(out_dir = 'streaming_dir', checkpoint_dir = 'checkpoint_dir')
