from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import functions as F

import shutil
import os

"""
simple example of reading from Kafka
streams. Groups. Reads Kafka from start,
so messages are not lost
"""

def _init_dir(
        out_dir,
        checkpoint_dir):
    for i in [out_dir, checkpoint_dir]:
        shutil.rmtree(i, ignore_errors=True)
        os.mkdir(i)


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
    _init_dir(
            out_dir = out_dir,
            checkpoint_dir = checkpoint_dir,
            )
    df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "numtest") \
      .load() \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .withColumn("col1", F.from_json("value", " struct<number:long>")) \
        .withColumn("col1", F.col("col1.number"))\
        .createOrReplaceTempView("my_table")
    spark.sql("select col1, count(*)  from my_table group by col1")\
            .writeStream\
            .trigger(processingTime='5 seconds')\
            .format("console")\
            .option("checkpointLocation", checkpoint_dir)\
            .outputMode("update")\
            .option("path", out_dir)\
            .start()\
            .awaitTermination()

if __name__ == '__main__':
    main(out_dir = 'streaming_dir', checkpoint_dir = 'checkpoint_dir')