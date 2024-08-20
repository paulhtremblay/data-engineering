from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
#from pyspark.sql import Window


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
    schema = StructType([
        StructField("userId", StringType()),
        StructField("timestamp", TimestampType()),
        ])

    events = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "events") \
      .load() \
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
      .select(F.from_json('value', schema).alias("json")) \
            .withColumn("userId", F.col("json.userId"))\
            .withColumn("timestamp", F.col("json.timestamp")) 


if __name__ == '__main__':
    main(out_dir = 'streaming_dir', checkpoint_dir = 'checkpoint_dir')
