from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro, to_avro

import shutil
import os

"""
simple example of reading from Kafka
in avro format
streams. window of 60 seconds,
with sliding window of 30 seconds
After 60 seconds, cache is flushed

RUN: ../../kafka/python producer_words.py 3
"""

FIRST=True

def _init_dir(*args):
    for i in args:
        if FIRST and os.path.isdir(i):
            shutil.rmtree(i)
        if not os.path.isdir(i):
            os.mkdir(i)

def _make_spark_context():
    scala_version = '2.12'
    spark_version = '3.3.1'
    packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1',
    f'org.apache.spark:spark-avro_{scala_version}:{spark_version}',
    ]
    spark = SparkSession.builder \
             .master("local") \
             .appName("read from kafka")\
              .config("spark.jars.packages", ",".join(packages))\
             .config( "spark.jars", f"{os.getcwd()}/sqlite-jdbc-3.34.0.jar") \
            .getOrCreate()
    return spark

def foreach_batch_function(df, epoch_id):
    driver = "org.sqlite.JDBC"
    path = 'example.db'
    url = 'jdbc:sqlite:' + path
    table_name = 'words'
    df.write.format('jdbc') \
            .option('url', url) \
            .mode("append")\
            .option('driver', driver) \
            .option('dbtable', table_name) \
            .save()

def main(out_dir, checkpoint_dir):
    _init_dir(
            checkpoint_dir)
    json_format_schema = open("../../kafka_/words.avsc", "r").read()
    spark = _make_spark_context()

    words = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "words") \
      .load() \
      .select(from_avro("value", json_format_schema).alias('json')) \
      .withColumn("word", F.col("json.word"))\
      .withColumn("timestamp", F.col("json.timestamp")) 
    words.createOrReplaceTempView("my_table")
    spark.sql("select word, timestamp from my_table") \
        .writeStream\
        .option("checkpointLocation", checkpoint_dir)\
        .foreachBatch( foreach_batch_function) \
        .start()\
        .awaitTermination()

if __name__ == '__main__':
    main(out_dir = 'streaming_dir', checkpoint_dir = 'checkpoint_dir')
