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

def main(out_dir, checkpoint_dir):
    jsonFormatSchema = open("../../kafka_/words.avsc", "r").read()
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
      .load() 

    output = words\
          .select(from_avro("value", jsonFormatSchema))
    output.writeStream\
            .trigger(processingTime='10 seconds')\
            .format("console")\
            .outputMode("update")\
            .start()\
            .awaitTermination()

    #df.write.mode("append").format("console").save()

if __name__ == '__main__':
    main(out_dir = 'streaming_dir', checkpoint_dir = 'checkpoint_dir')
