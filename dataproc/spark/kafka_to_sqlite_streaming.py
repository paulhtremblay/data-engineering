from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import functions as F
#from pyspark.sql import Window


import shutil
import os

"""
2 dataframes
1. aggregation. Written to db 
2. raw. Written to db

RUN: ../../kafka/python producer_words.py 3
"""

def _init_dir(*args):
    for i in args:
        if not os.path.isdir(i):
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
             .appName("stream from kafka to sqlite")\
              .config("spark.jars.packages", ",".join(packages))\
            .config(
                "spark.jars",
                f"{os.getcwd()}/sqlite-jdbc-3.34.0.jar") \
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



def main(checkpoint_dir):
    _init_dir(checkpoint_dir)
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
    words.createOrReplaceTempView("my_table2")
    spark.sql("select word, timestamp from my_table2") \
            .writeStream \
            .option("checkpointLocation", checkpoint_dir)\
            .foreachBatch(
                    foreach_batch_function) \
                            .start()\
                            .awaitTermination()

if __name__ == '__main__':
    main(
            checkpoint_dir = 'checkpoint_dir_words',
            )
