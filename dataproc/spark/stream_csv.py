from pyspark.sql import SparkSession
from pyspark import SparkContext
from  pyspark.sql import SQLContext
from pyspark.sql.types import *

import datetime
import os
import shutil
import csv
import random
import uuid

"""
simple example of streaming from a file folder.
User make_data1.py to generate data after
running this script
"""

def _init_dir(data_dir, checkpoint_dir, out_dir):
    shutil.rmtree(data_dir, ignore_errors=True)
    shutil.rmtree(checkpoint_dir, ignore_errors = True)
    shutil.rmtree(out_dir, ignore_errors = True)
    os.mkdir(data_dir)
    os.mkdir(checkpoint_dir)
    os.mkdir(out_dir)

def _make_spark_context():
    spark = SparkSession.builder \
             .master("local") \
             .appName("stream to csv")\
             .config("spark.some.config.option", "some-value") \
            .getOrCreate()
    return spark

def _get_schema():
    return StructType([StructField('creation_time',TimestampType(),True),
        StructField('key',LongType(),True),
        StructField('x',LongType(),True),
                        ])

def _get_streaming(spark, schema, data_dir):
    streaming = spark.readStream.schema(schema).option("maxFilesPerTrigger", 1)\
        .csv(data_dir)
    spark.conf.set("spark.sql.shuffle.partitions", 5)
    return streaming

def _create_table_and_job(spark, 
        streaming, 
        out_dir,
        checkpoint_dir
        ):
    streaming.createOrReplaceTempView("my_table")
    query = spark.sql("select * from my_table").writeStream.trigger(processingTime='60 seconds')\
        .format("parquet")\
        .option("checkpointLocation", checkpoint_dir).outputMode("append")\
        .option("path", out_dir).start()
    query.awaitTermination()


def main():
    _init_dir(
            data_dir = 'data', 
            checkpoint_dir = 'checkpoint_dir',
            out_dir = 'streaming_out'
            )
    spark = _make_spark_context()
    schema = _get_schema()
    streaming = _get_streaming(
            spark = spark,
            schema = schema,
            data_dir = 'data',
            )
    _create_table_and_job(
            spark = spark,
            streaming = streaming,
            out_dir = 'streaming_out',
            checkpoint_dir = 'checkpoint_dir'
            )

if __name__ == '__main__':
    main()
