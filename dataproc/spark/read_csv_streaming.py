from pyspark.sql import SparkSession
from pyspark import SparkContext
from  pyspark.sql import SQLContext
from pyspark.sql.types import *

import os
import csv
import shutil
"""
simple example of streaming csv.
and then reading that file
"""

def _init_dir(
        data_dir, 
        out_dir,
        checkpoint_dir):
    for i in [data_dir, out_dir, checkpoint_dir]:
        shutil.rmtree(i, ignore_errors=True)
        os.mkdir(i)

def _make_spark_context():
    spark = SparkSession.builder \
             .master("local") \
             .appName("stream from csv")\
             .config("spark.some.config.option", "some-value") \
            .getOrCreate()
    return spark

def _get_schema():
    return StructType([StructField('creation_time',TimestampType(),True),
        StructField('key',LongType(),True),
        StructField('x',LongType(),True),
                        ])

def main(
        data_dir ='data', 
        out_dir = 'data_out', 
        checkpoint_dir = 'checkpoint_dir'):
    _init_dir(
            data_dir = data_dir, 
            out_dir = out_dir,
            checkpoint_dir = checkpoint_dir,
            )
    spark = _make_spark_context()
    spark.conf.set("spark.sql.shuffle.partitions", 5)

    streaming = spark.readStream.schema(_get_schema())\
            .option("maxFilesPerTrigger", 1)\
            .csv(data_dir)
    streaming.registerTempTable("my_table")
    spark.sql("select * from my_table")\
            .writeStream\
            .trigger(processingTime='60 seconds')\
            .format("console")\
            .option("checkpointLocation", checkpoint_dir)\
            .outputMode("append")\
            .option("path", out_dir)\
            .start()\
            .awaitTermination()

if __name__ == '__main__':
    main()
