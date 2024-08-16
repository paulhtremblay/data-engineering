from pyspark.sql import SparkSession
from pyspark import SparkContext
from  pyspark.sql import SQLContext
from pyspark.sql.types import *

import os
import csv
import shutil
"""
simple example of reading from a file folder.
and then reading that file
"""

def _init_dir(data_dir, out_dir):
    shutil.rmtree(data_dir, ignore_errors=True)
    shutil.rmtree(out_dir, ignore_errors = True)
    os.mkdir(data_dir)
    os.mkdir(out_dir)

def _make_spark_context():
    spark = SparkSession.builder \
             .master("local") \
             .appName("read from csv")\
             .config("spark.some.config.option", "some-value") \
            .getOrCreate()
    return spark

def main(data_dir ='data', out_dir = 'data_out'):
    _init_dir(
            data_dir = data_dir, 
            out_dir = out_dir
            )
    spark = _make_spark_context()
    in_dir = 'data'
    out_dir = 'out_data'
    df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
    df.write.mode("overwrite").format("csv").save(out_dir)
    spark.read.csv(in_dir, schema=df.schema, nullValue="Hyukjin Kwon").show()
    print(df.schema)

if __name__ == '__main__':
    main()
