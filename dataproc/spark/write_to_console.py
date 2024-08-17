from pyspark.sql import SparkSession
from pyspark import SparkContext
from  pyspark.sql import SQLContext
from pyspark.sql.types import *

import os
import csv
import shutil
"""
create dataframe and write to console
"""

def _make_spark_context():
    spark = SparkSession.builder \
             .master("local") \
             .appName("read from csv")\
             .config("spark.some.config.option", "some-value") \
            .getOrCreate()
    return spark

def main(data_dir ='data', out_dir = 'data_out'):
    spark = _make_spark_context()
    df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
    df.write.mode("append").format("console").save()

if __name__ == '__main__':
    main()
