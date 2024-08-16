from pyspark.sql import SparkSession
from pyspark import SparkContext
from  pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions as F


import os
import csv
import shutil
import json
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
    spark = _make_spark_context()
    df = spark.createDataFrame([{"key": 1, "data": '{"name": "Henry", "age": 59}'}])
    df = df.withColumn("col1", F.from_json("data", " struct<age:long>"))
    df = df.withColumn("col1", F.col("col1.age"))
    df.show()
    df2 = spark.createDataFrame([
        {"key": 1, 
            "data": '{"person": {"name": "Henry", "age": 59}}'}])
    df2 = df2.withColumn("col1", F.from_json("data", "person struct<age:long>"))
    df2 = df2.withColumn("col1", F.col("col1.person.age"))
    df2.show()
    df3 = spark.createDataFrame([
        {"key": 1, 
            "data": '{"person": {"name": "Henry", "age": 59}}'}])
    df3.createOrReplaceTempView("my_table")
    spark.sql("""
    SELECT from_json(data, 'person struct<age:long>').person.age as age
    FROM my_table
""").show()
    #safe cast: if column doesn't exist, null is returned
    df4 = spark.createDataFrame([
        {"key": 1, 
            "data": '{"person": {"name": "Henry", "age": 59}}'}])
    df4 = df4.withColumn("col1", F.from_json("data", "personn struct<age:long>"))
    df4 = df4.withColumn("col1", F.col("col1.personn.age"))
    df4.show()

if __name__ == '__main__':
    main()
