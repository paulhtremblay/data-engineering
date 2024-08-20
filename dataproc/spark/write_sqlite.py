from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import functions as F

import sqlite3

import shutil
import os

def _init():
    return

    con = sqlite3.connect('example.db')
    cur = con.cursor()

def _make_spark_context():
    spark = SparkSession.builder \
        .master("local") \
        .appName("read from sqllite")\
        .config(
            "spark.jars",
            f"{os.getcwd()}/sqlite-jdbc-3.34.0.jar") \
        .getOrCreate()
    return spark

def main():
    _init()
    spark = _make_spark_context()
    df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
    driver = "org.sqlite.JDBC"
    path = 'example.db'
    url = 'jdbc:sqlite:' + path
    table_name = 'example1'
    df.write.format('jdbc').option('url', url).mode('append') \
            .option('driver', driver) \
            .option('dbtable', table_name) \
            .save()


if __name__ == '__main__':
    main()
