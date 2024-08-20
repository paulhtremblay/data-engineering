from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import functions as F

import sqlite3

import shutil
import os

def _init():

    con = sqlite3.connect('example.db')
    cur = con.cursor()
    cur.execute(
        '''CREATE TABLE if not exists example1
           (age integer, name text)''')
    cur.execute("INSERT INTO example1 VALUES (59,'Henry')")
    con.commit()
    con.close()

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
    #df = spark.createDataFrame([{"age": 100, "name": "Hyukjin Kwon"}])
    driver = "org.sqlite.JDBC"
    path = 'example.db'
    url = 'jdbc:sqlite:' + path
    table_name = 'example1'
    df = spark.read.format("jdbc").option("url", url).option('dbtable', table_name) \
            .option("driver", driver).load()
    df.show()


if __name__ == '__main__':
    main()
