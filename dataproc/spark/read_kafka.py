from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *
from pyspark.sql import functions as F


"""
simple example of reading from Kafka
"""


def _make_spark_context():
    scala_version = '2.12'
    spark_version = '3.1.2'
    packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
    ]
    spark = SparkSession.builder \
             .master("local") \
             .appName("read from kafka")\
              .config("spark.jars.packages", ",".join(packages))\
            .getOrCreate()
    return spark

def main():
    spark = _make_spark_context()
    df = spark \
      .read \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "numtest") \
      .load()
    df2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    df3 = df2.select('value')
    df3 = df3.withColumn("col1", F.from_json("value", " struct<number:long>"))
    df3 = df3.withColumn("col1", F.col("col1.number"))
    df3.createOrReplaceTempView("my_table")
    spark.sql("select col1, count(*)  from my_table group by col1 order by col1").show(truncate = False)

if __name__ == '__main__':
    main()
