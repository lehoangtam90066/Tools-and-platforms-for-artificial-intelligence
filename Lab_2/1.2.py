import random

from pyspark.sql import SparkSession
import findspark

findspark.init()

spark = SparkSession.builder\
    .master('local[1]')\
    .appName('bai_thuc_hanh_2.com')\
    .getOrCreate()

numberRDD = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2)
print(numberRDD.first())