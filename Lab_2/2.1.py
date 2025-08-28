import random

from pyspark.sql import SparkSession
import findspark

findspark.init()

spark = SparkSession.builder\
    .master('local[1]')\
    .appName('bai_thuc_hanh_2.com')\
    .getOrCreate()

data = ((1, "Truong"), (2, "Dai"), (3, "Hoc"), (4, "Van"), (5, "Lang"))

rdd = spark.sparkContext.parallelize(data, 2)
print(rdd.collect())