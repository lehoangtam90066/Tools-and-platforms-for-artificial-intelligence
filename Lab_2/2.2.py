import random

from pyspark.sql import SparkSession
import findspark

findspark.init()

spark = SparkSession.builder\
    .master('local[1]')\
    .appName('bai_thuc_hanh_2.com')\
    .getOrCreate()

data = ((1, "Truong"), (2, "Dai"), (2, "Hoc"), (3, "Van"), (3, "Lang"))

rdd = spark.sparkContext.parallelize(data, 2)
rdd2 = rdd.groupByKey().mapValues(list)
print(rdd2.collect())