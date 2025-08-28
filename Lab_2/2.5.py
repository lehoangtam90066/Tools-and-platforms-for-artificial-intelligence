import random

from pyspark.sql import SparkSession
import findspark

findspark.init()

spark = SparkSession.builder\
    .master('local[1]')\
    .appName('bai_thuc_hanh_2.com')\
    .getOrCreate()

data = [(1, 8.5), (2, 7.2), (3, 9.3)]
data1 = [(1, "A"), (2, "B"), (3, "C")]

rdd = spark.sparkContext.parallelize(data, 2)
rdd1 = spark.sparkContext.parallelize(data1, 2)

rdd2 = rdd.join(rdd1)
print(rdd2.collect())