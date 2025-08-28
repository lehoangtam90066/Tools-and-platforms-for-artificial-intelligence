import random

from pyspark.sql import SparkSession
import findspark

findspark.init()

spark = SparkSession.builder\
    .master('local[1]')\
    .appName('bai_thuc_hanh_2.com')\
    .getOrCreate()

data = [('Project', 1), ('Gutenberg', 1), ('Adventures', 1), ('Adventures', 1), ('Gutenberg', 1), ('Project', 1)]

rdd = spark.sparkContext.parallelize(data, 2)
rdd2 = rdd.reduceByKey(lambda a, b: a + b)

print(rdd2.collect())