from pyspark.sql import SparkSession
import findspark
findspark.init()
spark = SparkSession.builder\
    .master('local[1]')\
    .appName('bai_thuc_hanh_1.com')\
    .getOrCreate()
data = [
    [1, 2, 3, 4, 5],
    [9, 8, 7]
]
rdd = spark.sparkContext.parallelize(data)
rdd2 = rdd.flatMap(lambda x: x * 2)  
print(rdd2.collect())
