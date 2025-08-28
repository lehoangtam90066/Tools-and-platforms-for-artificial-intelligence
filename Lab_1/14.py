from pyspark.sql import SparkSession
import findspark
findspark.init()
spark = SparkSession.builder\
    .master('local[1]')\
    .appName('bai_thuc_hanh_1.com')\
    .getOrCreate()
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
data1 = [1, 22, 34, 45, 5, 6, 7, 8, 90, 100, 101, 124]
rdd = spark.sparkContext.parallelize(data)
rdd1 = spark.sparkContext.parallelize(data1)
rdd2 = rdd.subtract(rdd1)  
print(rdd2.collect())
