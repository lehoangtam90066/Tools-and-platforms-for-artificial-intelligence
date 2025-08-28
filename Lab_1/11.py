from pyspark.sql import SparkSession
import findspark
findspark.init()
spark = SparkSession.builder\
    .master('local[1]')\
    .appName('bai_thuc_hanh_1.com')\
    .getOrCreate()
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
rdd = spark.sparkContext.parallelize(data, 2)  
def checkEvenNumber(partition_index, iter):
    temp = []
    for i in iter:
        if i % 2 == 0: 
            temp.append((partition_index, i, '--->', i * 3))  
    return temp
rdd2 = rdd.mapPartitionsWithIndex(checkEvenNumber)  
print(rdd2.collect())
