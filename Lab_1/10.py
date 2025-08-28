from pyspark.sql import SparkSession
import findspark
findspark.init()
spark = SparkSession.builder\
    .master('local[1]')\
    .appName('bai_thuc_hanh_1.com')\
    .getOrCreate()
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
rdd = spark.sparkContext.parallelize(data, 2) 
def checkEvenNumber(partition):
    temp = []
    for i in partition:
        if i % 2 == 0:  
            temp.append(i * 2)  
    return iter(temp)  
rdd2 = rdd.mapPartitions(checkEvenNumber)  
print(rdd2.collect())