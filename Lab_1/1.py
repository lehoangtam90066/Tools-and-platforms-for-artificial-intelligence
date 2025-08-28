from pyspark.sql import SparkSession
import random
spark = SparkSession.builder \
    .appName("Count Even and Odd Numbers") \
    .getOrCreate()
data = spark.sparkContext.parallelize(range(1, 1000001)).map(lambda _: random.randint(0, 1000))
even_count = data.filter(lambda x: x % 2 == 0).count()
odd_count = data.filter(lambda x: x % 2 != 0).count()
print(f"Số chẵn: {even_count}, Số lẻ: {odd_count}")
