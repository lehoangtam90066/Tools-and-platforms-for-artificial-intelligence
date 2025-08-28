import random
from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Multiple Conditions and Divisibility") \
    .getOrCreate()
data = spark.sparkContext.parallelize(range(1, 1000001)).map(lambda _: random.randint(-1000, 1000))
processed_data = data.map(lambda x: x * 3 if x % 2 == 0 and x > 0 else
                                      x * 5 if x % 2 == 0 and x < 0 else
                                      x * 2 if x % 2 != 0 and x > 0 else
                                      x * 4)
divisible_by_2 = processed_data.filter(lambda x: x % 2 == 0).count()
divisible_by_3 = processed_data.filter(lambda x: x % 3 == 0).count()
divisible_by_5 = processed_data.filter(lambda x: x % 5 == 0).count()
print(f"Số chia hết cho 2: {divisible_by_2}")
print(f"Số chia hết cho 3: {divisible_by_3}")
print(f"Số chia hết cho 5: {divisible_by_5}")
