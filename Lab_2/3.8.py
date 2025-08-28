from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("MovieRatingAnalysis").getOrCreate()
df = spark.read.option("delimiter", "\t").csv("F:\Năm 3 Học Kì 2\Các công cụ và nền tảng cho trí tuệ nhân tạo\\2274802010780_LeHoangTam_Lab2\\2274802010780_LeHoangTam_Lab2\data\movie-ratings.tsv", header=False)
df = df.withColumnRenamed("_c0", "rating").withColumnRenamed("_c1", "movie").withColumnRenamed("_c2", "year")
filtered_df = df.filter((col('rating').isNull()) | (col('rating') < 1))
filtered_df.show()
spark.stop()
