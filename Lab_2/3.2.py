from pyspark.sql import SparkSession
from pyspark.sql.functions import count, desc
spark = SparkSession.builder.appName("MovieAnalysis").getOrCreate()
df = spark.read.csv("F:\Năm 3 Học Kì 2\Các công cụ và nền tảng cho trí tuệ nhân tạo\\2274802010780_LeHoangTam_Lab2\\2274802010780_LeHoangTam_Lab2\data\movies.tsv", header=False, sep="\t", inferSchema=True)
# Đổi tên các cột
df = df.withColumnRenamed("_c0", "actor").withColumnRenamed("_c1", "title").withColumnRenamed("_c2", "year")
# Nhóm theo năm và đếm số lượng phim
actor_movie_count = df.groupBy("actor").agg(count("*").alias("count")).orderBy(desc("count"))
actor_movie_count.show()


