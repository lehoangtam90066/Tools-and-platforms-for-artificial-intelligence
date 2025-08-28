from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("MovieRatingAnalysis").getOrCreate()

# Đọc dữ liệu từ file TSV
df = spark.read.option("delimiter", "\t").csv("F:\Năm 3 Học Kì 2\Các công cụ và nền tảng cho trí tuệ nhân tạo\\2274802010780_LeHoangTam_Lab2\\2274802010780_LeHoangTam_Lab2\data\movie-ratings.tsv", header=False)
df = df.withColumnRenamed("_c0", "rating").withColumnRenamed("_c1", "movie").withColumnRenamed("_c2", "year")

# Tính rating trung bình cho movie trước và sau 2000
before_2000_avg = df.filter(df["year"] < 2000).agg(avg("rating").alias("avg_rating_before_2000")).collect()[0][0]
after_2000_avg = df.filter(df["year"] >= 2000).agg(avg("rating").alias("avg_rating_after_2000")).collect()[0][0]

# Tính khoảng cách giữa rating trung bình
rating_difference = abs(before_2000_avg - after_2000_avg)

print(f"Khoảng cách giữa rating trung bình trước và sau 2000 là: {rating_difference}")

# Dừng SparkSession
spark.stop()
