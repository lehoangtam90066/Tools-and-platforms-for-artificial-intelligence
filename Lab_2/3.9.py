from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("MovieRatingAnalysis").getOrCreate()

# Đọc dữ liệu từ file TSV
df = spark.read.option("delimiter", "\t").csv("F:\Năm 3 Học Kì 2\Các công cụ và nền tảng cho trí tuệ nhân tạo\\2274802010780_LeHoangTam_Lab2\\2274802010780_LeHoangTam_Lab2\data\movie-ratings.tsv", header=False)
df = df.withColumnRenamed("_c0", "rating").withColumnRenamed("_c1", "movie").withColumnRenamed("_c2", "year")

# Tính toán 'rating spread' cho mỗi movie
rating_spread_df = df.groupBy("movie").agg(
    (max("rating") - min("rating")).alias("rating_spread")
)

# Sắp xếp theo rating_spread từ cao xuống thấp
sorted_df = rating_spread_df.orderBy(col("rating_spread").desc())

# Hiển thị kết quả
sorted_df.show()

# Dừng SparkSession
spark.stop()
