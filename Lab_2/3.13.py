from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("MovieRatingAnalysis").getOrCreate()

# Đọc dữ liệu từ file movies
movies_df = spark.read.option("delimiter", "\t").csv("F:\Năm 3 Học Kì 2\Các công cụ và nền tảng cho trí tuệ nhân tạo\\2274802010780_LeHoangTam_Lab2\\2274802010780_LeHoangTam_Lab2\data\movies.tsv", header=False)
movies_df = movies_df.withColumnRenamed("_c0", "actor").withColumnRenamed("_c1", "movie").withColumnRenamed("_c2", "year")

# Tính trung bình rating và số lượng phim cho từng diễn viên
actor_stats = movies_df.groupBy("actor").agg(
    avg("rating").alias("avg_rating"),
    count("movie").alias("num_movies")
)

# Tìm diễn viên có rating trung bình cao nhất nhưng đóng ít phim nhất
best_actor = actor_stats.orderBy(col("avg_rating").desc(), col("num_movies").asc()).first()

print(f"Diễn viên thành công nhất: {best_actor['actor']}")

# Dừng SparkSession
spark.stop()
