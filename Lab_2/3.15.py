from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

# Khởi tạo SparkSession
spark = SparkSession.builder.appName("MovieRatingAnalysis").getOrCreate()

# Đọc dữ liệu từ file movies
movies_df = spark.read.option("delimiter", "\t").csv("F:\Năm 3 Học Kì 2\Các công cụ và nền tảng cho trí tuệ nhân tạo\\2274802010780_LeHoangTam_Lab2\\2274802010780_LeHoangTam_Lab2\data\movies.tsv", header=False)
movies_df = movies_df.withColumnRenamed("_c0", "actor").withColumnRenamed("_c1", "movie").withColumnRenamed("_c2", "year")

# Tính rating trung bình và số lượng phim của từng diễn viên theo năm
actor_year_stats = movies_df.groupBy("actor", "year").agg(
    avg("rating").alias("avg_rating"),
    count("movie").alias("num_movies")
)

# Tìm năm thành công nhất của mỗi diễn viên
actor_year_stats.show()

# Dừng SparkSession
spark.stop()
